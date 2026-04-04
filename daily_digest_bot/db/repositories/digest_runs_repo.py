from __future__ import annotations

import sqlite3
from datetime import datetime

from daily_digest_bot.models import DigestRun


class DigestRunsRepository:
    """Persistence operations for digest run history and dedupe checks."""

    def create_digest_run(self, conn: sqlite3.Connection, run: DigestRun, local_digest_date: str) -> None:
        """Insert or replace a digest run record for a user/date window."""
        conn.execute(
            """
            INSERT OR REPLACE INTO digest_runs
            (run_id, user_id, local_digest_date, window_start, window_end, sent_at, item_count, linked_item_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run.run_id,
                run.user_id,
                local_digest_date,
                run.window_start.isoformat(),
                run.window_end.isoformat(),
                run.sent_at.isoformat(),
                run.item_count,
                run.linked_item_count,
            ),
        )

    def has_digest_run_for_local_date(self, conn: sqlite3.Connection, user_id: str, local_digest_date: str) -> bool:
        """Return True if a digest has already been sent for user/date."""
        row = conn.execute(
            """
            SELECT 1 FROM digest_runs WHERE user_id = ? AND local_digest_date = ? LIMIT 1
            """,
            (user_id, local_digest_date),
        ).fetchone()
        return row is not None

    def list_recent_digest_runs(self, conn: sqlite3.Connection, limit: int = 100) -> list[DigestRun]:
        """Return latest digest runs up to limit ordered by sent_at descending."""
        rows = conn.execute(
            """
            SELECT * FROM digest_runs ORDER BY sent_at DESC LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [
            DigestRun(
                run_id=row["run_id"],
                user_id=row["user_id"],
                window_start=datetime.fromisoformat(row["window_start"]),
                window_end=datetime.fromisoformat(row["window_end"]),
                sent_at=datetime.fromisoformat(row["sent_at"]),
                item_count=int(row["item_count"]),
                linked_item_count=int(row["linked_item_count"]),
            )
            for row in rows
        ]

    def purge_old_digest_runs(self, conn: sqlite3.Connection, event_cutoff: int) -> None:
        """Delete digest run rows older than event_cutoff epoch seconds."""
        conn.execute(
            """
            DELETE FROM digest_runs
            WHERE CAST(strftime('%s', sent_at) AS INTEGER) < ?
            """,
            (event_cutoff,),
        )
