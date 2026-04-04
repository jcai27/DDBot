from __future__ import annotations

import sqlite3
from datetime import datetime, timezone


class FeedbackRepository:
    """Persistence operations for explicit user feedback signals."""

    def add_feedback(self, conn: sqlite3.Connection, user_id: str, event_id: str, signal_type: str, run_id: str | None = None) -> None:
        """Insert one feedback event row for a user/event/run."""
        conn.execute(
            """
            INSERT INTO feedback_events (user_id, event_id, run_id, signal_type, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (user_id, event_id, run_id, signal_type, datetime.now(timezone.utc).isoformat()),
        )

    def list_feedback_window_days(self, conn: sqlite3.Connection, days: int = 14) -> list[sqlite3.Row]:
        """Return feedback rows created in the trailing time window."""
        cutoff = datetime.now(timezone.utc).timestamp() - (days * 24 * 3600)
        return conn.execute(
            """
            SELECT * FROM feedback_events
            WHERE CAST(strftime('%s', created_at) AS INTEGER) >= ?
            """,
            (int(cutoff),),
        ).fetchall()

    def purge_old_feedback(self, conn: sqlite3.Connection, event_cutoff: int) -> None:
        """Delete feedback rows older than event_cutoff epoch seconds."""
        conn.execute(
            """
            DELETE FROM feedback_events
            WHERE CAST(strftime('%s', created_at) AS INTEGER) < ?
            """,
            (event_cutoff,),
        )
