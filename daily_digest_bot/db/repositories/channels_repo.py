from __future__ import annotations

import sqlite3
from datetime import datetime, timezone


class ChannelsRepository:
    """Persistence operations for channels and ingestion watermarks."""

    def upsert_channel(self, conn: sqlite3.Connection, channel_id: str, name: str) -> None:
        """Insert/update a channel definition by channel id."""
        conn.execute(
            """
            INSERT INTO channels (channel_id, name)
            VALUES (?, ?)
            ON CONFLICT(channel_id) DO UPDATE SET
              name=excluded.name
            """,
            (channel_id, name),
        )

    def set_channel_watermark(self, conn: sqlite3.Connection, channel_id: str, last_history_ts: str) -> None:
        """Store last seen history timestamp for incremental ingestion."""
        conn.execute(
            """
            INSERT INTO ingestion_state (channel_id, last_history_ts, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(channel_id) DO UPDATE SET
              last_history_ts=excluded.last_history_ts,
              updated_at=excluded.updated_at
            """,
            (channel_id, last_history_ts, datetime.now(timezone.utc).isoformat()),
        )

    def get_channel_watermark(self, conn: sqlite3.Connection, channel_id: str) -> str | None:
        """Return channel watermark timestamp if present, else None."""
        row = conn.execute(
            "SELECT last_history_ts FROM ingestion_state WHERE channel_id = ?",
            (channel_id,),
        ).fetchone()
        return str(row["last_history_ts"]) if row else None
