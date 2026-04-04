from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

from daily_digest_bot.models import Message


class MessagesRepository:
    """Persistence operations for raw Slack messages."""

    def upsert_message(self, conn: sqlite3.Connection, message: Message) -> None:
        """Insert/update a single message row keyed by message_id."""
        conn.execute(
            """
            INSERT INTO messages
            (message_id, channel_id, user_id, text, ts, thread_ts, reactions_count, reply_count, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(message_id) DO UPDATE SET
              text=excluded.text,
              thread_ts=excluded.thread_ts,
              reactions_count=excluded.reactions_count,
              reply_count=excluded.reply_count,
              updated_at=excluded.updated_at
            """,
            (
                message.message_id,
                message.channel_id,
                message.user_id,
                message.text,
                message.ts,
                message.thread_ts,
                message.reactions_count,
                message.reply_count,
                datetime.now(timezone.utc).isoformat(),
            ),
        )

    def get_thread_messages(self, conn: sqlite3.Connection, key_ts: str, channel_id: str) -> list[Message]:
        """Return ordered messages for a thread root timestamp in a channel."""
        rows = conn.execute(
            """
            SELECT * FROM messages
            WHERE channel_id = ?
              AND (thread_ts = ? OR (thread_ts IS NULL AND ts = ?))
            ORDER BY CAST(ts AS REAL) ASC
            """,
            (channel_id, key_ts, key_ts),
        ).fetchall()
        return [
            Message(
                message_id=row["message_id"],
                channel_id=row["channel_id"],
                user_id=row["user_id"],
                text=row["text"],
                ts=row["ts"],
                thread_ts=row["thread_ts"],
                reactions_count=int(row["reactions_count"]),
                reply_count=int(row["reply_count"]),
            )
            for row in rows
        ]

    def purge_old_messages(self, conn: sqlite3.Connection, raw_cutoff: int) -> None:
        """Delete message rows older than raw_cutoff epoch seconds."""
        conn.execute(
            "DELETE FROM messages WHERE CAST(ts AS REAL) < ?",
            (raw_cutoff,),
        )
