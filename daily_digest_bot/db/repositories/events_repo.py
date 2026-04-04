from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone

from daily_digest_bot.models import EventType, StructuredEvent


class EventsRepository:
    """Persistence operations for structured events and event lifecycle queries."""

    def upsert_structured_event(self, conn: sqlite3.Connection, event: StructuredEvent) -> None:
        """Insert/update one structured event keyed by event_id."""
        conn.execute(
            """
            INSERT INTO structured_events (
              event_id, thread_ts, channel_id, summary, event_type, project, subsystem,
              participants_json, urgency_score, relevant_roles_json, is_open,
              source_thread_link, created_at, confidence, dedupe_group_id, last_seen_at, needs_reprocess
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(event_id) DO UPDATE SET
              summary=excluded.summary,
              event_type=excluded.event_type,
              project=excluded.project,
              subsystem=excluded.subsystem,
              participants_json=excluded.participants_json,
              urgency_score=excluded.urgency_score,
              relevant_roles_json=excluded.relevant_roles_json,
              is_open=excluded.is_open,
              source_thread_link=excluded.source_thread_link,
              created_at=excluded.created_at,
              confidence=excluded.confidence,
              dedupe_group_id=excluded.dedupe_group_id,
              last_seen_at=excluded.last_seen_at,
              needs_reprocess=excluded.needs_reprocess
            """,
            (
                event.event_id,
                event.thread_ts,
                event.channel_id,
                event.summary,
                event.event_type.value,
                event.project,
                event.subsystem,
                json.dumps(event.participants),
                event.urgency_score,
                json.dumps(event.relevant_roles),
                1 if event.is_open else 0,
                event.source_thread_link,
                event.created_at.isoformat(),
                event.confidence,
                event.dedupe_group_id,
                event.last_seen_at.isoformat() if event.last_seen_at else event.created_at.isoformat(),
                1 if event.needs_reprocess else 0,
            ),
        )

    def list_structured_events_last_24h(self, conn: sqlite3.Connection) -> list[StructuredEvent]:
        """Return recent events seen in the last 24 hours."""
        cutoff = datetime.now(timezone.utc).timestamp() - 24 * 3600
        rows = conn.execute(
            """
            SELECT * FROM structured_events
            WHERE CAST(strftime('%s', COALESCE(last_seen_at, created_at)) AS INTEGER) >= ?
            ORDER BY urgency_score DESC, COALESCE(last_seen_at, created_at) DESC
            """,
            (int(cutoff),),
        ).fetchall()
        return [self._row_to_event(row) for row in rows]

    def list_open_high_urgency_events(self, conn: sqlite3.Connection, min_urgency: float = 0.7) -> list[StructuredEvent]:
        """Return open events above urgency threshold."""
        rows = conn.execute(
            """
            SELECT * FROM structured_events
            WHERE is_open = 1 AND urgency_score >= ?
            ORDER BY urgency_score DESC, COALESCE(last_seen_at, created_at) DESC
            """,
            (min_urgency,),
        ).fetchall()
        return [self._row_to_event(row) for row in rows]

    def apply_dedupe_groups_last_24h(self, conn: sqlite3.Connection) -> None:
        """Recompute dedupe_group_id for recent events from project/subsystem."""
        events = self.list_structured_events_last_24h(conn)
        for event in events:
            group_id = f"{event.project}:{event.subsystem}".lower()
            conn.execute(
                "UPDATE structured_events SET dedupe_group_id = ? WHERE event_id = ?",
                (group_id, event.event_id),
            )

    def purge_old_events(self, conn: sqlite3.Connection, event_cutoff: int) -> None:
        """Delete events older than event_cutoff epoch seconds."""
        conn.execute(
            """
            DELETE FROM structured_events
            WHERE CAST(strftime('%s', COALESCE(last_seen_at, created_at)) AS INTEGER) < ?
            """,
            (event_cutoff,),
        )

    def _row_to_event(self, row: sqlite3.Row) -> StructuredEvent:
        """Convert DB row into StructuredEvent dataclass."""
        return StructuredEvent(
            event_id=row["event_id"],
            thread_ts=row["thread_ts"],
            channel_id=row["channel_id"],
            summary=row["summary"],
            event_type=EventType(row["event_type"]),
            project=row["project"],
            subsystem=row["subsystem"],
            participants=json.loads(row["participants_json"]),
            urgency_score=float(row["urgency_score"]),
            relevant_roles=json.loads(row["relevant_roles_json"]),
            is_open=bool(row["is_open"]),
            source_thread_link=row["source_thread_link"],
            created_at=datetime.fromisoformat(row["created_at"]),
            confidence=float(row["confidence"]),
            dedupe_group_id=row["dedupe_group_id"],
            last_seen_at=datetime.fromisoformat(row["last_seen_at"]) if row["last_seen_at"] else None,
            needs_reprocess=bool(row["needs_reprocess"]),
        )
