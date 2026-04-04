from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

from daily_digest_bot.db.connection import DBConnection
from daily_digest_bot.db.repositories import (
    ChannelsRepository,
    DigestRunsRepository,
    EventsRepository,
    FeedbackRepository,
    MessagesRepository,
    UsersRepository,
)
from daily_digest_bot.db.schema import init_schema
from daily_digest_bot.models import DigestRun, Message, StructuredEvent, User, UserProfile


class Store:
    """Compatibility facade over smaller repository modules."""

    def __init__(self, db_path: str = "digest.db") -> None:
        self.db = DBConnection(db_path)
        self.users = UsersRepository()
        self.channels = ChannelsRepository()
        self.messages = MessagesRepository()
        self.events = EventsRepository()
        self.digest_runs = DigestRunsRepository()
        self.feedback = FeedbackRepository()

    def connect(self):
        """Return a commit-on-success DB context manager."""
        return self.db.connect()

    def init_schema(self) -> None:
        """Create/upgrade DB schema to the current expected shape."""
        with self.db.connect() as conn:
            init_schema(conn)

    def upsert_user(self, user: User) -> None:
        with self.db.connect() as conn:
            self.users.upsert_user(conn, user)

    def upsert_channel(self, channel_id: str, name: str) -> None:
        with self.db.connect() as conn:
            self.channels.upsert_channel(conn, channel_id, name)

    def upsert_message(self, message: Message) -> None:
        with self.db.connect() as conn:
            self.messages.upsert_message(conn, message)

    def set_channel_watermark(self, channel_id: str, last_history_ts: str) -> None:
        with self.db.connect() as conn:
            self.channels.set_channel_watermark(conn, channel_id, last_history_ts)

    def get_channel_watermark(self, channel_id: str) -> str | None:
        with self.db.connect() as conn:
            return self.channels.get_channel_watermark(conn, channel_id)

    def get_thread_messages(self, key_ts: str, channel_id: str) -> list[Message]:
        with self.db.connect() as conn:
            return self.messages.get_thread_messages(conn, key_ts, channel_id)

    def upsert_structured_event(self, event: StructuredEvent) -> None:
        with self.db.connect() as conn:
            self.events.upsert_structured_event(conn, event)

    def list_structured_events_last_24h(self) -> list[StructuredEvent]:
        with self.db.connect() as conn:
            return self.events.list_structured_events_last_24h(conn)

    def list_open_high_urgency_events(self, min_urgency: float = 0.7) -> list[StructuredEvent]:
        with self.db.connect() as conn:
            return self.events.list_open_high_urgency_events(conn, min_urgency=min_urgency)

    def apply_dedupe_groups_last_24h(self) -> None:
        with self.db.connect() as conn:
            self.events.apply_dedupe_groups_last_24h(conn)

    def upsert_user_profile(self, profile: UserProfile) -> None:
        with self.db.connect() as conn:
            self.users.upsert_user_profile(conn, profile)

    def list_user_profiles(self, digest_enabled_only: bool = False) -> list[UserProfile]:
        with self.db.connect() as conn:
            return self.users.list_user_profiles(conn, digest_enabled_only=digest_enabled_only)

    def list_users(self) -> list[User]:
        with self.db.connect() as conn:
            return self.users.list_users(conn)

    def add_feedback(self, user_id: str, event_id: str, signal_type: str, run_id: str | None = None) -> None:
        with self.db.connect() as conn:
            self.feedback.add_feedback(conn, user_id, event_id, signal_type, run_id=run_id)

    def list_feedback_window_days(self, days: int = 14) -> list[sqlite3.Row]:
        with self.db.connect() as conn:
            return self.feedback.list_feedback_window_days(conn, days=days)

    def create_digest_run(self, run: DigestRun, local_digest_date: str) -> None:
        with self.db.connect() as conn:
            self.digest_runs.create_digest_run(conn, run, local_digest_date)

    def has_digest_run_for_local_date(self, user_id: str, local_digest_date: str) -> bool:
        with self.db.connect() as conn:
            return self.digest_runs.has_digest_run_for_local_date(conn, user_id, local_digest_date)

    def list_recent_digest_runs(self, limit: int = 100) -> list[DigestRun]:
        with self.db.connect() as conn:
            return self.digest_runs.list_recent_digest_runs(conn, limit=limit)

    def purge_old_data(self, raw_retention_days: int, event_retention_days: int) -> None:
        """Apply retention cutoffs to messages, events, feedback, and digest runs."""
        raw_cutoff = int(datetime.now(timezone.utc).timestamp() - raw_retention_days * 24 * 3600)
        event_cutoff = int(datetime.now(timezone.utc).timestamp() - event_retention_days * 24 * 3600)
        with self.db.connect() as conn:
            self.messages.purge_old_messages(conn, raw_cutoff)
            self.events.purge_old_events(conn, event_cutoff)
            self.feedback.purge_old_feedback(conn, event_cutoff)
            self.digest_runs.purge_old_digest_runs(conn, event_cutoff)
