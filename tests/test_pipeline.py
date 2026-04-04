from datetime import datetime, timezone

from daily_digest_bot.delivery import StdoutDeliveryClient
from daily_digest_bot.ingestion import IngestionService, SlackClient
from daily_digest_bot.models import Message, User
from daily_digest_bot.pipeline import DailyDigestPipeline
from daily_digest_bot.store import Store


class FixedSlackClient(SlackClient):
    def __init__(self) -> None:
        base = datetime.now(timezone.utc).timestamp()
        self.thread_ts = f"{base:.4f}"
        self.reply_ts = f"{base + 0.0001:.4f}"

    def fetch_users(self) -> list[User]:
        return [
            User(user_id="U1", display_name="Avery", role="hardware_engineer"),
            User(user_id="U2", display_name="Mina", role="firmware_engineer"),
        ]

    def fetch_channels(self) -> list[dict[str, str]]:
        return [{"channel_id": "C1", "name": "hw-project-atlas"}]

    def fetch_channel_messages(self, channel_id: str, oldest_ts: str) -> list[Message]:
        rows = [
            Message(
                message_id=f"C1:{self.thread_ts}",
                channel_id="C1",
                user_id="U1",
                text="Blocker: thermal chamber unavailable for validation run.",
                ts=self.thread_ts,
                reply_count=1,
            )
        ]
        oldest = float(oldest_ts)
        return [r for r in rows if float(r.ts) > oldest]

    def fetch_thread_replies(self, channel_id: str, thread_ts: str) -> list[Message]:
        return [
            Message(
                message_id=f"C1:{self.thread_ts}",
                channel_id="C1",
                user_id="U1",
                text="Blocker: thermal chamber unavailable for validation run.",
                ts=self.thread_ts,
                reply_count=1,
            ),
            Message(
                message_id=f"C1:{self.reply_ts}",
                channel_id="C1",
                user_id="U2",
                text="Risk: schedule impact is 2 days unless we borrow lab capacity.",
                ts=self.reply_ts,
                thread_ts=self.thread_ts,
                reactions_count=2,
            ),
        ]


def _count_rows(store: Store, table: str) -> int:
    with store.connect() as conn:
        row = conn.execute(f"SELECT COUNT(*) AS n FROM {table}").fetchone()
    return int(row["n"])


def test_incremental_ingestion_idempotent(tmp_path) -> None:
    db_path = tmp_path / "digest.db"
    store = Store(db_path=str(db_path))
    store.init_schema()

    ingestion = IngestionService(store=store, slack_client=FixedSlackClient())
    stats_1 = ingestion.run()
    stats_2 = ingestion.run()

    assert stats_1["messages_upserted"] > 0
    assert stats_2["messages_upserted"] == 0
    assert _count_rows(store, "messages") == 2
    assert len(stats_1["touched_threads"]) == 1
    assert len(stats_2["touched_threads"]) == 0


def test_pipeline_run_creates_digest_run_and_metrics(tmp_path) -> None:
    db_path = tmp_path / "digest.db"
    store = Store(db_path=str(db_path))
    pipeline = DailyDigestPipeline(
        store=store,
        ingestion_service=IngestionService(store=store, slack_client=FixedSlackClient()),
        delivery_client=StdoutDeliveryClient(),
    )

    now = datetime(2026, 4, 3, 14, 0, tzinfo=timezone.utc)  # Friday 10am ET
    metrics = pipeline.run(now_utc=now, force_send=True)

    events = store.list_structured_events_last_24h()
    users = store.list_users()
    profiles = store.list_user_profiles()
    runs = store.list_recent_digest_runs(limit=10)

    assert len(events) >= 1
    assert len(users) == 2
    assert len(profiles) == len(users)
    assert len(runs) >= 1
    assert int(metrics["digests_sent"]) >= 1
    assert float(metrics["linked_item_ratio"]) >= 0.8
