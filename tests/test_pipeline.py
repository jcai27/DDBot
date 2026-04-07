from datetime import datetime, timezone

from daily_digest_bot.app import AppConfig, build_pipeline
from daily_digest_bot.delivery import DigestDeliveryClient, StdoutDeliveryClient
from daily_digest_bot.ingestion import IngestionService, SlackClient
from daily_digest_bot.llm import OpenAIClient
from daily_digest_bot.models import Message, User, UserProfile
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


class FakeLLMClient(OpenAIClient):
    def __init__(self) -> None:
        super().__init__(api_key="test", model="test-model")

    def json_completion(self, *, system_prompt: str, user_prompt: str, temperature: float = 0.2) -> dict:
        return {
            "summary": "Blocker in thermal validation; schedule risk unless backup capacity assigned.",
            "event_type": "blocker",
            "project": "atlas",
            "subsystem": "thermal",
            "participants": ["U1", "U2"],
            "urgency_score": 0.9,
            "relevant_roles": ["hardware_engineer", "pm"],
            "is_open": True,
            "confidence": 0.95,
        }

    def text_completion(self, *, system_prompt: str, user_prompt: str, temperature: float = 0.3) -> str:
        return (
            "What Needs Attention Today\n"
            "- Thermal blocker remains open. https://slack.com/app_redirect?channel=C1&message_ts=1710000000.0001\n\n"
            "Active Blockers & Risks\n"
            "- Validation capacity risk. https://slack.com/app_redirect?channel=C1&message_ts=1710000000.0001\n\n"
            "Decisions & Calls Needed\n"
            "- Decide whether to borrow external chamber.\n\n"
            "Recommended Next Actions\n"
            "- Assign owner and escalate timeline risk."
        )


class CaptureDeliveryClient(DigestDeliveryClient):
    def __init__(self) -> None:
        self.sent_user_ids: list[str] = []

    def send_dm(self, user_id: str, text: str, run_id: str | None = None, event_ids: list[str] | None = None) -> None:
        self.sent_user_ids.append(user_id)


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
        extract_llm_client=FakeLLMClient(),
        digest_llm_client=FakeLLMClient(),
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


def test_seed_demo_data_uses_stdout_delivery_even_without_dry_run(tmp_path) -> None:
    config = AppConfig(
        db_path=str(tmp_path / "digest.db"),
        run_digest=True,
        seed_demo_data=True,
        dry_run=False,
        force_send=True,
        slack_bot_token="",
        slack_channel_ids=[],
        openai_api_key="test",
        openai_model="test-model",
        digest_recipient_mode="opt_in",
        digest_local_hour=9,
        retention_days=90,
    )

    pipeline = build_pipeline(config)

    assert isinstance(pipeline.delivery_client, StdoutDeliveryClient)


def test_pipeline_skips_stale_profiles_not_seen_in_current_ingestion(tmp_path) -> None:
    db_path = tmp_path / "digest.db"
    store = Store(db_path=str(db_path))
    store.init_schema()
    store.upsert_user(User(user_id="USTALE", display_name="Stale Demo", role="engineer"))
    store.upsert_user_profile(
        UserProfile(
            user_id="USTALE",
            role="engineer",
            active_projects=["atlas"],
            ownership_areas=["thermal"],
            digest_preferences={"max_items": 6, "delivery_hour_local": 9},
            learned_feedback_weights={},
            digest_enabled=True,
            timezone="America/New_York",
        )
    )
    delivery = CaptureDeliveryClient()
    pipeline = DailyDigestPipeline(
        store=store,
        ingestion_service=IngestionService(store=store, slack_client=FixedSlackClient()),
        delivery_client=delivery,
        extract_llm_client=FakeLLMClient(),
        digest_llm_client=FakeLLMClient(),
    )

    now = datetime(2026, 4, 3, 14, 0, tzinfo=timezone.utc)
    metrics = pipeline.run(now_utc=now, force_send=True)

    assert "USTALE" not in delivery.sent_user_ids
    assert set(delivery.sent_user_ids) == {"U1", "U2"}
    assert int(metrics["digests_sent"]) == 2
