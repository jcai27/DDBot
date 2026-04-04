from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from daily_digest_bot.delivery import DigestDeliveryClient
from daily_digest_bot.digest import DigestGenerator
from daily_digest_bot.ingestion import IngestionService
from daily_digest_bot.llm import OpenAIClient
from daily_digest_bot.models import DigestRun, StructuredEvent
from daily_digest_bot.personalization import PersonalizationService
from daily_digest_bot.ranking import RankingEngine
from daily_digest_bot.store import Store
from daily_digest_bot.thread_processing import ThreadProcessor


class DailyDigestPipeline:
    """Coordinates ingestion, extraction, ranking, digest creation, and delivery."""
    def __init__(
        self,
        store: Store,
        ingestion_service: IngestionService,
        delivery_client: DigestDeliveryClient,
        extract_llm_client: OpenAIClient | None = None,
        digest_llm_client: OpenAIClient | None = None,
        digest_recipient_mode: str = "opt_in",
        digest_local_hour: int = 9,
        retention_days: int = 90,
    ) -> None:
        self.store = store
        self.ingestion_service = ingestion_service
        self.delivery_client = delivery_client
        self.thread_processor = ThreadProcessor(llm_client=extract_llm_client)
        self.personalization_service = PersonalizationService(digest_recipient_mode=digest_recipient_mode)
        self.ranking_engine = RankingEngine()
        self.digest_generator = DigestGenerator(llm_client=digest_llm_client)
        self.digest_local_hour = digest_local_hour
        self.retention_days = retention_days

    def run(self, now_utc: datetime | None = None, force_send: bool = False) -> dict[str, int | float]:
        """Execute one digest cycle and return operational metrics."""
        now = now_utc or datetime.now(timezone.utc)
        self.store.init_schema()

        ingest_stats = self.ingestion_service.run()
        touched_threads = ingest_stats.get("touched_threads", [])
        processed_threads, extracted_events, low_confidence = self._process_threads_to_events(touched_threads=touched_threads)
        self.store.apply_dedupe_groups_last_24h()

        users = {u.user_id: u for u in self.store.list_users()}
        events_24h = self.store.list_structured_events_last_24h()

        existing_profiles = {p.user_id: p for p in self.store.list_user_profiles()}
        for user in users.values():
            if user.user_id not in existing_profiles:
                profile = self.personalization_service.bootstrap_profile(user, events_24h)
                self.store.upsert_user_profile(profile)
                existing_profiles[user.user_id] = profile

        open_high = self.store.list_open_high_urgency_events(min_urgency=0.7)
        candidate_pool = self._merge_candidates(events_24h, open_high)

        digests_sent = 0
        linked_items_sum = 0
        items_sum = 0

        profiles = self.store.list_user_profiles(digest_enabled_only=True)
        for profile in profiles:
            user = users.get(profile.user_id)
            if user is None:
                continue
            if user.user_id == "USLACKBOT" or user.display_name.strip().lower() == "slackbot":
                continue
            if not force_send and not self._should_send_for_user(now, profile):
                continue

            ranked = self.ranking_engine.rank(profile, candidate_pool)
            digest_text, item_count, linked_item_count = self.digest_generator.build_digest(
                user=user,
                ranked_events=ranked,
                max_items=int(profile.digest_preferences.get("max_items", 6)),
                user_profile=profile,
            )

            run_id = str(uuid.uuid4())
            top_event_ids = [item.event.event_id for item in ranked[: int(profile.digest_preferences.get("max_items", 6))]]
            self.delivery_client.send_dm(user_id=profile.user_id, text=digest_text, run_id=run_id, event_ids=top_event_ids)

            local_date = self._local_date_str(now, profile.timezone)
            run = DigestRun(
                run_id=run_id,
                user_id=profile.user_id,
                window_start=now - timedelta(hours=24),
                window_end=now,
                sent_at=now,
                item_count=item_count,
                linked_item_count=linked_item_count,
            )
            self.store.create_digest_run(run=run, local_digest_date=local_date)

            digests_sent += 1
            linked_items_sum += linked_item_count
            items_sum += item_count

        self.store.purge_old_data(
            raw_retention_days=self.retention_days,
            event_retention_days=self.retention_days,
        )

        link_ratio = (linked_items_sum / items_sum) if items_sum else 1.0
        metrics: dict[str, int | float] = {
            "ingestion_users": int(ingest_stats.get("users", 0)),
            "ingestion_channels": int(ingest_stats.get("channels", 0)),
            "messages_upserted": int(ingest_stats.get("messages_upserted", 0)),
            "thread_refreshes": int(ingest_stats.get("thread_refreshes", 0)),
            "threads_processed": processed_threads,
            "events_extracted": extracted_events,
            "low_confidence_events": low_confidence,
            "digests_sent": digests_sent,
            "linked_item_ratio": round(link_ratio, 3),
        }
        return metrics

    def _process_threads_to_events(self, touched_threads: list[tuple[str, str]]) -> tuple[int, int, int]:
        """Convert touched Slack threads into structured events and counts."""
        processed_threads = 0
        extracted_events = 0
        low_confidence = 0

        for thread_ts, channel_id in touched_threads:
            messages = self.store.get_thread_messages(thread_ts, channel_id)
            if not messages:
                continue
            event = self.thread_processor.process_thread(
                thread_ts=thread_ts,
                channel_id=channel_id,
                messages=messages,
            )
            self.store.upsert_structured_event(event)
            processed_threads += 1
            extracted_events += 1
            if event.needs_reprocess:
                low_confidence += 1

        return processed_threads, extracted_events, low_confidence

    def _merge_candidates(self, events_24h: list[StructuredEvent], open_high: list[StructuredEvent]) -> list[StructuredEvent]:
        """Merge event sources by event_id, preferring latest object per id."""
        by_id = {event.event_id: event for event in events_24h}
        for event in open_high:
            by_id[event.event_id] = event
        return list(by_id.values())

    def _should_send_for_user(self, now_utc: datetime, profile) -> bool:
        """Apply weekday/hour/dedup constraints for a recipient."""
        try:
            tz = ZoneInfo(profile.timezone)
        except Exception:
            tz = ZoneInfo("America/New_York")

        local_now = now_utc.astimezone(tz)
        if local_now.weekday() >= 5:
            return False

        desired_hour = int(profile.digest_preferences.get("delivery_hour_local", self.digest_local_hour))
        if local_now.hour < desired_hour:
            return False

        local_date = local_now.date().isoformat()
        if self.store.has_digest_run_for_local_date(profile.user_id, local_date):
            return False

        return True

    def _local_date_str(self, now_utc: datetime, timezone_name: str) -> str:
        """Return YYYY-MM-DD in the recipient timezone with safe fallback."""
        try:
            tz = ZoneInfo(timezone_name)
        except Exception:
            tz = ZoneInfo("America/New_York")
        return now_utc.astimezone(tz).date().isoformat()
