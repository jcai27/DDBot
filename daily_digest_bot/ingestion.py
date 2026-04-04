from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
import sys

from daily_digest_bot.models import Message, User
from daily_digest_bot.slack_api import SlackApiClient, SlackApiError
from daily_digest_bot.store import Store


class SlackClient(ABC):
    """Interface for Slack data sources used by ingestion."""
    @abstractmethod
    def fetch_users(self) -> list[User]:
        raise NotImplementedError

    @abstractmethod
    def fetch_channels(self) -> list[dict[str, str]]:
        raise NotImplementedError

    @abstractmethod
    def fetch_channel_messages(self, channel_id: str, oldest_ts: str) -> list[Message]:
        raise NotImplementedError

    @abstractmethod
    def fetch_thread_replies(self, channel_id: str, thread_ts: str) -> list[Message]:
        raise NotImplementedError


class SlackWebClient(SlackClient):
    """Production Slack source backed by Slack Web API."""
    def __init__(self, bot_token: str, channel_ids: list[str]) -> None:
        self.bot_token = bot_token
        self.channel_ids = channel_ids
        self.api = SlackApiClient(bot_token=bot_token)

    def fetch_users(self) -> list[User]:
        """Fetch human workspace users and infer coarse role labels from title."""
        users: list[User] = []
        cursor = ""
        while True:
            payload = self._api_get("users.list", {"limit": "200", "cursor": cursor})
            for member in payload.get("members", []):
                user_id = member.get("id")
                if not user_id or member.get("deleted") or member.get("is_bot") or member.get("is_app_user"):
                    continue
                if user_id == "USLACKBOT":
                    continue
                profile = member.get("profile", {})
                display_name = profile.get("display_name") or profile.get("real_name") or member.get("name") or user_id
                title = (profile.get("title") or "").lower()
                role = self._infer_role(title)
                users.append(User(user_id=user_id, display_name=display_name, role=role))
            cursor = payload.get("response_metadata", {}).get("next_cursor", "")
            if not cursor:
                break
        return users

    def fetch_channels(self) -> list[dict[str, str]]:
        """Fetch accessible channels, optionally filtered to configured channel ids."""
        channels: list[dict[str, str]] = []
        cursor = ""
        wanted = set(self.channel_ids)
        while True:
            payload = self._api_get(
                "conversations.list",
                {
                    "types": "public_channel,private_channel",
                    "exclude_archived": "true",
                    "limit": "200",
                    "cursor": cursor,
                },
            )
            for channel in payload.get("channels", []):
                channel_id = channel.get("id")
                if not channel_id:
                    continue
                if wanted and channel_id not in wanted:
                    continue
                channels.append({"channel_id": channel_id, "name": channel.get("name", channel_id)})
            cursor = payload.get("response_metadata", {}).get("next_cursor", "")
            if not cursor:
                break
        return channels

    def fetch_channel_messages(self, channel_id: str, oldest_ts: str) -> list[Message]:
        """Fetch new channel messages after oldest_ts with subtype filtering."""
        out: list[Message] = []
        cursor = ""
        while True:
            try:
                payload = self._api_get(
                    "conversations.history",
                    {
                        "channel": channel_id,
                        "oldest": oldest_ts,
                        "inclusive": "false",
                        "limit": "200",
                        "cursor": cursor,
                    },
                )
            except SlackApiError as exc:
                if exc.error == "not_in_channel":
                    joined = self._try_join_public_channel(channel_id)
                    if joined:
                        continue
                    print(
                        f"[warn] skipping channel={channel_id} reason=not_in_channel",
                        file=sys.stderr,
                    )
                    return []
                raise

            for msg in payload.get("messages", []):
                if not self._should_ingest_message(msg):
                    continue
                out.append(self._slack_msg_to_model(channel_id=channel_id, raw=msg))

            cursor = payload.get("response_metadata", {}).get("next_cursor", "")
            if not cursor:
                break
        return out

    def fetch_thread_replies(self, channel_id: str, thread_ts: str) -> list[Message]:
        """Fetch full reply set for a thread root timestamp."""
        payload = self._api_get(
            "conversations.replies",
            {
                "channel": channel_id,
                "ts": thread_ts,
                "limit": "200",
            },
        )
        out: list[Message] = []
        for idx, msg in enumerate(payload.get("messages", [])):
            if not self._should_ingest_message(msg):
                continue
            # API includes root as first item.
            if idx == 0:
                out.append(self._slack_msg_to_model(channel_id=channel_id, raw=msg, root_thread_ts=None))
                continue
            out.append(self._slack_msg_to_model(channel_id=channel_id, raw=msg, root_thread_ts=thread_ts))
        return out

    def _should_ingest_message(self, raw: dict) -> bool:
        """Return True for content messages and False for system subtype events."""
        subtype = raw.get("subtype")
        # Keep regular user messages and bot-authored chat content; skip other system subtypes.
        return subtype is None or subtype == "bot_message"

    def _slack_msg_to_model(self, channel_id: str, raw: dict, root_thread_ts: str | None = None) -> Message:
        ts = raw.get("ts", "")
        thread_ts = root_thread_ts or raw.get("thread_ts")
        reactions = raw.get("reactions", [])
        reactions_count = sum(int(r.get("count", 0)) for r in reactions)
        message_id = f"{channel_id}:{ts}"
        return Message(
            message_id=message_id,
            channel_id=channel_id,
            user_id=raw.get("user", "UNKNOWN"),
            text=raw.get("text", ""),
            ts=ts,
            thread_ts=thread_ts if thread_ts and thread_ts != ts else None,
            reactions_count=reactions_count,
            reply_count=int(raw.get("reply_count", 0)),
        )

    def _api_get(self, method: str, params: dict[str, str]) -> dict:
        return self.api.api_get(method=method, params=params)

    def _api_post(self, method: str, payload: dict) -> dict:
        return self.api.api_post(method=method, payload=payload)

    def _try_join_public_channel(self, channel_id: str) -> bool:
        try:
            self._api_post("conversations.join", {"channel": channel_id})
            return True
        except SlackApiError:
            return False

    def _infer_role(self, title: str) -> str:
        if "firmware" in title:
            return "firmware_engineer"
        if "hardware" in title or "electrical" in title or "ee" in title:
            return "hardware_engineer"
        if "pm" in title or "product" in title:
            return "pm"
        return "engineer"


class DemoSlackClient(SlackClient):
    """Deterministic in-process data source for demo and local dry runs."""
    def fetch_users(self) -> list[User]:
        return [
            User(user_id="U1", display_name="Avery", role="hardware_engineer"),
            User(user_id="U2", display_name="Mina", role="firmware_engineer"),
            User(user_id="U3", display_name="Ravi", role="pm"),
        ]

    def fetch_channels(self) -> list[dict[str, str]]:
        return [
            {"channel_id": "C1", "name": "hw-project-atlas"},
            {"channel_id": "C2", "name": "hw-bringup"},
        ]

    def fetch_channel_messages(self, channel_id: str, oldest_ts: str) -> list[Message]:
        now = datetime.now(timezone.utc)
        base_ts = f"{int(now.timestamp())}.0001"
        if channel_id == "C1":
            return [
                Message(
                    message_id=f"{channel_id}:{base_ts}",
                    channel_id="C1",
                    user_id="U1",
                    text="Blocker: thermal test failed on rev-B board, we cannot ship this week.",
                    ts=base_ts,
                    reply_count=2,
                )
            ]

        thread_ts = f"{int(now.timestamp())}.1001"
        return [
            Message(
                message_id=f"{channel_id}:{thread_ts}",
                channel_id="C2",
                user_id="U2",
                text="Decision: move to firmware v1.4 for bringup, issue resolved.",
                ts=thread_ts,
                reply_count=1,
            )
        ]

    def fetch_thread_replies(self, channel_id: str, thread_ts: str) -> list[Message]:
        if channel_id == "C1":
            return [
                Message(
                    message_id=f"C1:{thread_ts}",
                    channel_id="C1",
                    user_id="U1",
                    text="Blocker: thermal test failed on rev-B board, we cannot ship this week.",
                    ts=thread_ts,
                    reply_count=2,
                ),
                Message(
                    message_id=f"C1:{thread_ts}a",
                    channel_id="C1",
                    user_id="U2",
                    text="Risk is high for demo timeline unless we bypass sensor path.",
                    ts=f"{thread_ts[:-1]}2",
                    thread_ts=thread_ts,
                    reactions_count=4,
                ),
                Message(
                    message_id=f"C1:{thread_ts}b",
                    channel_id="C1",
                    user_id="U3",
                    text="Open question: do we accept temporary workaround for customer build?",
                    ts=f"{thread_ts[:-1]}3",
                    thread_ts=thread_ts,
                    reactions_count=2,
                ),
            ]

        return [
            Message(
                message_id=f"C2:{thread_ts}",
                channel_id="C2",
                user_id="U2",
                text="Decision: move to firmware v1.4 for bringup, issue resolved.",
                ts=thread_ts,
                reply_count=1,
            ),
            Message(
                message_id=f"C2:{thread_ts}a",
                channel_id="C2",
                user_id="U1",
                text="Status update: regression suite passed for power rail startup.",
                ts=f"{thread_ts[:-1]}2",
                thread_ts=thread_ts,
            ),
        ]


class IngestionService:
    """Persist Slack users/messages incrementally and return touched thread ids."""
    def __init__(self, store: Store, slack_client: SlackClient, default_backfill_hours: int = 24) -> None:
        self.store = store
        self.slack_client = slack_client
        self.default_backfill_hours = default_backfill_hours

    def run(self) -> dict[str, int | list[tuple[str, str]]]:
        """Run one incremental ingestion pass across all target channels."""
        users = self.slack_client.fetch_users()
        channels = self.slack_client.fetch_channels()

        for user in users:
            self.store.upsert_user(user)

        message_count = 0
        thread_refreshes = 0
        touched_threads: set[tuple[str, str]] = set()
        for channel in channels:
            channel_id = channel["channel_id"]
            self.store.upsert_channel(channel_id=channel_id, name=channel["name"])

            watermark = self.store.get_channel_watermark(channel_id)
            if watermark is None:
                watermark = str((datetime.now(timezone.utc) - timedelta(hours=self.default_backfill_hours)).timestamp())

            new_messages = self.slack_client.fetch_channel_messages(channel_id=channel_id, oldest_ts=watermark)
            changed_threads: set[str] = set()
            max_ts = float(watermark)
            for message in new_messages:
                self.store.upsert_message(message)
                message_count += 1
                max_ts = max(max_ts, float(message.ts))
                thread_root = message.thread_ts or message.ts
                if message.reply_count > 0 or message.thread_ts is not None:
                    changed_threads.add(thread_root)

            for thread_ts in changed_threads:
                replies = self.slack_client.fetch_thread_replies(channel_id=channel_id, thread_ts=thread_ts)
                if not replies:
                    continue
                thread_refreshes += 1
                thread_max = float(thread_ts)
                for reply in replies:
                    self.store.upsert_message(reply)
                    message_count += 1
                    thread_max = max(thread_max, float(reply.ts))
                touched_threads.add((thread_ts, channel_id))
                max_ts = max(max_ts, thread_max)

            self.store.set_channel_watermark(channel_id=channel_id, last_history_ts=str(max_ts))

        return {
            "users": len(users),
            "channels": len(channels),
            "messages_upserted": message_count,
            "thread_refreshes": thread_refreshes,
            "touched_threads": sorted(touched_threads),
        }
