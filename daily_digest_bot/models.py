from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum


class EventType(str, Enum):
    BLOCKER = "blocker"
    RISK = "risk"
    DECISION = "decision"
    STATUS_UPDATE = "status_update"
    UNRESOLVED_QUESTION = "unresolved_question"


@dataclass(slots=True)
class User:
    user_id: str
    display_name: str
    role: str = "engineer"


@dataclass(slots=True)
class Message:
    message_id: str
    channel_id: str
    user_id: str
    text: str
    ts: str
    thread_ts: str | None = None
    reactions_count: int = 0
    reply_count: int = 0


@dataclass(slots=True)
class StructuredEvent:
    event_id: str
    thread_ts: str
    channel_id: str
    summary: str
    event_type: EventType
    project: str
    subsystem: str
    participants: list[str]
    urgency_score: float
    relevant_roles: list[str]
    is_open: bool
    source_thread_link: str
    created_at: datetime
    confidence: float = 0.0
    dedupe_group_id: str = ""
    last_seen_at: datetime | None = None
    needs_reprocess: bool = False


@dataclass(slots=True)
class UserProfile:
    user_id: str
    role: str
    active_projects: list[str]
    ownership_areas: list[str]
    digest_preferences: dict[str, str | int | float | bool]
    learned_feedback_weights: dict[str, float]
    digest_enabled: bool = True
    timezone: str = "America/New_York"


@dataclass(slots=True)
class RankedEvent:
    event: StructuredEvent
    score: float


@dataclass(slots=True)
class DigestRun:
    run_id: str
    user_id: str
    window_start: datetime
    window_end: datetime
    sent_at: datetime
    item_count: int
    linked_item_count: int
