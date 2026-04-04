from datetime import datetime, timedelta, timezone

from daily_digest_bot.models import EventType, StructuredEvent, UserProfile
from daily_digest_bot.ranking import RankingEngine


def event(
    event_id: str,
    subsystem: str,
    urgency: float,
    is_open: bool,
    roles: list[str],
    project: str = "atlas",
    participants: list[str] | None = None,
    hours_ago: int = 1,
) -> StructuredEvent:
    now = datetime.now(timezone.utc)
    return StructuredEvent(
        event_id=event_id,
        thread_ts="1710000000.0001",
        channel_id="C1",
        summary="summary",
        event_type=EventType.BLOCKER,
        project=project,
        subsystem=subsystem,
        participants=participants or ["U2"],
        urgency_score=urgency,
        relevant_roles=roles,
        is_open=is_open,
        source_thread_link="https://example.com",
        created_at=now - timedelta(hours=hours_ago),
        confidence=0.9,
        dedupe_group_id=f"{project}:{subsystem}",
        last_seen_at=now - timedelta(hours=hours_ago),
    )


def test_ranking_prefers_role_ownership_and_urgency() -> None:
    profile = UserProfile(
        user_id="U1",
        role="hardware_engineer",
        active_projects=["atlas"],
        ownership_areas=["thermal"],
        digest_preferences={"max_items": 6},
        learned_feedback_weights={
            "role_match": 1.0,
            "ownership_match": 1.0,
            "project_match": 1.0,
            "urgency": 1.2,
            "recency": 0.5,
            "novelty": 0.8,
            "open_issue_bonus": 0.8,
            "involvement_penalty": 0.6,
            "engagement_boost": 0.4,
        },
        digest_enabled=True,
        timezone="America/New_York",
    )

    e1 = event("E1", subsystem="thermal", urgency=0.9, is_open=True, roles=["hardware_engineer"], hours_ago=1)
    e2 = event("E2", subsystem="firmware", urgency=0.5, is_open=False, roles=["pm"], hours_ago=10)

    ranked = RankingEngine().rank(profile, [e2, e1])
    assert ranked[0].event.event_id == "E1"
    assert ranked[0].score > ranked[1].score


def test_open_blocker_carryover_beats_low_status() -> None:
    profile = UserProfile(
        user_id="U1",
        role="hardware_engineer",
        active_projects=["atlas"],
        ownership_areas=["thermal"],
        digest_preferences={"max_items": 6},
        learned_feedback_weights={},
        digest_enabled=True,
        timezone="America/New_York",
    )

    carryover = event(
        "E-open",
        subsystem="thermal",
        urgency=0.8,
        is_open=True,
        roles=["hardware_engineer"],
        hours_ago=20,
    )
    low = event(
        "E-low",
        subsystem="thermal",
        urgency=0.3,
        is_open=False,
        roles=["hardware_engineer"],
        hours_ago=1,
    )

    ranked = RankingEngine().rank(profile, [low, carryover])
    assert ranked[0].event.event_id == "E-open"
