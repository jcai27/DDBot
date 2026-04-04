from __future__ import annotations

from collections import Counter

from daily_digest_bot.models import StructuredEvent, User, UserProfile


class PersonalizationService:
    """Bootstraps simple user profiles from recent event participation."""
    def __init__(self, digest_recipient_mode: str = "opt_in") -> None:
        self.digest_recipient_mode = digest_recipient_mode

    def bootstrap_profile(self, user: User, events: list[StructuredEvent]) -> UserProfile:
        """Create an initial profile with inferred projects/subsystems and defaults."""
        user_events = [e for e in events if user.user_id in e.participants]
        projects = [e.project for e in user_events]
        subsystems = [e.subsystem for e in user_events]

        top_projects = [p for p, _ in Counter(projects).most_common(3)] or ["general"]
        top_subsystems = [s for s, _ in Counter(subsystems).most_common(3)] or ["general"]

        return UserProfile(
            user_id=user.user_id,
            role=user.role,
            active_projects=top_projects,
            ownership_areas=top_subsystems,
            digest_preferences={"max_items": 6, "delivery_hour_local": 9},
            learned_feedback_weights={
                "role_match": 1.0,
                "ownership_match": 1.0,
                "project_match": 1.0,
                "urgency": 1.1,
                "recency": 0.7,
                "novelty": 0.8,
                "open_issue_bonus": 0.8,
                "involvement_penalty": 0.6,
                "engagement_boost": 0.4,
            },
            digest_enabled=self._default_digest_enabled(),
            timezone="America/New_York",
        )

    def _default_digest_enabled(self) -> bool:
        """Resolve bootstrap digest-enabled behavior from recipient mode."""
        # v1 default: opt-in mode still enabled for bootstrap so onboarding can later disable.
        return self.digest_recipient_mode in {"opt_in", "allowlist", "all"}
