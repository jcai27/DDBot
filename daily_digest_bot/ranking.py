from __future__ import annotations

from datetime import datetime, timezone

from daily_digest_bot.models import RankedEvent, StructuredEvent, UserProfile


class RankingEngine:
    """Score and rank structured events for a specific user profile."""
    DEFAULT_WEIGHTS: dict[str, float] = {
        "role_match": 1.0,
        "ownership_match": 1.0,
        "project_match": 1.0,
        "urgency": 1.2,
        "recency": 0.6,
        "open_issue_bonus": 0.8,
    }

    def rank(self, user: UserProfile, candidates: list[StructuredEvent]) -> list[RankedEvent]:
        """Return high-to-low ranked events after deduping by dedupe_group_id."""
        ranked: list[RankedEvent] = []
        dedupe_best: dict[str, RankedEvent] = {}

        for event in candidates:
            # Score each event against this specific user profile.
            score = self._score(user, event)
            ranked_item = RankedEvent(event=event, score=score)
            # Dedupe by conceptual group (project/subsystem) when available.
            dedupe_key = event.dedupe_group_id or event.event_id
            current = dedupe_best.get(dedupe_key)
            # Keep only the best-scoring representative per dedupe bucket.
            if current is None or ranked_item.score > current.score:
                dedupe_best[dedupe_key] = ranked_item

        # Convert deduped map into display order expected by digest generation.
        ranked = list(dedupe_best.values())
        ranked.sort(key=lambda item: item.score, reverse=True)
        return ranked

    def _score(self, user: UserProfile, event: StructuredEvent) -> float:
        """Compute weighted relevance score for one event."""
        # Fixed weights provide predictable ranking behavior in v1.
        w = self.DEFAULT_WEIGHTS

        # Binary matches for role/ownership/project affinity.
        role_match = 1.0 if user.role in event.relevant_roles else 0.0
        ownership_match = 1.0 if event.subsystem in user.ownership_areas else 0.0
        project_match = 1.0 if event.project in user.active_projects else 0.0
        # Continuous signals for urgency and recency.
        urgency = float(event.urgency_score)
        recency = self._recency_score(event)
        # Explicit bonus for unresolved items to keep active risks visible.
        open_issue_bonus = 0.7 if event.is_open else 0.0

        return (
            role_match * w["role_match"]
            + ownership_match * w["ownership_match"]
            + project_match * w["project_match"]
            + urgency * w["urgency"]
            + recency * w["recency"]
            + open_issue_bonus * w["open_issue_bonus"]
        )

    def _recency_score(self, event: StructuredEvent) -> float:
        """Map event age to a bounded recency contribution."""
        now = datetime.now(timezone.utc)
        seen_at = event.last_seen_at or event.created_at
        hours = max(0.0, (now - seen_at).total_seconds() / 3600.0)
        if hours <= 2:
            return 1.0
        if hours <= 6:
            return 0.8
        if hours <= 12:
            return 0.6
        if hours <= 24:
            return 0.4
        return 0.2
