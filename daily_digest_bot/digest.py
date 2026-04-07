from __future__ import annotations

import json
import os
import re
from urllib import parse

from daily_digest_bot.llm import OpenAIAPIError, OpenAIClient
from daily_digest_bot.models import RankedEvent, User, UserProfile


class DigestGenerator:
    """Build recipient digest text via LLM-first strategy with deterministic fallback."""
    def __init__(self, llm_client: OpenAIClient, min_link_ratio: float = 0.8) -> None:
        self.llm_client = llm_client
        self.min_link_ratio = min_link_ratio

    def build_digest(
        self,
        user: User,
        ranked_events: list[RankedEvent],
        max_items: int = 6,
        user_profile: UserProfile | None = None,
    ) -> tuple[str, int, int]:
        """Return digest text plus item and linked-item counts for observability."""
        # Clamp to recipient preference to keep output concise/actionable.
        top = ranked_events[:max_items]
        # Primary path: model-generated digest from structured events.
        llm_digest = self._build_with_llm(user=user, user_profile=user_profile, top=top)
        if llm_digest:
            item_count = len(top)
            # Estimate whether model output linked enough items back to source threads.
            linked_item_count = self._estimate_linked_items(llm_digest)
            if item_count == 0 or (linked_item_count / max(1, item_count)) >= self.min_link_ratio:
                return llm_digest, item_count, linked_item_count

        # Fallback path keeps product reliable even on model/API failures.
        fallback = self._build_fallback(user=user, top=top)
        return fallback, len(top), self._count_linked_from_events(top)

    def _build_with_llm(self, user: User, user_profile: UserProfile | None, top: list[RankedEvent]) -> str:
        """Generate digest text from ranked events using the configured LLM client."""
        if not top:
            return ""

        # Build compact event payload for prompt context.
        event_lines = []
        for item in top:
            event = item.event
            event_lines.append(
                {
                    "score": round(item.score, 3),
                    "summary": event.summary,
                    "event_type": event.event_type.value,
                    "project": event.project,
                    "subsystem": event.subsystem,
                    "urgency_score": event.urgency_score,
                    "is_open": event.is_open,
                    "link": event.source_thread_link,
                }
            )

        profile_blob = {
            "role": user.role,
            "active_projects": user_profile.active_projects if user_profile else [],
            "ownership_areas": user_profile.ownership_areas if user_profile else [],
            "digest_preferences": user_profile.digest_preferences if user_profile else {},
        }

        # Strict section contract allows downstream block formatter to parse sections reliably.
        system_prompt = (
            "You are generating a concise, action-oriented daily digest for hardware engineering team members. "
            "Use only provided structured events. "
            "Return plain text with these sections exactly:\n"
            "What Needs Attention Today\n"
            "Active Blockers & Risks\n"
            "Decisions & Calls Needed\n"
            "Recommended Next Actions\n"
            "Most bullets must include a source Slack link."
        )
        user_prompt = (
            f"user={user.display_name} ({user.user_id})\n"
            f"profile={json.dumps(profile_blob)}\n"
            f"events={json.dumps(event_lines)}\n"
            "Write the digest now."
        )
        try:
            # Lower temperature keeps digest stable and less verbose/noisy.
            text = self.llm_client.text_completion(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                temperature=0.35,
            )
        except OpenAIAPIError:
            return ""
        return text.strip()

    def _build_fallback(self, user: User, top: list[RankedEvent]) -> str:
        """Build deterministic digest text when LLM output is unavailable/insufficient."""
        # Reserve top section for highest-priority items only.
        top_priorities = top[:3]
        top_ids = {item.event.event_id for item in top_priorities}
        # Avoid repeating top-priority events in subsequent sections.
        open_issues = [
            item
            for item in top
            if item.event.event_id not in top_ids and item.event.is_open and item.event.event_type.value in {"blocker", "risk"}
        ]
        decisions = [item for item in top if item.event.event_id not in top_ids and item.event.event_type.value == "decision"]

        # Build plain-text sectioned digest expected by delivery block parser.
        lines: list[str] = []
        lines.append(f"Daily hardware digest for {user.display_name}")
        lines.append("")

        lines.append("What Needs Attention Today")
        lines.extend(self._format_lines(top_priorities))
        lines.append("")

        lines.append("Active Blockers & Risks")
        lines.extend(self._format_lines(open_issues))
        lines.append("")

        lines.append("Decisions & Calls Needed")
        lines.extend(self._format_lines(decisions))
        lines.append("")

        lines.append("Recommended Next Actions")
        if top:
            first = top[0].event
            # Keep final section explicitly action-oriented and tied to source.
            lines.append(
                f"- Focus first on {self._clean_token(first.project)} / {self._clean_token(first.subsystem)}."
            )
            lines.append(
                f"- If this stays open through today, flag it in standup and assign a single owner for next action."
            )
            source_link = self._display_thread_link(first.source_thread_link)
            lines.append(
                f"- Track updates: <{source_link}|Open thread>"
            )
        else:
            lines.append("- No high-signal events in the last 24 hours.")

        return "\n".join(lines)

    def _format_lines(self, items: list[RankedEvent]) -> list[str]:
        """Render ranked events into readable bullet lines with thread links."""
        if not items:
            return ["- None"]

        lines: list[str] = []
        for item in items:
            event = item.event
            label = self._event_label(event.event_type.value)
            # 3-line bullet structure: summary, metadata, source link.
            lines.append(f"- *{label}*: {event.summary}")
            lines.append(f"  Project: {self._clean_token(event.project)} | Subsystem: {self._clean_token(event.subsystem)}")
            source_link = self._display_thread_link(event.source_thread_link)
            lines.append(f"  Source: <{source_link}|Open thread>")
        return lines

    def _event_label(self, event_type: str) -> str:
        labels = {
            "risk": "Risk",
            "blocker": "Blocker",
            "decision": "Decision",
            "status_update": "Status Update",
            "issue": "Issue",
        }
        return labels.get(event_type, event_type.replace("_", " ").title())

    def _clean_token(self, value: str) -> str:
        return value.replace("_", " ").strip()

    def _display_thread_link(self, raw_link: str) -> str:
        """Normalize redirect-style Slack links into stable, clickable web URLs."""
        link = raw_link.strip()
        if not link:
            return raw_link

        parsed = parse.urlparse(link)
        if parsed.netloc != "slack.com" or parsed.path != "/app_redirect":
            return link

        query = parse.parse_qs(parsed.query)
        channel = (query.get("channel") or [None])[0]
        message_ts = (query.get("message_ts") or [None])[0]
        if not channel or not message_ts:
            return link

        ts_digits = "".join(ch for ch in message_ts if ch.isdigit())
        ts_padded = (ts_digits + ("0" * 16))[:16]
        team = ((query.get("team") or [None])[0] or os.getenv("SLACK_TEAM_ID", "")).strip()
        if team:
            return f"https://app.slack.com/client/{team}/{channel}/thread/{channel}-{ts_padded}"
        return f"https://slack.com/archives/{channel}/p{ts_padded}"

    def _estimate_linked_items(self, text: str) -> int:
        # Approximate linked bullets by counting Slack links in any format this app emits.
        return len(re.findall(r"https://(?:app\.)?slack\.com/(?:app_redirect|archives|client)[^\s>|)]*", text))

    def _count_linked_from_events(self, top: list[RankedEvent]) -> int:
        return sum(1 for item in top if item.event.source_thread_link)
