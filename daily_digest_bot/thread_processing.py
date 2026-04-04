from __future__ import annotations

import hashlib
from datetime import datetime, timezone
import os

from daily_digest_bot.llm import OpenAIAPIError, OpenAIClient
from daily_digest_bot.models import EventType, Message, StructuredEvent


class ThreadProcessor:
    """Extract a single structured event from a Slack thread conversation."""
    EVENT_KEYWORDS: dict[EventType, tuple[str, ...]] = {
        EventType.BLOCKER: ("blocker", "blocked", "cannot ship", "stuck"),
        EventType.RISK: ("risk", "concern", "might fail", "uncertain"),
        EventType.DECISION: ("decision", "decided", "approved"),
        EventType.STATUS_UPDATE: ("status", "update", "progress", "passed"),
        EventType.UNRESOLVED_QUESTION: ("?", "open question", "anyone know"),
    }

    def __init__(self, llm_client: OpenAIClient, confidence_threshold: float = 0.65) -> None:
        self.llm_client = llm_client
        self.confidence_threshold = confidence_threshold

    def process_thread(self, thread_ts: str, channel_id: str, messages: list[Message]) -> StructuredEvent:
        """Run deterministic+LLM extraction and return the final structured event."""
        # Keep a merged text buffer available for potential diagnostics/future heuristics.
        # (Current extraction paths compute their own text, but we preserve this local view.)
        merged_text = " ".join(m.text.strip() for m in messages)
        _ = merged_text

        # Always compute deterministic extraction first so we have a safe baseline event
        # even when the model is disabled, fails, or returns low-confidence output.
        default_event = self._deterministic_extract(thread_ts=thread_ts, channel_id=channel_id, messages=messages)

        # Attempt LLM-assisted extraction. This can return a partial field set.
        llm_fields = self._extract_with_llm(channel_id=channel_id, messages=messages)
        # Confidence defaults to 0.0 when model output is missing/unparseable.
        confidence = float(llm_fields.get("confidence", 0.0))

        # Guardrail: below threshold we trust deterministic extraction more than model output.
        # We still store model confidence to support observability and later reprocessing.
        if confidence < self.confidence_threshold:
            default_event.confidence = confidence
            default_event.needs_reprocess = True
            return default_event

        # Confidence passed: merge model fields on top of deterministic defaults.
        # Any missing model field falls back to the baseline event value.
        event_type = llm_fields.get("event_type", default_event.event_type)
        urgency_score = llm_fields.get("urgency_score", default_event.urgency_score)
        is_open = llm_fields.get("is_open", default_event.is_open)
        project = llm_fields.get("project", default_event.project)
        subsystem = llm_fields.get("subsystem", default_event.subsystem)
        participants = llm_fields.get("participants", default_event.participants)
        summary = llm_fields.get("summary", default_event.summary)
        relevant_roles = llm_fields.get("relevant_roles", default_event.relevant_roles)

        # Event identity is stable for a given thread/channel/summary combination.
        # This supports idempotent upsert behavior in storage.
        digest_input = f"{thread_ts}:{channel_id}:{summary}"
        event_id = hashlib.sha1(digest_input.encode("utf-8")).hexdigest()[:16]
        # Group key used downstream to collapse near-duplicate events in ranking.
        dedupe_group = f"{project}:{subsystem}".lower()

        # Emit final structured event with model-backed fields and confidence metadata.
        return StructuredEvent(
            event_id=event_id,
            thread_ts=thread_ts,
            channel_id=channel_id,
            summary=summary,
            event_type=event_type,
            project=project,
            subsystem=subsystem,
            participants=participants,
            urgency_score=urgency_score,
            relevant_roles=relevant_roles,
            is_open=is_open,
            source_thread_link=self._build_source_thread_link(channel_id=channel_id, thread_ts=thread_ts),
            created_at=datetime.now(timezone.utc),
            confidence=confidence,
            dedupe_group_id=dedupe_group,
            last_seen_at=datetime.now(timezone.utc),
            needs_reprocess=False,
        )

    def _deterministic_extract(self, thread_ts: str, channel_id: str, messages: list[Message]) -> StructuredEvent:
        # Deterministic pass is intentionally simple and fully local (no network/model dependency).
        merged_text = " ".join(m.text.strip() for m in messages)
        event_type = self._classify_event_type(merged_text)
        urgency_score = self._urgency_score(merged_text, event_type)
        is_open = self._is_open(merged_text)

        # Project/subsystem heuristics are coarse but stable and low-cost.
        project = self._extract_project(channel_id)
        subsystem = self._extract_subsystem(merged_text)
        # Participants are inferred from message authors observed in thread.
        participants = sorted({m.user_id for m in messages})
        summary = self._factual_summary(messages)
        relevant_roles = self._relevant_roles(event_type, subsystem)

        digest_input = f"{thread_ts}:{channel_id}:{summary}"
        event_id = hashlib.sha1(digest_input.encode("utf-8")).hexdigest()[:16]
        dedupe_group = f"{project}:{subsystem}".lower()

        return StructuredEvent(
            event_id=event_id,
            thread_ts=thread_ts,
            channel_id=channel_id,
            summary=summary,
            event_type=event_type,
            project=project,
            subsystem=subsystem,
            participants=participants,
            urgency_score=urgency_score,
            relevant_roles=relevant_roles,
            is_open=is_open,
            source_thread_link=self._build_source_thread_link(channel_id=channel_id, thread_ts=thread_ts),
            created_at=datetime.now(timezone.utc),
            confidence=0.0,
            dedupe_group_id=dedupe_group,
            last_seen_at=datetime.now(timezone.utc),
            needs_reprocess=True,
        )

    def _extract_with_llm(self, channel_id: str, messages: list[Message]) -> dict:
        """Call LLM extractor and sanitize output into allowed schema values."""
        # Serialize thread with timestamps and user ids to preserve conversational context.
        thread_blob = "\n".join(f"{m.ts} | {m.user_id}: {m.text}" for m in messages)
        system_prompt = (
            "You extract high-fidelity structured operational events from hardware engineering Slack threads. "
            "Return strict JSON only. Do not invent facts."
        )
        user_prompt = (
            "Extract a single event from this thread.\n"
            "Allowed event_type: blocker, risk, decision, status_update, unresolved_question\n"
            "Allowed relevant_roles: hardware_engineer, firmware_engineer, pm, engineer\n"
            "Return JSON with: summary, event_type, project, subsystem, participants, urgency_score, "
            "relevant_roles, is_open, confidence.\n"
            "confidence must be 0..1 and represent extraction certainty.\n"
            "summary must be factual and concise.\n"
            f"channel_id: {channel_id}\n"
            f"thread:\n{thread_blob}"
        )

        try:
            raw = self.llm_client.json_completion(system_prompt=system_prompt, user_prompt=user_prompt, temperature=0.1)
        except OpenAIAPIError:
            # Caller handles empty dict as model-unavailable/invalid output.
            return {}

        out: dict = {}
        # Validate and coerce each field defensively before using it.
        event_type_raw = str(raw.get("event_type", "")).strip()
        if event_type_raw in {e.value for e in EventType}:
            out["event_type"] = EventType(event_type_raw)

        urgency_raw = raw.get("urgency_score")
        if isinstance(urgency_raw, (int, float)):
            out["urgency_score"] = min(1.0, max(0.0, float(urgency_raw)))

        confidence_raw = raw.get("confidence")
        if isinstance(confidence_raw, (int, float)):
            out["confidence"] = min(1.0, max(0.0, float(confidence_raw)))

        if isinstance(raw.get("is_open"), bool):
            out["is_open"] = raw["is_open"]

        if isinstance(raw.get("project"), str) and raw["project"].strip():
            out["project"] = raw["project"].strip().lower().replace(" ", "_")

        if isinstance(raw.get("subsystem"), str) and raw["subsystem"].strip():
            out["subsystem"] = raw["subsystem"].strip().lower().replace(" ", "_")

        if isinstance(raw.get("summary"), str) and raw["summary"].strip():
            out["summary"] = raw["summary"].strip()

        participants = raw.get("participants")
        if isinstance(participants, list):
            # Keep unique, non-empty participant ids only.
            cleaned_participants = sorted({str(p).strip() for p in participants if str(p).strip()})
            if cleaned_participants:
                out["participants"] = cleaned_participants

        relevant_roles = raw.get("relevant_roles")
        if isinstance(relevant_roles, list):
            allowed_roles = {"hardware_engineer", "firmware_engineer", "pm", "engineer"}
            # Constrain roles to known enum-like set for downstream ranking consistency.
            cleaned_roles = sorted({str(r).strip() for r in relevant_roles if str(r).strip() in allowed_roles})
            if cleaned_roles:
                out["relevant_roles"] = cleaned_roles

        return out

    def _classify_event_type(self, text: str) -> EventType:
        lower = text.lower()
        for event_type, keywords in self.EVENT_KEYWORDS.items():
            if any(keyword in lower for keyword in keywords):
                return event_type
        return EventType.STATUS_UPDATE

    def _urgency_score(self, text: str, event_type: EventType) -> float:
        lower = text.lower()
        score = 0.2
        if event_type in {EventType.BLOCKER, EventType.RISK}:
            score += 0.35
        if "cannot" in lower or "critical" in lower or "high" in lower:
            score += 0.25
        if "ship" in lower or "customer" in lower:
            score += 0.15
        return min(1.0, score)

    def _is_open(self, text: str) -> bool:
        lower = text.lower()
        resolved_tokens = ("resolved", "fixed", "done", "closed", "pass", "passed", "✅")
        return not any(token in lower for token in resolved_tokens)

    def _extract_project(self, channel_id: str) -> str:
        if channel_id == "C1":
            return "atlas"
        if channel_id == "C2":
            return "bringup"
        return "unknown"

    def _extract_subsystem(self, text: str) -> str:
        lower = text.lower()
        if "thermal" in lower:
            return "thermal"
        if "firmware" in lower:
            return "firmware"
        if "power" in lower:
            return "power"
        if "sensor" in lower:
            return "sensor"
        return "general"

    def _factual_summary(self, messages: list[Message]) -> str:
        first = messages[0].text.strip() if messages else ""
        follow_ups = max(0, len(messages) - 1)
        if follow_ups:
            return f"{first} ({follow_ups} follow-up replies)."
        return first

    def _relevant_roles(self, event_type: EventType, subsystem: str) -> list[str]:
        base = ["hardware_engineer", "firmware_engineer", "pm"]
        if event_type == EventType.BLOCKER:
            return ["hardware_engineer", "pm"]
        if subsystem == "firmware":
            return ["firmware_engineer", "pm"]
        return base

    def _build_source_thread_link(self, channel_id: str, thread_ts: str) -> str:
        """Build a stable URL pointing at the thread root message."""
        team_id = os.getenv("SLACK_TEAM_ID", "").strip()
        if team_id:
            return f"https://slack.com/app_redirect?team={team_id}&channel={channel_id}&message_ts={thread_ts}"

        # Canonical thread-root permalink form (works when user is in the target workspace/channel).
        ts_digits = "".join(ch for ch in thread_ts if ch.isdigit())
        ts_padded = (ts_digits + "0" * 16)[:16]
        return f"https://slack.com/archives/{channel_id}/p{ts_padded}"
