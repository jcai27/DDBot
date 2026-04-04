from __future__ import annotations

from abc import ABC, abstractmethod

from daily_digest_bot.slack_api import SlackApiClient, SlackApiError


class DigestDeliveryClient(ABC):
    """Abstract digest delivery interface."""
    @abstractmethod
    def send_dm(self, user_id: str, text: str, run_id: str | None = None, event_ids: list[str] | None = None) -> None:
        raise NotImplementedError


class StdoutDeliveryClient(DigestDeliveryClient):
    """Delivery adapter that prints digest content to stdout for dry runs."""
    def send_dm(self, user_id: str, text: str, run_id: str | None = None, event_ids: list[str] | None = None) -> None:
        print(f"\n=== DM to {user_id} run_id={run_id or '-'} ===")
        print(text)


class SlackDeliveryClient(DigestDeliveryClient):
    """Slack DM delivery adapter with block formatting and feedback controls."""
    def __init__(self, bot_token: str) -> None:
        self.bot_token = bot_token
        self.api = SlackApiClient(bot_token=bot_token)

    def send_dm(self, user_id: str, text: str, run_id: str | None = None, event_ids: list[str] | None = None) -> None:
        """Open a DM with user_id and post digest content plus metadata blocks."""
        channel_id = self._open_dm(user_id=user_id)
        blocks = self._build_blocks(text=text, run_id=run_id, event_ids=event_ids or [])
        payload = {
            "channel": channel_id,
            "text": text,
            "blocks": blocks,
        }
        self._api_post("chat.postMessage", payload)

    def _open_dm(self, user_id: str) -> str:
        """Open or retrieve the DM channel id for a target user id."""
        payload = self._api_post("conversations.open", {"users": user_id})
        channel = payload.get("channel", {})
        channel_id = channel.get("id")
        if not channel_id:
            raise RuntimeError(f"Unable to open DM channel for user {user_id}")
        return channel_id

    def _build_blocks(self, text: str, run_id: str | None, event_ids: list[str]) -> list[dict]:
        """Build Slack Block Kit payload with digest body and feedback buttons."""
        event_token = ",".join(event_ids[:3])
        useful_value = f"run:{run_id or ''}|sig:useful|events:{event_token}"
        not_useful_value = f"run:{run_id or ''}|sig:not_useful|events:{event_token}"
        blocks: list[dict] = []
        blocks.extend(self._format_digest_blocks(text))
        blocks.append({"type": "divider"})
        blocks.append(
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "Useful"},
                        "value": useful_value,
                        "action_id": "digest_feedback_useful",
                    },
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "Not useful"},
                        "value": not_useful_value,
                        "action_id": "digest_feedback_not_useful",
                    },
                ],
            }
        )
        return blocks

    def _format_digest_blocks(self, text: str) -> list[dict]:
        """Split digest text into titled section blocks with visual separators."""
        section_titles = [
            "What Needs Attention Today",
            "Active Blockers & Risks",
            "Decisions & Calls Needed",
            "Recommended Next Actions",
            "Top priorities",
            "Open blockers / risks",
            "Decisions made",
            "What this means for you",
        ]
        lines = [line.rstrip() for line in text.splitlines()]

        intro_lines: list[str] = []
        sections: list[tuple[str, list[str]]] = []
        current_title: str | None = None
        current_lines: list[str] = []

        for line in lines:
            if line in section_titles:
                if current_title is not None:
                    sections.append((current_title, current_lines))
                current_title = line
                current_lines = []
                continue
            if current_title is None:
                if line.strip():
                    intro_lines.append(line)
            else:
                current_lines.append(line)

        if current_title is not None:
            sections.append((current_title, current_lines))

        if not sections:
            return [{"type": "section", "text": {"type": "mrkdwn", "text": text}}]

        blocks: list[dict] = []
        header_text = "*Daily Hardware Digest*"
        if intro_lines:
            header_text += "\n" + "\n".join(intro_lines)
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": header_text}})

        for title, body_lines in sections:
            blocks.append({"type": "divider"})
            body = "\n".join(body_lines).strip()
            if not body:
                body = "- None"
            blocks.append(
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": f"*{title}*\n{body}"},
                }
            )
        return blocks

    def _api_post(self, method: str, payload: dict) -> dict:
        try:
            return self.api.api_post(method=method, payload=payload)
        except SlackApiError as exc:
            # Preserve historical RuntimeError behavior at delivery call sites.
            raise RuntimeError(str(exc)) from exc
