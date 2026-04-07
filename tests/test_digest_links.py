from daily_digest_bot.digest import DigestGenerator
from daily_digest_bot.llm import OpenAIClient


class FakeLLMClient(OpenAIClient):
    def __init__(self) -> None:
        super().__init__(api_key="test", model="test-model")

    def json_completion(self, *, system_prompt: str, user_prompt: str, temperature: float = 0.2) -> dict:
        return {}

    def text_completion(self, *, system_prompt: str, user_prompt: str, temperature: float = 0.3) -> str:
        return ""


def test_display_thread_link_converts_app_redirect_to_archives_when_team_missing(monkeypatch) -> None:
    monkeypatch.delenv("SLACK_TEAM_ID", raising=False)
    generator = DigestGenerator(llm_client=FakeLLMClient())

    link = generator._display_thread_link("https://slack.com/app_redirect?channel=C0AR5UBKKMF&message_ts=1775274328.274209")
    assert link == "https://slack.com/archives/C0AR5UBKKMF/p1775274328274209"


def test_display_thread_link_converts_app_redirect_to_app_client_when_team_present(monkeypatch) -> None:
    monkeypatch.setenv("SLACK_TEAM_ID", "T12345")
    generator = DigestGenerator(llm_client=FakeLLMClient())

    link = generator._display_thread_link("https://slack.com/app_redirect?channel=C0AR5UBKKMF&message_ts=1775274328.274209")
    assert link == "https://app.slack.com/client/T12345/C0AR5UBKKMF/thread/C0AR5UBKKMF-1775274328274209"


def test_estimate_linked_items_counts_slack_link_formats() -> None:
    generator = DigestGenerator(llm_client=FakeLLMClient())
    text = "\n".join(
        [
            "- <https://slack.com/app_redirect?channel=C1&message_ts=1775274328.274209|Open thread>",
            "- https://slack.com/app_redirect?team=T12345&channel=C2&message_ts=1775274328.274209",
            "- https://slack.com/archives/C3/p1775274328274209",
            "- <https://app.slack.com/client/T12345/C4/thread/C4-1775274328274209|Open thread>",
        ]
    )

    assert generator._estimate_linked_items(text) == 4
