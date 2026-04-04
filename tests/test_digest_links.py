from daily_digest_bot.digest import DigestGenerator


def test_display_thread_link_converts_app_redirect_to_archives_when_team_missing(monkeypatch) -> None:
    monkeypatch.delenv("SLACK_TEAM_ID", raising=False)
    generator = DigestGenerator(llm_client=None)

    link = generator._display_thread_link("https://slack.com/app_redirect?channel=C0AR5UBKKMF&message_ts=1775274328.274209")
    assert link == "https://slack.com/archives/C0AR5UBKKMF/p1775274328274209"


def test_display_thread_link_converts_app_redirect_to_app_client_when_team_present(monkeypatch) -> None:
    monkeypatch.setenv("SLACK_TEAM_ID", "T12345")
    generator = DigestGenerator(llm_client=None)

    link = generator._display_thread_link("https://slack.com/app_redirect?channel=C0AR5UBKKMF&message_ts=1775274328.274209")
    assert link == "https://app.slack.com/client/T12345/C0AR5UBKKMF/thread/C0AR5UBKKMF-1775274328274209"
