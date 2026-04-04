from daily_digest_bot.thread_processing import ThreadProcessor


def test_build_source_thread_link_uses_team_redirect_when_set(monkeypatch) -> None:
    monkeypatch.setenv("SLACK_TEAM_ID", "T12345")
    tp = ThreadProcessor(llm_client=None)

    link = tp._build_source_thread_link(channel_id="C999", thread_ts="1775274328.274209")
    assert link == "https://slack.com/app_redirect?team=T12345&channel=C999&message_ts=1775274328.274209"


def test_build_source_thread_link_falls_back_to_archives_permalink(monkeypatch) -> None:
    monkeypatch.delenv("SLACK_TEAM_ID", raising=False)
    tp = ThreadProcessor(llm_client=None)

    link = tp._build_source_thread_link(channel_id="C999", thread_ts="1775274328.274209")
    assert link == "https://slack.com/archives/C999/p1775274328274209"
