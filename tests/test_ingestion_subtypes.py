from daily_digest_bot.ingestion import SlackWebClient


def test_fetch_channel_messages_keeps_bot_message_and_skips_other_subtypes() -> None:
    client = SlackWebClient(bot_token="xoxb-test", channel_ids=["C1"])

    def fake_api_get(method: str, params: dict[str, str]) -> dict:
        assert method == "conversations.history"
        return {
            "messages": [
                {"ts": "1.0001", "user": "U1", "text": "human root"},
                {"ts": "1.0002", "subtype": "bot_message", "user": "B1", "text": "bot content"},
                {"ts": "1.0003", "subtype": "channel_join", "user": "U2", "text": "joined"},
            ],
            "response_metadata": {},
        }

    client._api_get = fake_api_get  # type: ignore[method-assign]
    out = client.fetch_channel_messages(channel_id="C1", oldest_ts="0")

    assert [m.text for m in out] == ["human root", "bot content"]


def test_fetch_thread_replies_keeps_bot_message_and_skips_other_subtypes() -> None:
    client = SlackWebClient(bot_token="xoxb-test", channel_ids=["C1"])

    def fake_api_get(method: str, params: dict[str, str]) -> dict:
        assert method == "conversations.replies"
        return {
            "messages": [
                {"ts": "2.0001", "user": "U1", "text": "thread root"},
                {"ts": "2.0002", "subtype": "bot_message", "user": "B1", "text": "bot reply"},
                {"ts": "2.0003", "subtype": "channel_topic", "user": "U2", "text": "topic change"},
            ]
        }

    client._api_get = fake_api_get  # type: ignore[method-assign]
    out = client.fetch_thread_replies(channel_id="C1", thread_ts="2.0001")

    assert [m.text for m in out] == ["thread root", "bot reply"]
    assert out[1].thread_ts == "2.0001"
