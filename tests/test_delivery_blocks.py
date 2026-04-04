from daily_digest_bot.delivery import SlackDeliveryClient


def test_build_blocks_formats_digest_sections_with_dividers() -> None:
    client = SlackDeliveryClient(bot_token="xoxb-test")
    text = """Daily hardware digest for Avery

Top priorities
- Priority item

Open blockers / risks
- Risk item

Decisions made
- Decision item

What this means for you
- Take action"""

    blocks = client._build_blocks(text=text, run_id="r1", event_ids=["E1", "E2"])

    assert blocks[0]["type"] == "section"
    assert "*Daily Hardware Digest*" in blocks[0]["text"]["text"]

    section_texts = [b["text"]["text"] for b in blocks if b["type"] == "section"]
    assert any(t.startswith("*Top priorities*") for t in section_texts)
    assert any(t.startswith("*Open blockers / risks*") for t in section_texts)
    assert any(t.startswith("*Decisions made*") for t in section_texts)
    assert any(t.startswith("*What this means for you*") for t in section_texts)

    assert blocks[-2]["type"] == "divider"
    assert blocks[-1]["type"] == "actions"


def test_build_blocks_falls_back_to_single_section_when_unstructured() -> None:
    client = SlackDeliveryClient(bot_token="xoxb-test")
    text = "Quick one-line update without known digest headers"

    blocks = client._build_blocks(text=text, run_id="r1", event_ids=[])

    assert blocks[0]["type"] == "section"
    assert blocks[0]["text"]["text"] == text
    assert blocks[-1]["type"] == "actions"
