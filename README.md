# Daily Digest Slack Bot (Lean v1)

Lightweight, personalized daily digest bot for hardware engineering teams.

## Lean workflow

1. Incremental ingestion
- Pull only new Slack activity per channel using `ingestion_state` watermark.
- For newly touched threaded roots, refresh full thread replies.

2. Structured event extraction
- Convert touched threads into structured events via OpenAI (with deterministic fallback).
- Store confidence for observability.

3. Personalization + ranking
- Recipients are opt-in (`user_profiles.digest_enabled = 1`).
- Rank with fixed lean features: role/ownership/project match, urgency, recency, open issue bonus.
- Dedupe in-memory by project/subsystem.

4. Digest generation
- Generate action-oriented DM sections from structured events:
  - What Needs Attention Today
  - Active Blockers & Risks
  - Decisions & Calls Needed
  - Recommended Next Actions
- Enforce high source-link coverage via fallback if needed.

5. Delivery + tracking
- DM-only delivery.
- Record `digest_runs` to avoid duplicate sends per user/day.
- Weekday local scheduling (default 9am), with `--force-send` override.

6. Retention
- Single retention policy (`RETENTION_DAYS`, default 90).

## Architecture

See [docs/architecture.md](docs/architecture.md) for the refactor baseline and module map.
See [docs/code-reference.md](docs/code-reference.md) for module-level behavior and runbook details.
See [docs/pipeline-function-walkthrough.md](docs/pipeline-function-walkthrough.md) for line-by-line pipeline function explanations.

## Required Slack scopes

- `users:read`
- `channels:read`
- `groups:read`
- `channels:history`
- `groups:history`
- `chat:write`
- `im:write`
- Optional for auto-join public channels: `channels:join`

## Configuration

```bash
SLACK_BOT_TOKEN=xoxb-...
SLACK_CHANNEL_IDS=C01234567,C07654321

OPENAI_API_KEY=sk-...
OPENAI_MODEL=gpt-4.1

DIGEST_RECIPIENT_MODE=opt_in
DIGEST_LOCAL_HOUR=9
RETENTION_DAYS=90
```

`OPENAI_API_KEY` is required for `--run-digest` execution.

## Run

Demo dry run:

```bash
python -m daily_digest_bot.main --seed-demo-data --run-digest --dry-run --force-send
```

Real Slack dry run:

```bash
python -m daily_digest_bot.main --run-digest --dry-run --force-send
```

Scheduled real send:

```bash
python -m daily_digest_bot.main --run-digest
```

## Seed Slack test traffic

Populate your Slack test channels with hardware-engineering threads and replies.

```bash
python scripts/seed_slack_test_data.py \
  --slack-bot-token "$SLACK_BOT_TOKEN" \
  --slack-channel-ids "$SLACK_CHANNEL_IDS" \
  --threads-per-channel 10
```

Use OpenAI generation with a smaller model:

```bash
python scripts/seed_slack_test_data.py \
  --slack-bot-token "$SLACK_BOT_TOKEN" \
  --slack-channel-ids "$SLACK_CHANNEL_IDS" \
  --generator openai \
  --openai-api-key "$OPENAI_API_KEY" \
  --openai-model gpt-4.1-mini
```

Preview only (no posts):

```bash
python scripts/seed_slack_test_data.py \
  --dry-run \
  --slack-channel-ids "$SLACK_CHANNEL_IDS"
```

Generate one continuous flowing conversation (example: 500 messages):

```bash
python scripts/seed_slack_test_data.py \
  --slack-bot-token "$SLACK_BOT_TOKEN" \
  --slack-channel-ids "$SLACK_CHANNEL_IDS" \
  --mode flowing \
  --total-messages 500 \
  --sleep-seconds 0.1
```

## Test

```bash
python -m pytest -q
```
