# Code Reference

This document explains the important code paths in the `daily_digest_bot` project after the refactor.

## 1) Runtime entry and wiring

- Entry point: [`daily_digest_bot/main.py`](/Users/joshua/SlackBot/daily_digest_bot/main.py)
  - Very thin wrapper that calls `run_from_cli()`.

- App wiring: [`daily_digest_bot/app/bootstrap.py`](/Users/joshua/SlackBot/daily_digest_bot/app/bootstrap.py)
  - `build_pipeline(config)` composes `Store`, ingestion client, delivery client, required OpenAI client, and `DailyDigestPipeline`.
  - `run_from_cli()` parses config and executes pipeline when `--run-digest` is enabled.

- CLI + env config: [`daily_digest_bot/app/config.py`](/Users/joshua/SlackBot/daily_digest_bot/app/config.py)
  - `parse_args()` defines all flags.
  - `config_from_args()` normalizes args into `AppConfig`.

### Important runtime flags

- `--run-digest`: execute pipeline.
- `--seed-demo-data`: use deterministic demo ingestion client.
- `--dry-run`: print digest output to stdout instead of sending Slack DM.
- `--force-send`: bypass schedule constraints.
- `--db-path`: SQLite file path.

## 2) End-to-end pipeline

- Orchestrator: [`daily_digest_bot/pipeline.py`](/Users/joshua/SlackBot/daily_digest_bot/pipeline.py)

### `DailyDigestPipeline.run()` flow

1. Initialize schema.
2. Ingest latest Slack data (`IngestionService.run`).
3. Process touched threads into structured events (`ThreadProcessor`).
4. Dedupe event groups.
5. Bootstrap missing user profiles.
6. Build candidate event pool (recent + open high urgency).
7. Rank events per recipient (`RankingEngine`).
8. Build digest text (`DigestGenerator`).
9. Deliver digest (`DigestDeliveryClient`).
10. Record digest run and retention cleanup.

### Output metrics

`run()` returns a metrics dict with ingestion counts, processing counts, send counts, and link ratio.

## 3) Ingestion and Slack read path

- Ingestion module: [`daily_digest_bot/ingestion.py`](/Users/joshua/SlackBot/daily_digest_bot/ingestion.py)

### Main classes

- `SlackClient` (abstract interface)
- `SlackWebClient` (real Slack API)
- `DemoSlackClient` (local deterministic test/demo data)
- `IngestionService` (persists fetched users/messages/channels and tracks touched threads)

### Message filtering behavior

`SlackWebClient` ingests:
- normal messages (`subtype` absent)
- bot-authored chat messages (`subtype == "bot_message"`)

It skips other system subtypes.

### Watermark behavior

Per-channel watermark (`ingestion_state.last_history_ts`) drives incremental fetch.

## 4) Thread extraction and event generation

- Thread processing: [`daily_digest_bot/thread_processing.py`](/Users/joshua/SlackBot/daily_digest_bot/thread_processing.py)

### `ThreadProcessor.process_thread()`

- Runs deterministic extraction first.
- If LLM extraction confidence is below threshold, keeps deterministic event and marks `needs_reprocess=True`.
- If confidence is sufficient, applies LLM fields.

### Link generation

`_build_source_thread_link()` produces source links:
- `app_redirect` with `team=` when `SLACK_TEAM_ID` is set.
- fallback Slack archives permalink form otherwise.

## 5) Ranking and personalization

- Ranking: [`daily_digest_bot/ranking.py`](/Users/joshua/SlackBot/daily_digest_bot/ranking.py)
- Personalization bootstrap: [`daily_digest_bot/personalization.py`](/Users/joshua/SlackBot/daily_digest_bot/personalization.py)

### Ranking inputs

- role match
- ownership area match
- project match
- urgency
- recency
- novelty/open issue factors

User profile controls `max_items` and optional preferred delivery hour.

## 6) Digest text generation and formatting

- Digest generation: [`daily_digest_bot/digest.py`](/Users/joshua/SlackBot/daily_digest_bot/digest.py)

### Strategy

1. Try LLM digest generation with strict required sections.
2. Enforce minimum source-link ratio.
3. Fallback to deterministic digest when LLM output missing/low quality.

### Required section headings

- `What Needs Attention Today`
- `Active Blockers & Risks`
- `Decisions & Calls Needed`
- `Recommended Next Actions`

### Link normalization

`_display_thread_link()` rewrites old `app_redirect` links into more reliable URL forms for display.

## 7) Delivery and Slack write path

- Delivery: [`daily_digest_bot/delivery.py`](/Users/joshua/SlackBot/daily_digest_bot/delivery.py)

### Clients

- `StdoutDeliveryClient`: local dry-run output.
- `SlackDeliveryClient`: opens DM and posts Block Kit message.

### Slack block behavior

- Bold digest title.
- Divider-separated sections.
- Feedback buttons (`Useful`, `Not useful`).

## 8) Shared Slack API transport

- Shared transport: [`daily_digest_bot/slack_api.py`](/Users/joshua/SlackBot/daily_digest_bot/slack_api.py)

### Purpose

Single Slack HTTP implementation reused by ingestion and delivery.

### Key types

- `SlackApiClient`
- `SlackApiError`

## 9) Data model

- Dataclasses and enums: [`daily_digest_bot/models.py`](/Users/joshua/SlackBot/daily_digest_bot/models.py)

Important entities:
- `User`, `Message`, `StructuredEvent`, `UserProfile`, `RankedEvent`, `DigestRun`
- `EventType` enum

## 10) Persistence layer

- Facade: [`daily_digest_bot/store.py`](/Users/joshua/SlackBot/daily_digest_bot/store.py)
- DB connection: [`daily_digest_bot/db/connection.py`](/Users/joshua/SlackBot/daily_digest_bot/db/connection.py)
- Schema/migrations: [`daily_digest_bot/db/schema.py`](/Users/joshua/SlackBot/daily_digest_bot/db/schema.py)
- Repositories:
  - [`users_repo.py`](/Users/joshua/SlackBot/daily_digest_bot/db/repositories/users_repo.py)
  - [`channels_repo.py`](/Users/joshua/SlackBot/daily_digest_bot/db/repositories/channels_repo.py)
  - [`messages_repo.py`](/Users/joshua/SlackBot/daily_digest_bot/db/repositories/messages_repo.py)
  - [`events_repo.py`](/Users/joshua/SlackBot/daily_digest_bot/db/repositories/events_repo.py)
  - [`digest_runs_repo.py`](/Users/joshua/SlackBot/daily_digest_bot/db/repositories/digest_runs_repo.py)
  - [`feedback_repo.py`](/Users/joshua/SlackBot/daily_digest_bot/db/repositories/feedback_repo.py)

### Why facade + repos

- `Store` preserves existing call sites.
- Repositories isolate SQL by data domain.

## 11) Test map

- Pipeline behavior: [`tests/test_pipeline.py`](/Users/joshua/SlackBot/tests/test_pipeline.py)
- Ranking: [`tests/test_ranking.py`](/Users/joshua/SlackBot/tests/test_ranking.py)
- Delivery block formatting: [`tests/test_delivery_blocks.py`](/Users/joshua/SlackBot/tests/test_delivery_blocks.py)
- Ingestion subtype filter: [`tests/test_ingestion_subtypes.py`](/Users/joshua/SlackBot/tests/test_ingestion_subtypes.py)
- Thread/source links: [`tests/test_thread_links.py`](/Users/joshua/SlackBot/tests/test_thread_links.py), [`tests/test_digest_links.py`](/Users/joshua/SlackBot/tests/test_digest_links.py)

## 12) Operational runbook

### Local dry run

```bash
python -m daily_digest_bot.main --run-digest --dry-run --force-send
```

### Real send

```bash
python -m daily_digest_bot.main --run-digest --force-send
```

### Fresh DB test when recipients are stale

```bash
python -m daily_digest_bot.main --db-path /tmp/digest_fresh.db --run-digest --force-send
```

### Common issues

- `user_not_found` on `conversations.open`:
  - usually stale users in DB or wrong workspace token.
  - fix by fresh DB path and verify token/workspace alignment.

- Slack link opens invalid URL:
  - set `SLACK_TEAM_ID` in env for team-aware links.

- 429 rate limits while seeding:
  - use higher `--sleep-seconds` and rely on retry/backoff in seeder.
