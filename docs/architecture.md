# Codebase Architecture (Refactor Baseline)

This document describes the current organization after the April 4, 2026 cleanup pass.

## Goals of the cleanup

- Reduce dead or unused code paths.
- Make startup/wiring code easier to understand.
- Keep behavior and runtime flags stable.
- Preserve test coverage and avoid risky broad rewrites.
- Document the new structure and cleanup decisions.

## Current top-level structure

- `daily_digest_bot/main.py`
  - Thin CLI entrypoint only. Delegates to app bootstrap.
- `daily_digest_bot/app/`
  - `config.py`: CLI arg parsing and `AppConfig` dataclass.
  - `bootstrap.py`: dependency wiring (`Store`, Slack clients, LLM client, pipeline).
- `daily_digest_bot/pipeline.py`
  - Orchestrates ingestion, extraction, ranking, digest generation, delivery, and retention.
- `daily_digest_bot/ingestion.py`
  - Slack data fetch logic + ingestion workflow.
- `daily_digest_bot/thread_processing.py`
  - Converts thread messages to structured events.
- `daily_digest_bot/digest.py`
  - Generates digest text (LLM + fallback) and normalizes display links.
- `daily_digest_bot/delivery.py`
  - Slack/stdout delivery clients and Slack block formatting.
- `daily_digest_bot/slack_api.py`
  - Shared Slack HTTP client and error type used by ingestion and delivery.
- `daily_digest_bot/store.py`
  - Thin compatibility facade over repository modules.
- `daily_digest_bot/db/`
  - `connection.py`: SQLite connection/context manager.
  - `schema.py`: schema + migration helpers.
  - `repositories/`: focused persistence modules by domain.
- `scripts/seed_slack_test_data.py`
  - Standalone seeding tool for generating synthetic Slack traffic.

## Removed dead code in this pass

1. Unused `FeedbackService` field from `DailyDigestPipeline`.
   - `pipeline.py` previously instantiated and stored it but never used.

2. Unused `thread_state` persistence path from `Store`.
   - Removed table creation/index and related methods:
     - `upsert_thread_state`
     - `mark_thread_processed`
     - `list_threads_needing_processing`
   - Removed retention delete that targeted `thread_state`.
   - The ingestion flow already uses `ingestion_state` watermarks and does not call these methods.

3. Unused feedback helper modules and dataclasses.
   - Removed files:
     - `daily_digest_bot/feedback.py`
     - `daily_digest_bot/interactions.py`
   - Removed dataclasses from `models.py`:
     - `Channel`
     - `Thread`

4. Store split into repositories (Phase 2).
   - Moved persistence logic into:
     - `db/repositories/users_repo.py`
     - `db/repositories/channels_repo.py`
     - `db/repositories/messages_repo.py`
     - `db/repositories/events_repo.py`
     - `db/repositories/digest_runs_repo.py`
     - `db/repositories/feedback_repo.py`
   - `Store` now delegates to these repositories and preserves the old interface.

5. Shared Slack transport (Phase 3).
   - Added `slack_api.py` with:
     - `SlackApiClient.api_get(...)`
     - `SlackApiClient.api_post(...)`
     - `SlackApiError`
   - `ingestion.py` and `delivery.py` now share this client instead of duplicating request code.

## Compatibility and behavior

- Existing CLI flags and env var behavior remain unchanged.
- Existing tests continue to pass after cleanup (`pytest -q`).
- `daily_digest_bot.main` import path remains valid.

## Next recommended cleanup phases

1. Move seed script (`scripts/seed_slack_test_data.py`) to package tools and reuse `SlackApiClient`.
2. Separate Slack message block formatting (`delivery.py`) into a dedicated formatter module.
3. Add a thin service layer around `thread_processing` and `digest` for clearer orchestration boundaries.
