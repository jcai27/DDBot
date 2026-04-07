what# Pipeline Function Walkthrough

This is a line-by-line (as needed) walkthrough of the most important product path.

## 1) `DailyDigestPipeline.run`
Source: `daily_digest_bot/pipeline.py:41`

### What this function does
Runs one full product cycle: ingest -> structure events -> rank -> generate digest -> deliver -> record metrics.

### Line-by-line walkthrough

- `43`: Resolve clock (`now`) so tests can pass deterministic timestamps.
- `44`: Ensure DB schema exists before any reads/writes.
- `46`: Pull fresh Slack data via ingestion service.
- `47`: Extract touched thread identifiers returned by ingestion.
- `48`: Convert touched threads to structured events and compute extraction counters.
- `49`: Normalize dedupe groups after extraction (project/subsystem grouping).
- `51`: Load users into dict for quick id lookup during delivery loop.
- `52`: Load recent events window used for ranking and profile bootstrap.
- `54-60`: Bootstrap missing user profiles from recent activity.
- `61`: Pull carryover open/high-urgency events.
- `62`: Merge recent + carryover into one candidate pool.
- `64-66`: Initialize aggregate send/link counters for metrics.
- `68`: Select only digest-enabled profiles as recipients.
- `69-72`: Skip profiles whose user row no longer exists.
- `73-74`: Never DM Slackbot/system identity.
- `75-76`: Apply schedule guards unless `force_send=True`.
- `78`: Rank candidate events for this specific recipient profile.
- `79-84`: Generate digest text + item/link counts.
- `86`: Generate unique run id for traceability.
- `87`: Capture top event ids for feedback context.
- `88`: Send digest to recipient through delivery adapter.
- `90`: Compute recipient-local digest date (for once-per-day dedupe).
- `91-99`: Build `DigestRun` persistence model.
- `100`: Persist run record.
- `102-104`: Update aggregate send/link counters.
- `106-109`: Apply retention cleanup.
- `111-122`: Build final metrics payload.
- `123`: Return metrics to caller.

### Product ties

- Ingestion freshness directly controls what appears in digests.
- Ranking + digest generation determine relevance and readability.
- Digest run recording prevents duplicate sends per local day.

## 2) `IngestionService.run`
Source: `daily_digest_bot/ingestion.py:292`

### What this function does
Incrementally syncs Slack users and channel messages into local storage.

### Line-by-line walkthrough

- `294-295`: Fetch users/channels from configured Slack source (`SlackWebClient` or demo).
- `297-298`: Upsert users first so message foreign keys resolve cleanly.
- `300-302`: Initialize counters and touched-thread accumulator.
- `303-305`: Upsert channel metadata.
- `307-310`: Read per-channel watermark, fallback to backfill window on first run.
- `311`: Fetch only new channel messages after watermark.
- `312-313`: Track changed thread roots and max timestamp seen.
- `314-320`: Upsert each new message and mark thread roots needing full refresh.
- `322-333`: For each changed thread, fetch full replies, upsert all, update counters, and mark as touched.
- `335`: Persist updated channel watermark so next run is incremental.
- `337-343`: Return ingestion stats consumed by pipeline metrics.

### Product ties

- This is the freshness gate: if this misses data, downstream ranking/digest quality drops.
- Watermarks make recurring runs cheap and avoid full-history scans.

## 3) `ThreadProcessor.process_thread`
Source: `daily_digest_bot/thread_processing.py:25`

### What this function does
Transforms one Slack thread into one `StructuredEvent` used by ranking and digest generation.

### Line-by-line walkthrough

- `28`: Build deterministic baseline extraction first (safe fallback).
- `30-31`: Attempt LLM extraction + confidence score.
- `33-36`: If confidence too low, return baseline event and mark `needs_reprocess=True`.
- `38-45`: Merge LLM fields with baseline defaults field-by-field.
- `47-49`: Build stable event id and dedupe key from thread/channel/summary.
- `51-69`: Return final structured event object with normalized link and metadata.

### Product ties

- This function defines the semantic “atoms” the product ranks and summarizes.
- Confidence fallback protects reliability when model output is weak.

## 4) `RankingEngine.rank`
Source: `daily_digest_bot/ranking.py:19`

### What this function does
Scores all candidate events for a user profile, dedupes, and orders descending.

### Line-by-line walkthrough

- `21-22`: Setup accumulator + dedupe map.
- `24-30`: Score each event and keep best-scoring event per dedupe group.
- `32-34`: Convert dedupe map to list and sort highest-first.

Supporting score path:
- `_score` (`36-54`): weighted blend of role/ownership/project/urgency/recency/open status.
- `_recency_score` (`56-69`): bucketized age-to-score function.

### Product ties

- This decides what users see first.
- Dedupe prevents repeated variants of the same issue from crowding digest space.

## 5) `DigestGenerator.build_digest`
Source: `daily_digest_bot/digest.py:18`

### What this function does
Creates final user-facing digest text with LLM-first strategy and deterministic fallback.

### Line-by-line walkthrough

- `26`: Clamp ranked list to `max_items`.
- `27`: Try LLM digest generation.
- `28-32`: Accept LLM output only when link coverage policy passes.
- `34-35`: Fallback to deterministic formatter otherwise.

Supporting paths:
- `_build_with_llm` (`37-91`): shape structured event/profile payload + prompt required sections.
- `_build_fallback` (`93-136`): deterministic sectioned digest text.
- `_display_thread_link` (`166-187`): normalize links into reliable clickable URLs.

### Product ties

- Controls final readability and actionability in user DM.
- Link policy enforces traceability back to source conversation.

## 6) `SlackDeliveryClient.send_dm`
Source: `daily_digest_bot/delivery.py:28`

### What this function does
Writes digest to Slack DM using Block Kit formatting and feedback controls.

### Line-by-line walkthrough

- `30`: Open/get DM channel id for recipient.
- `31`: Build block payload from digest text + run metadata.
- `32-36`: Compose final Slack message payload.
- `37`: Post to `chat.postMessage`.

Supporting paths:
- `_build_blocks` (`48-75`): section blocks + feedback buttons.
- `_format_digest_blocks` (`77-132`): parse digest text into titled Slack sections.
- `_api_post` (`134-139`): shared Slack API client call + error normalization.

### Product ties

- This is the last mile to user-visible product output.
- Feedback buttons are the hook for future learning loops.

## 7) Orchestration helper functions in pipeline
Source: `daily_digest_bot/pipeline.py`

- `_process_threads_to_events` (`125-146`): pull thread messages from store, process with `ThreadProcessor`, persist events, count low confidence.
- `_merge_candidates` (`148-153`): union recent and carryover by `event_id`.
- `_should_send_for_user` (`155-174`): enforce weekday/time window and once-per-local-day dedupe.
- `_local_date_str` (`176-182`): timezone-safe local-date conversion for run dedupe keys.

## 8) How the important functions tie together (single run)

1. `IngestionService.run` writes fresh users/messages and returns touched thread ids.
2. `DailyDigestPipeline._process_threads_to_events` calls `ThreadProcessor.process_thread` for each touched thread.
3. Pipeline loads candidate events and user profiles.
4. `RankingEngine.rank` orders event relevance per user.
5. `DigestGenerator.build_digest` turns top-ranked events into digest text.
6. `SlackDeliveryClient.send_dm` sends digest to recipient.
7. Pipeline records `DigestRun` and returns metrics.
