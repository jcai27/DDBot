from __future__ import annotations

import json

from daily_digest_bot.delivery import SlackDeliveryClient, StdoutDeliveryClient
from daily_digest_bot.ingestion import DemoSlackClient, IngestionService, SlackWebClient
from daily_digest_bot.llm import OpenAIClient
from daily_digest_bot.pipeline import DailyDigestPipeline
from daily_digest_bot.store import Store

from .config import AppConfig, config_from_args, parse_args


def build_pipeline(config: AppConfig) -> DailyDigestPipeline:
    """Compose concrete dependencies and return a ready-to-run pipeline."""
    store = Store(db_path=config.db_path)

    if config.seed_demo_data:
        slack_client = DemoSlackClient()
    else:
        if not config.slack_bot_token:
            raise ValueError("Missing Slack bot token. Set --slack-bot-token or SLACK_BOT_TOKEN.")
        slack_client = SlackWebClient(bot_token=config.slack_bot_token, channel_ids=config.slack_channel_ids)

    if not config.dry_run and not config.slack_bot_token:
        raise ValueError("Missing Slack bot token for delivery. Set --slack-bot-token or SLACK_BOT_TOKEN.")

    delivery_client = StdoutDeliveryClient() if config.dry_run else SlackDeliveryClient(bot_token=config.slack_bot_token)

    if not config.openai_api_key:
        raise ValueError("Missing OpenAI API key. Set --openai-api-key or OPENAI_API_KEY.")
    llm_client = OpenAIClient(api_key=config.openai_api_key, model=config.openai_model)

    return DailyDigestPipeline(
        store=store,
        ingestion_service=IngestionService(store=store, slack_client=slack_client),
        delivery_client=delivery_client,
        extract_llm_client=llm_client,
        digest_llm_client=llm_client,
        digest_recipient_mode=config.digest_recipient_mode,
        digest_local_hour=config.digest_local_hour,
        retention_days=config.retention_days,
    )


def run_from_cli() -> None:
    """CLI entry wrapper: parse config, build pipeline, optionally execute run."""
    config = config_from_args(parse_args())
    if not config.run_digest:
        return

    pipeline = build_pipeline(config)
    metrics = pipeline.run(force_send=config.force_send)
    print("\n=== METRICS ===")
    print(json.dumps(metrics, indent=2, sort_keys=True))
