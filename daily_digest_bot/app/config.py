from __future__ import annotations

import argparse
import os
from dataclasses import dataclass


@dataclass(slots=True)
class AppConfig:
    """Normalized runtime configuration used to compose the application."""
    db_path: str
    run_digest: bool
    seed_demo_data: bool
    dry_run: bool
    force_send: bool
    slack_bot_token: str
    slack_channel_ids: list[str]
    openai_api_key: str
    openai_model: str
    digest_recipient_mode: str
    digest_local_hour: int
    retention_days: int


def parse_args() -> argparse.Namespace:
    """Define and parse CLI flags with env-backed defaults."""
    parser = argparse.ArgumentParser(description="Daily digest Slack bot v1")
    parser.add_argument("--db-path", default="digest.db")
    parser.add_argument("--run-digest", action="store_true", help="Run daily digest pipeline")
    parser.add_argument("--seed-demo-data", action="store_true", help="Use demo Slack data source")
    parser.add_argument("--dry-run", action="store_true", help="Print output instead of Slack DM")
    parser.add_argument("--force-send", action="store_true", help="Bypass local schedule window checks")

    parser.add_argument("--slack-bot-token", default=os.getenv("SLACK_BOT_TOKEN", ""))
    parser.add_argument(
        "--slack-channel-ids",
        default=os.getenv("SLACK_CHANNEL_IDS", ""),
        help="Comma-separated Slack channel IDs for ingestion, e.g. C123,C456",
    )

    parser.add_argument("--openai-api-key", default=os.getenv("OPENAI_API_KEY", ""))
    parser.add_argument("--openai-model", default=os.getenv("OPENAI_MODEL", "gpt-4.1"))
    parser.add_argument("--digest-recipient-mode", default=os.getenv("DIGEST_RECIPIENT_MODE", "opt_in"))
    parser.add_argument("--digest-local-hour", type=int, default=int(os.getenv("DIGEST_LOCAL_HOUR", "9")))
    parser.add_argument("--retention-days", type=int, default=int(os.getenv("RETENTION_DAYS", "90")))

    return parser.parse_args()


def config_from_args(args: argparse.Namespace) -> AppConfig:
    """Convert argparse output into a strongly-typed AppConfig object."""
    channel_ids = [c.strip() for c in args.slack_channel_ids.split(",") if c.strip()]
    return AppConfig(
        db_path=args.db_path,
        run_digest=bool(args.run_digest),
        seed_demo_data=bool(args.seed_demo_data),
        dry_run=bool(args.dry_run),
        force_send=bool(args.force_send),
        slack_bot_token=args.slack_bot_token,
        slack_channel_ids=channel_ids,
        openai_api_key=args.openai_api_key,
        openai_model=args.openai_model,
        digest_recipient_mode=args.digest_recipient_mode,
        digest_local_hour=int(args.digest_local_hour),
        retention_days=int(args.retention_days),
    )
