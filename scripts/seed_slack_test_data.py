from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
import os
from urllib import parse
import random
import sys
import time
from urllib import error, request


@dataclass
class ThreadSeed:
    root: str
    replies: list[str]


class ApiError(RuntimeError):
    pass


class SlackSeeder:
    def __init__(self, bot_token: str, max_retries: int = 8) -> None:
        self.bot_token = bot_token
        self.max_retries = max_retries

    def post_message(self, channel_id: str, text: str, thread_ts: str | None = None) -> dict:
        payload: dict[str, str] = {"channel": channel_id, "text": text}
        if thread_ts:
            payload["thread_ts"] = thread_ts
        return self._api_post("chat.postMessage", payload)

    def channel_name(self, channel_id: str) -> str:
        payload = self._api_get("conversations.info", {"channel": channel_id})
        channel = payload.get("channel", {})
        return str(channel.get("name") or channel_id)

    def _api_get(self, method: str, params: dict[str, str]) -> dict:
        query = "&".join(f"{k}={parse.quote(v)}" for k, v in params.items())
        url = f"https://slack.com/api/{method}?{query}"
        req = request.Request(url=url, method="GET")
        req.add_header("Authorization", f"Bearer {self.bot_token}")
        payload = self._urlopen_json(req)
        if not payload.get("ok"):
            raise ApiError(f"Slack API error for {method}: {payload.get('error', 'unknown_error')}")
        return payload

    def _api_post(self, method: str, payload: dict) -> dict:
        body = json.dumps(payload).encode("utf-8")
        req = request.Request(url=f"https://slack.com/api/{method}", data=body, method="POST")
        req.add_header("Authorization", f"Bearer {self.bot_token}")
        req.add_header("Content-Type", "application/json; charset=utf-8")
        parsed = self._urlopen_json(req)
        if not parsed.get("ok"):
            raise ApiError(f"Slack API error for {method}: {parsed.get('error', 'unknown_error')}")
        return parsed

    def _urlopen_json(self, req: request.Request) -> dict:
        attempt = 0
        while True:
            try:
                with request.urlopen(req, timeout=30) as resp:
                    return json.loads(resp.read().decode("utf-8"))
            except error.HTTPError as exc:
                if exc.code == 429 and attempt < self.max_retries:
                    retry_after_header = exc.headers.get("Retry-After", "1")
                    try:
                        retry_after = float(retry_after_header)
                    except ValueError:
                        retry_after = 1.0
                    # Slack advises backing off by Retry-After; jitter avoids synchronized retries.
                    sleep_for = max(retry_after, 1.0) + random.uniform(0.05, 0.3)
                    time.sleep(sleep_for)
                    attempt += 1
                    continue
                body = exc.read().decode("utf-8", errors="ignore")
                raise ApiError(f"HTTP error {exc.code}: {body}") from exc


class OpenAIThreadGenerator:
    def __init__(self, api_key: str, model: str) -> None:
        self.api_key = api_key
        self.model = model

    def generate(self, *, channel_name: str, threads: int, min_replies: int, max_replies: int) -> list[ThreadSeed]:
        schema = {
            "type": "object",
            "properties": {
                "threads": {
                    "type": "array",
                    "minItems": threads,
                    "maxItems": threads,
                    "items": {
                        "type": "object",
                        "properties": {
                            "root": {"type": "string", "minLength": 12, "maxLength": 220},
                            "replies": {
                                "type": "array",
                                "minItems": min_replies,
                                "maxItems": max_replies,
                                "items": {"type": "string", "minLength": 8, "maxLength": 180},
                            },
                        },
                        "required": ["root", "replies"],
                        "additionalProperties": False,
                    },
                }
            },
            "required": ["threads"],
            "additionalProperties": False,
        }

        prompt = (
            "Generate realistic Slack thread seeds for a hardware engineering team. "
            f"Channel context: #{channel_name}. "
            "Use concise, human messages that look like real team chat, not formal status reports. "
            "Include board bring-up, firmware-hardware coordination, test failures, risks, decisions, "
            "manufacturing, validation, and scheduling tradeoffs. "
            "Do not use markdown, numbering, or labels."
        )

        payload = {
            "model": self.model,
            "messages": [
                {
                    "role": "system",
                    "content": "You generate JSON only for synthetic engineering Slack data.",
                },
                {
                    "role": "user",
                    "content": prompt,
                },
            ],
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "name": "slack_seed_threads",
                    "strict": True,
                    "schema": schema,
                },
            },
            "temperature": 0.8,
        }

        req = request.Request(
            url="https://api.openai.com/v1/chat/completions",
            data=json.dumps(payload).encode("utf-8"),
            method="POST",
        )
        req.add_header("Authorization", f"Bearer {self.api_key}")
        req.add_header("Content-Type", "application/json")

        try:
            with request.urlopen(req, timeout=60) as resp:
                parsed = json.loads(resp.read().decode("utf-8"))
        except error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="ignore")
            raise ApiError(f"OpenAI API error ({exc.code}): {body}") from exc

        choices = parsed.get("choices", [])
        if not choices:
            raise ApiError("OpenAI API returned no choices")

        content = choices[0].get("message", {}).get("content", "")
        if not content:
            raise ApiError("OpenAI API returned empty content")

        data = json.loads(content)
        threads_raw = data.get("threads", [])
        if not isinstance(threads_raw, list):
            raise ApiError("OpenAI JSON payload missing threads array")

        out: list[ThreadSeed] = []
        for item in threads_raw:
            root = str(item.get("root", "")).strip()
            replies = [str(r).strip() for r in item.get("replies", []) if str(r).strip()]
            if not root or not replies:
                continue
            out.append(ThreadSeed(root=root, replies=replies))

        if len(out) != threads:
            raise ApiError(f"Expected {threads} threads from OpenAI, got {len(out)}")

        return out

    def generate_flowing_messages(self, *, channel_name: str, total_messages: int) -> list[str]:
        schema = {
            "type": "object",
            "properties": {
                "messages": {
                    "type": "array",
                    "minItems": total_messages,
                    "maxItems": total_messages,
                    "items": {"type": "string", "minLength": 8, "maxLength": 200},
                }
            },
            "required": ["messages"],
            "additionalProperties": False,
        }

        prompt = (
            "Generate one continuous flowing Slack conversation for a hardware engineering team. "
            f"Channel context: #{channel_name}. "
            "Each message should feel like the next step in the same ongoing conversation. "
            "Cover realistic bring-up and cross-functional work: firmware, validation, QA, ops, PM, and risks. "
            "Include updates, questions, decisions, follow-ups, and occasional short acknowledgements. "
            "Do not add speaker labels or markdown."
        )

        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": "You generate JSON only for synthetic engineering Slack data."},
                {"role": "user", "content": prompt},
            ],
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "name": "slack_seed_flowing_messages",
                    "strict": True,
                    "schema": schema,
                },
            },
            "temperature": 0.8,
        }

        req = request.Request(
            url="https://api.openai.com/v1/chat/completions",
            data=json.dumps(payload).encode("utf-8"),
            method="POST",
        )
        req.add_header("Authorization", f"Bearer {self.api_key}")
        req.add_header("Content-Type", "application/json")

        try:
            with request.urlopen(req, timeout=90) as resp:
                parsed = json.loads(resp.read().decode("utf-8"))
        except error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="ignore")
            raise ApiError(f"OpenAI API error ({exc.code}): {body}") from exc

        choices = parsed.get("choices", [])
        if not choices:
            raise ApiError("OpenAI API returned no choices")
        content = choices[0].get("message", {}).get("content", "")
        if not content:
            raise ApiError("OpenAI API returned empty content")

        data = json.loads(content)
        messages = [str(m).strip() for m in data.get("messages", []) if str(m).strip()]
        if len(messages) != total_messages:
            raise ApiError(f"Expected {total_messages} messages from OpenAI, got {len(messages)}")
        return messages


class TemplateThreadGenerator:
    ROOTS = [
        "Atlas EVT board still brown-outs during camera bring-up on rail VDD_1V8.",
        "Thermal sweep failed at 80C; PMIC throttles before workload plateau.",
        "SPI flash read retries increased after last layout change on rev D.",
        "Need go/no-go decision: ship with bootloader workaround or slip DVT by 2 days.",
        "Factory pilot found 6 percent failure on USB-C orientation detect.",
        "Regression: BLE pairing drops when IMU stream is enabled at 200Hz.",
        "Validation found intermittent I2C NACK on sensor bus after deep sleep wake.",
        "Power budget over by 140mW in idle with new ADC calibration path enabled.",
        "RF team flagged harmonic peak near spec limit after antenna retune.",
        "Action needed: confirm if rev C can reuse rev B fixture without pogo pin update.",
    ]

    REPLIES = [
        "I reproduced on rack 3 using build 1.9.12 and attached the scope capture.",
        "Firmware can gate init sequence behind rail-good; patch up in 30 minutes.",
        "If we keep current config, risk is missed thermal qualification window on Tuesday.",
        "I can run a bisect between the last two commits after lunch and post results.",
        "Ops asked for final decision by 4pm ET so they can lock tomorrow's line plan.",
        "Suggest adding 15ms settle delay before peripheral enable; this fixed similar issue last quarter.",
        "Let's confirm on two more units before we call this a board-level root cause.",
        "Can QA prioritize stress loop overnight so we have data for standup tomorrow.",
        "I pushed logs to the bring-up folder and tagged the failing serial numbers.",
        "Proposing we track this as a blocker until we pass 100 consecutive cycles.",
    ]

    FLOWING_MESSAGES = [
        "Seeing intermittent brown-out on Atlas EVT during camera init at boot.",
        "Can you share rail capture around VDD_1V8 when the fault hits.",
        "Uploaded scope trace, dip is about 120us right before sensor probe.",
        "Firmware can delay probe by 20ms, testing patch on build 1.9.14.",
        "Patch flashed on unit 07, boot looks stable across first 30 cycles.",
        "Good sign, can QA run 200-cycle stress tonight on two racks.",
        "Yes, scheduling rack 2 and rack 3 now, results by 9am ET.",
        "Thermal team also reported throttle at 80C with current PMIC config.",
        "If throttle persists we may miss Tuesday qualification window.",
        "Let us compare with rev B baseline to isolate layout impact.",
        "Rev B passes same workload, so likely rev D power sequencing issue.",
        "I can prep firmware gate for peripheral enable behind rail-good.",
        "Please include logs with serial numbers so ops can track fallout.",
        "Posted logs and affected serials in bring-up folder.",
        "Any update on USB-C orientation detect failures from pilot line.",
        "Pilot line still at 6 percent fail, mostly on station 4.",
        "Could be fixture wear, pogo pins looked misaligned this morning.",
        "Ops can swap fixture heads by noon if we confirm root cause.",
        "Run one pass on replacement head and share fail delta.",
        "Done, fail rate dropped to 1.2 percent on first 120 units.",
        "That is within threshold for pilot, but keep monitoring hourly.",
        "On BLE regression, pairing still drops with IMU stream at 200Hz.",
        "I reproduced on latest firmware, appears tied to ISR load spikes.",
        "Can we lower IMU stream to 100Hz for current validation run.",
        "Yes for validation, but product requirement is still 200Hz target.",
        "Let us file temporary waiver and track permanent fix for DVT.",
        "Agreed, adding blocker note and owner in tracker now.",
        "Need go or no-go on shipping with bootloader workaround.",
        "My vote is go for EVT samples, no-go for customer pilot.",
        "PM agrees, we can hold pilot until thermal and BLE are closed.",
    ]

    def __init__(self, rng: random.Random) -> None:
        self.rng = rng

    def generate(self, *, threads: int, min_replies: int, max_replies: int) -> list[ThreadSeed]:
        out: list[ThreadSeed] = []
        for _ in range(threads):
            root = self.rng.choice(self.ROOTS)
            replies_n = self.rng.randint(min_replies, max_replies)
            replies = self.rng.sample(self.REPLIES, k=min(replies_n, len(self.REPLIES)))
            out.append(ThreadSeed(root=root, replies=replies))
        return out

    def generate_flowing_messages(self, *, total_messages: int) -> list[str]:
        out: list[str] = []
        for idx in range(total_messages):
            base = self.FLOWING_MESSAGES[idx % len(self.FLOWING_MESSAGES)]
            out.append(base)
        return out


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Seed hardware-engineering Slack test traffic")
    parser.add_argument(
        "--slack-bot-token",
        default=os.getenv("SLACK_BOT_TOKEN", ""),
        help="Slack bot token (or set SLACK_BOT_TOKEN)",
    )
    parser.add_argument(
        "--slack-channel-ids",
        default=os.getenv("SLACK_CHANNEL_IDS", ""),
        help="Comma-separated channel IDs (or set SLACK_CHANNEL_IDS)",
    )
    parser.add_argument("--threads-per-channel", type=int, default=8)
    parser.add_argument("--min-replies", type=int, default=1)
    parser.add_argument("--max-replies", type=int, default=4)
    parser.add_argument("--sleep-seconds", type=float, default=0.35)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument(
        "--mode",
        choices=["threads", "flowing"],
        default="threads",
        help="threads: multiple root/reply threads. flowing: one continuous conversation thread.",
    )
    parser.add_argument(
        "--total-messages",
        type=int,
        default=500,
        help="Only used in flowing mode. Total messages to post in one continuous thread.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print messages without posting to Slack")
    parser.add_argument(
        "--generator",
        choices=["auto", "template", "openai"],
        default="auto",
        help="Message generator. auto uses openai when OPENAI_API_KEY is set, else template.",
    )
    parser.add_argument("--openai-api-key", default=os.getenv("OPENAI_API_KEY", ""))
    parser.add_argument(
        "--openai-model",
        default=os.getenv("OPENAI_MODEL", "gpt-4.1-mini"),
        help="OpenAI model for generation (default: gpt-4.1-mini)",
    )
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> list[str]:
    errors: list[str] = []
    if not args.slack_channel_ids.strip():
        errors.append("Missing channel IDs: set --slack-channel-ids or SLACK_CHANNEL_IDS")
    if args.min_replies < 1:
        errors.append("--min-replies must be >= 1")
    if args.max_replies < args.min_replies:
        errors.append("--max-replies must be >= --min-replies")
    if args.threads_per_channel < 1:
        errors.append("--threads-per-channel must be >= 1")
    if args.mode == "flowing" and args.total_messages < 2:
        errors.append("--total-messages must be >= 2 in flowing mode")
    if args.generator == "openai" and not args.openai_api_key:
        errors.append("Missing OpenAI API key: set --openai-api-key or OPENAI_API_KEY")
    if not args.dry_run and not args.slack_bot_token:
        errors.append("Missing Slack bot token: set --slack-bot-token or SLACK_BOT_TOKEN")
    return errors


def main() -> int:
    args = parse_args()
    errors = validate_args(args)
    if errors:
        for item in errors:
            print(f"error: {item}", file=sys.stderr)
        return 2

    channel_ids = [item.strip() for item in args.slack_channel_ids.split(",") if item.strip()]
    rng = random.Random(args.seed)

    slack = SlackSeeder(bot_token=args.slack_bot_token) if not args.dry_run else None
    template = TemplateThreadGenerator(rng=rng)
    openai_gen = None
    selected_generator = args.generator
    if args.generator == "auto":
        selected_generator = "openai" if args.openai_api_key else "template"
    if selected_generator == "openai":
        openai_gen = OpenAIThreadGenerator(api_key=args.openai_api_key, model=args.openai_model)

    total_roots = 0
    total_replies = 0

    for channel_id in channel_ids:
        channel_name = channel_id
        if slack:
            try:
                channel_name = slack.channel_name(channel_id)
            except Exception as exc:  # noqa: BLE001
                print(f"warn: unable to resolve channel name for {channel_id}: {exc}", file=sys.stderr)

        try:
            if args.mode == "flowing":
                if openai_gen is not None:
                    flowing_messages = openai_gen.generate_flowing_messages(
                        channel_name=channel_name,
                        total_messages=args.total_messages,
                    )
                else:
                    flowing_messages = template.generate_flowing_messages(total_messages=args.total_messages)
            else:
                if openai_gen is not None:
                    seeds = openai_gen.generate(
                        channel_name=channel_name,
                        threads=args.threads_per_channel,
                        min_replies=args.min_replies,
                        max_replies=args.max_replies,
                    )
                else:
                    seeds = template.generate(
                        threads=args.threads_per_channel,
                        min_replies=args.min_replies,
                        max_replies=args.max_replies,
                    )
        except Exception as exc:  # noqa: BLE001
            print(
                f"warn: generator failed for channel {channel_id} ({exc}); falling back to template messages",
                file=sys.stderr,
            )
            if args.mode == "flowing":
                flowing_messages = template.generate_flowing_messages(total_messages=args.total_messages)
            else:
                seeds = template.generate(
                    threads=args.threads_per_channel,
                    min_replies=args.min_replies,
                    max_replies=args.max_replies,
                )

        if args.mode == "flowing":
            total_roots += 1
            total_replies += max(0, len(flowing_messages) - 1)
            if args.dry_run:
                if flowing_messages:
                    print(f"[{channel_id}] ROOT: {flowing_messages[0]}")
                    for msg in flowing_messages[1:]:
                        print(f"[{channel_id}]   REPLY: {msg}")
                continue

            assert slack is not None
            if not flowing_messages:
                continue
            root_post = slack.post_message(channel_id=channel_id, text=flowing_messages[0])
            root_ts = str(root_post.get("ts", ""))
            if not root_ts:
                raise ApiError(f"Missing ts when posting root message to {channel_id}")

            for msg in flowing_messages[1:]:
                slack.post_message(channel_id=channel_id, text=msg, thread_ts=root_ts)
                if args.sleep_seconds > 0:
                    time.sleep(args.sleep_seconds)

            if args.sleep_seconds > 0:
                time.sleep(args.sleep_seconds)
            continue

        for thread in seeds:
            total_roots += 1
            total_replies += len(thread.replies)

            if args.dry_run:
                print(f"[{channel_id}] ROOT: {thread.root}")
                for reply in thread.replies:
                    print(f"[{channel_id}]   REPLY: {reply}")
                continue

            assert slack is not None
            root_post = slack.post_message(channel_id=channel_id, text=thread.root)
            root_ts = str(root_post.get("ts", ""))
            if not root_ts:
                raise ApiError(f"Missing ts when posting root message to {channel_id}")

            for reply in thread.replies:
                slack.post_message(channel_id=channel_id, text=reply, thread_ts=root_ts)
                if args.sleep_seconds > 0:
                    time.sleep(args.sleep_seconds)

            if args.sleep_seconds > 0:
                time.sleep(args.sleep_seconds)

    print(
        "seed complete "
        f"channels={len(channel_ids)} roots={total_roots} replies={total_replies} "
        f"generator={selected_generator} model={args.openai_model if selected_generator == 'openai' else 'n/a'}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
