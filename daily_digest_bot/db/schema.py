from __future__ import annotations

import sqlite3


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS channels (
  channel_id TEXT PRIMARY KEY,
  name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS users (
  user_id TEXT PRIMARY KEY,
  display_name TEXT NOT NULL,
  role TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS messages (
  message_id TEXT PRIMARY KEY,
  channel_id TEXT NOT NULL,
  user_id TEXT NOT NULL,
  text TEXT NOT NULL,
  ts TEXT NOT NULL,
  thread_ts TEXT,
  reactions_count INTEGER NOT NULL DEFAULT 0,
  reply_count INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY(channel_id) REFERENCES channels(channel_id),
  FOREIGN KEY(user_id) REFERENCES users(user_id)
);

CREATE TABLE IF NOT EXISTS ingestion_state (
  channel_id TEXT PRIMARY KEY,
  last_history_ts TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS structured_events (
  event_id TEXT PRIMARY KEY,
  thread_ts TEXT NOT NULL,
  channel_id TEXT NOT NULL,
  summary TEXT NOT NULL,
  event_type TEXT NOT NULL,
  project TEXT NOT NULL,
  subsystem TEXT NOT NULL,
  participants_json TEXT NOT NULL,
  urgency_score REAL NOT NULL,
  relevant_roles_json TEXT NOT NULL,
  is_open INTEGER NOT NULL,
  source_thread_link TEXT NOT NULL,
  created_at TEXT NOT NULL,
  confidence REAL NOT NULL DEFAULT 0.0,
  dedupe_group_id TEXT NOT NULL DEFAULT '',
  last_seen_at TEXT,
  needs_reprocess INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS user_profiles (
  user_id TEXT PRIMARY KEY,
  role TEXT NOT NULL,
  active_projects_json TEXT NOT NULL,
  ownership_areas_json TEXT NOT NULL,
  digest_preferences_json TEXT NOT NULL,
  learned_feedback_weights_json TEXT NOT NULL,
  digest_enabled INTEGER NOT NULL DEFAULT 1,
  timezone TEXT NOT NULL DEFAULT 'America/New_York'
);

CREATE TABLE IF NOT EXISTS digest_runs (
  run_id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL,
  local_digest_date TEXT NOT NULL,
  window_start TEXT NOT NULL,
  window_end TEXT NOT NULL,
  sent_at TEXT NOT NULL,
  item_count INTEGER NOT NULL,
  linked_item_count INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS feedback_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id TEXT NOT NULL,
  event_id TEXT NOT NULL,
  run_id TEXT,
  signal_type TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_messages_thread_ts ON messages(thread_ts);
CREATE INDEX IF NOT EXISTS idx_messages_ts ON messages(ts);
CREATE INDEX IF NOT EXISTS idx_events_created_at ON structured_events(created_at);
CREATE INDEX IF NOT EXISTS idx_feedback_created_at ON feedback_events(created_at);
CREATE INDEX IF NOT EXISTS idx_digest_runs_user_date ON digest_runs(user_id, local_digest_date);
CREATE INDEX IF NOT EXISTS idx_events_last_seen_at ON structured_events(last_seen_at);
CREATE INDEX IF NOT EXISTS idx_events_dedupe ON structured_events(dedupe_group_id);
"""


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    migrate_columns(conn)


def migrate_columns(conn: sqlite3.Connection) -> None:
    ensure_column(conn, "messages", "reply_count", "INTEGER NOT NULL DEFAULT 0")
    ensure_column(conn, "messages", "updated_at", "TEXT")
    ensure_column(conn, "structured_events", "confidence", "REAL NOT NULL DEFAULT 0.0")
    ensure_column(conn, "structured_events", "dedupe_group_id", "TEXT NOT NULL DEFAULT ''")
    ensure_column(conn, "structured_events", "last_seen_at", "TEXT")
    ensure_column(conn, "structured_events", "needs_reprocess", "INTEGER NOT NULL DEFAULT 0")
    ensure_column(conn, "user_profiles", "digest_enabled", "INTEGER NOT NULL DEFAULT 1")
    ensure_column(conn, "user_profiles", "timezone", "TEXT NOT NULL DEFAULT 'America/New_York'")


def ensure_column(conn: sqlite3.Connection, table: str, column: str, col_type: str) -> None:
    cols = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
