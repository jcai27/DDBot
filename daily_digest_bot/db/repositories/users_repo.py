from __future__ import annotations

import json
import sqlite3

from daily_digest_bot.models import User, UserProfile


class UsersRepository:
    """Persistence operations for users and user profiles."""

    def upsert_user(self, conn: sqlite3.Connection, user: User) -> None:
        """Insert/update a user row keyed by user_id."""
        conn.execute(
            """
            INSERT INTO users (user_id, display_name, role)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
              display_name=excluded.display_name,
              role=excluded.role
            """,
            (user.user_id, user.display_name, user.role),
        )

    def list_users(self, conn: sqlite3.Connection) -> list[User]:
        """Return all users currently stored in the workspace snapshot."""
        rows = conn.execute("SELECT * FROM users").fetchall()
        return [User(user_id=row["user_id"], display_name=row["display_name"], role=row["role"]) for row in rows]

    def upsert_user_profile(self, conn: sqlite3.Connection, profile: UserProfile) -> None:
        """Insert/update per-user digest personalization profile."""
        conn.execute(
            """
            INSERT INTO user_profiles (
              user_id, role, active_projects_json, ownership_areas_json,
              digest_preferences_json, learned_feedback_weights_json, digest_enabled, timezone
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
              role=excluded.role,
              active_projects_json=excluded.active_projects_json,
              ownership_areas_json=excluded.ownership_areas_json,
              digest_preferences_json=excluded.digest_preferences_json,
              learned_feedback_weights_json=excluded.learned_feedback_weights_json,
              digest_enabled=excluded.digest_enabled,
              timezone=excluded.timezone
            """,
            (
                profile.user_id,
                profile.role,
                json.dumps(profile.active_projects),
                json.dumps(profile.ownership_areas),
                json.dumps(profile.digest_preferences),
                json.dumps(profile.learned_feedback_weights),
                1 if profile.digest_enabled else 0,
                profile.timezone,
            ),
        )

    def list_user_profiles(self, conn: sqlite3.Connection, digest_enabled_only: bool = False) -> list[UserProfile]:
        """Return user profiles, optionally restricted to digest-enabled rows."""
        where_clause = "WHERE digest_enabled = 1" if digest_enabled_only else ""
        rows = conn.execute(f"SELECT * FROM user_profiles {where_clause}").fetchall()
        return [
            UserProfile(
                user_id=row["user_id"],
                role=row["role"],
                active_projects=json.loads(row["active_projects_json"]),
                ownership_areas=json.loads(row["ownership_areas_json"]),
                digest_preferences=json.loads(row["digest_preferences_json"]),
                learned_feedback_weights=json.loads(row["learned_feedback_weights_json"]),
                digest_enabled=bool(row["digest_enabled"]),
                timezone=row["timezone"],
            )
            for row in rows
        ]
