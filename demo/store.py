"""社区账号和状态使用独立的 SQLite 文件。"""
from __future__ import annotations

import hashlib
import json
import re
import secrets
import sqlite3
from threading import RLock
from contextlib import closing
from pathlib import Path


class Store:
    def __init__(self, path: str | None) -> None:
        self._lock = RLock()
        self._db = sqlite3.connect(path or ":memory:", check_same_thread=False)
        self._db.execute("""CREATE TABLE IF NOT EXISTS accounts (
            username TEXT PRIMARY KEY, role TEXT NOT NULL, salt BLOB NOT NULL, digest BLOB NOT NULL
        )""")
        self._db.execute("""CREATE TABLE IF NOT EXISTS community_state (
            id INTEGER PRIMARY KEY CHECK (id = 1), payload TEXT NOT NULL
        )""")

    @staticmethod
    def _digest(password: str, salt: bytes) -> bytes:
        return hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 600_000)

    def set_account(self, username: str, password: str, role: str, *, bootstrap: bool = False) -> None:
        salt = secrets.token_bytes(16)
        with self._lock, self._db:
            if bootstrap:
                self._db.execute("INSERT OR IGNORE INTO accounts VALUES (?, ?, ?, ?)",
                                 (username, role, salt, self._digest(password, salt)))
            else:
                self._db.execute("""INSERT INTO accounts VALUES (?, ?, ?, ?)
                    ON CONFLICT(username) DO UPDATE SET salt=excluded.salt, digest=excluded.digest
                    WHERE accounts.role='guest' AND excluded.role='guest'""",
                    (username, role, salt, self._digest(password, salt)))

    def authenticate(self, username: str, password: str) -> str | None:
        with self._lock, closing(self._db.cursor()) as cursor:
            row = cursor.execute("SELECT role, salt, digest FROM accounts WHERE username=?",
                                 (username,)).fetchone()
        role, salt, digest = row or (None, bytes(16), bytes(32))
        valid = secrets.compare_digest(self._digest(password, salt), digest)
        return role if valid else None

    def guest_names(self) -> list[str]:
        with self._lock:
            return [row[0] for row in self._db.execute("SELECT username FROM accounts WHERE role='guest' ORDER BY username")]

    def load(self) -> dict | None:
        with self._lock:
            row = self._db.execute("SELECT payload FROM community_state WHERE id=1").fetchone()
        return json.loads(row[0]) if row else None

    def save(self, payload: dict) -> None:
        with self._lock, self._db:
            self._db.execute("""INSERT INTO community_state VALUES (1, ?)
                ON CONFLICT(id) DO UPDATE SET payload=excluded.payload""", (json.dumps(payload),))


def guest_name_valid(username: str) -> bool:
    return username != "owner" and re.fullmatch(r"[a-z][a-z0-9_-]{2,23}", username) is not None


def import_posts(path: Path) -> dict:
    posts = json.loads(path.read_text())
    return {
        "creators": {
            post["creator_id"]: {"creator_id": post["creator_id"], "name": post["creator_name"]}
            for post in posts
        },
        "posts": {
            post["post_id"]: {key: post[key] for key in ("post_id", "creator_id", "title", "body")}
            for post in reversed(posts)
        },
        "likes": {post["post_id"]: post["likes"] for post in posts},
        "comments": {post["post_id"]: post["comments"] for post in posts},
    }
