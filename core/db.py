# core/db.py
import json
import logging
import sqlite3
from pathlib import Path
from typing import Optional, Dict, Any, List, Literal, TypedDict
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

SrcType = Literal["github", "hn", "reddit", "ph"]

class RawEvent(TypedDict):
    ts: str                  # ISO8601 UTC, e.g. "2025-09-03T07:30:00Z"
    src: SrcType
    url: str
    title: Optional[str]
    text: Optional[str]
    topic_guess: str
    metrics_json: str        # must be a valid JSON string

def to_iso8601_utc(dt: Optional[datetime] = None) -> str:
    """Return an ISO8601 UTC string with 'Z' suffix."""
    if dt is None:
        dt = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

class TrendRadarDB:
    """SQLite manager (persistent connection) for Tech Trend Radar raw events."""

    def __init__(self, db_path: str = "./trend_radar.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(exist_ok=True)
        # Persistent connection; enable WAL for better concurrent R/W and bulk inserts
        self.conn = sqlite3.connect(
            self.db_path,
            check_same_thread=False,   # allows use from different threads if needed later
            isolation_level=None       # autocommit mode; we will use explicit BEGIN for bulks
        )
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self.conn.execute("PRAGMA foreign_keys=ON;")
        self._init_db()

    def _init_db(self) -> None:
        """Initialize schema and indexes if they don't exist."""
        try:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS raw_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT NOT NULL,
                    src TEXT NOT NULL CHECK(src IN ('github','hn','reddit','ph')),
                    url TEXT NOT NULL,
                    title TEXT,
                    text TEXT,
                    topic_guess TEXT NOT NULL,
                    metrics_json TEXT NOT NULL
                )
            """)
            self.conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_raw_src_ts
                ON raw_events(src, ts)
            """)
            self.conn.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_raw_url
                ON raw_events(url)
            """)
            logger.info("Database initialized at %s", self.db_path)
        except sqlite3.Error as e:
            logger.error("Failed to initialize database: %s", e)
            raise

    @staticmethod
    def _validate_event(ev: RawEvent) -> None:
        """Strict validation before insert to avoid corrupt rows."""
        # ts must be ISO8601 UTC (basic sanity check)
        if not isinstance(ev.get("ts"), str) or not ev["ts"].endswith("Z"):
            raise ValueError("ts must be ISO8601 UTC string ending with 'Z'")
        # src must be one of allowed values (type-checked too)
        if ev.get("src") not in ("github", "hn", "reddit", "ph"):
            raise ValueError("src must be one of: github, hn, reddit, ph")
        # url required and must look like http/https
        url = ev.get("url") or ""
        if not (url.startswith("http://") or url.startswith("https://")):
            raise ValueError("url must start with http:// or https://")
        # topic_guess required
        if not ev.get("topic_guess"):
            raise ValueError("topic_guess is required")
        # metrics_json must be valid JSON
        mj = ev.get("metrics_json")
        if not isinstance(mj, str):
            raise ValueError("metrics_json must be a JSON string")
        try:
            json.loads(mj)
        except Exception as e:
            raise ValueError(f"metrics_json is not valid JSON: {e}") from e

    def insert_event(self, event_data: RawEvent) -> bool:
        """Insert a single event; returns True if inserted, False if duplicate."""
        try:
            self._validate_event(event_data)
            cur = self.conn.execute(
                """
                INSERT OR IGNORE INTO raw_events
                (ts, src, url, title, text, topic_guess, metrics_json)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_data["ts"],
                    event_data["src"],
                    event_data["url"],
                    event_data.get("title"),
                    event_data.get("text"),
                    event_data["topic_guess"],
                    event_data["metrics_json"],
                ),
            )
            return cur.rowcount > 0
        except Exception as e:
            logger.error("Failed to insert event (%s): %s", event_data.get("url", "?"), e)
            return False

    def insert_events_bulk(self, events: List[RawEvent]) -> Dict[str, int]:
        """
        Efficient bulk insert using executemany within a single transaction.
        Returns counts: {'inserted': X, 'duplicates': Y, 'failed': Z}
        """
        inserted = duplicates = failed = 0
        if not events:
            return {"inserted": 0, "duplicates": 0, "failed": 0}

        # Validate upfront (fast-fail on bad rows, count failed)
        validated: List[RawEvent] = []
        for ev in events:
            try:
                self._validate_event(ev)
                validated.append(ev)
            except Exception as e:
                failed += 1
                logger.warning("Skipping invalid event (%s): %s", ev.get("url", "?"), e)

        if not validated:
            return {"inserted": 0, "duplicates": 0, "failed": failed}

        try:
            self.conn.execute("BEGIN")
            cur = self.conn.executemany(
                """
                INSERT OR IGNORE INTO raw_events
                (ts, src, url, title, text, topic_guess, metrics_json)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        ev["ts"],
                        ev["src"],
                        ev["url"],
                        ev.get("title"),
                        ev.get("text"),
                        ev["topic_guess"],
                        ev["metrics_json"],
                    )
                    for ev in validated
                ],
            )
            self.conn.execute("COMMIT")

            # sqlite3.Cursor.rowcount for executemany may return total affected rows
            affected = cur.rowcount if cur.rowcount is not None else 0
            # duplicates = validated - inserted
            inserted = affected
            duplicates = len(validated) - inserted
        except sqlite3.Error as e:
            logger.error("Bulk insert failed, rolling back: %s", e)
            try:
                self.conn.execute("ROLLBACK")
            except Exception:
                pass
            # Count all validated as failed in this case
            failed += len(validated)
        return {"inserted": inserted, "duplicates": duplicates, "failed": failed}

    def get_stats(self) -> Dict[str, int]:
        """Return counts per source in raw_events."""
        try:
            cur = self.conn.execute(
                "SELECT src, COUNT(*) FROM raw_events GROUP BY src"
            )
            return {row[0]: row[1] for row in cur.fetchall()}
        except sqlite3.Error as e:
            logger.error("Failed to get stats: %s", e)
            return {}

    def close(self) -> None:
        """Close persistent connection."""
        try:
            self.conn.close()
        except Exception:
            pass
