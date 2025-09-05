# core/db.py
import sqlite3
import logging
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

class TrendRadarDB:
    """SQLite database manager for Tech Trend Radar raw events."""
    
    def __init__(self, db_path: str = "./trend_radar.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(exist_ok=True)
        self._init_db()
    
    def _init_db(self) -> None:
        """Initialize database and create tables if they don't exist."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
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
                
                # Create indexes
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_raw_src_ts 
                    ON raw_events(src, ts)
                """)
                
                conn.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_raw_url 
                    ON raw_events(url)
                """)
                
                conn.commit()
                logger.info(f"Database initialized at {self.db_path}")
                
        except sqlite3.Error as e:
            logger.error(f"Failed to initialize database: {e}")
            raise
    
    def _normalize_timestamp(self, ts: str) -> str:
        """
        Normalize timestamp to ISO8601 UTC format ending with 'Z'.
        
        Args:
            ts: Timestamp string
            
        Returns:
            Normalized timestamp string
        """
        try:
            # If already ends with Z, return as is
            if ts.endswith('Z'):
                return ts
            
            # If ends with +00:00, replace with Z
            if ts.endswith('+00:00'):
                return ts[:-6] + 'Z'
            
            # If no timezone info, assume UTC and add Z
            if 'T' in ts and not ts.endswith('Z') and '+' not in ts:
                return ts + 'Z'
            
            # Try to parse and reformat
            dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
            return dt.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')
            
        except Exception as e:
            logger.warning(f"Failed to normalize timestamp '{ts}': {e}")
            # Fallback to current UTC time
            return datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
    
    def insert_event(self, event_data: Dict[str, Any]) -> bool:
        """
        Insert a new event, avoiding duplicates by URL.
        
        Args:
            event_data: Dictionary with event fields
            
        Returns:
            bool: True if inserted, False if duplicate
        """
        try:
            # Normalize timestamp
            normalized_ts = self._normalize_timestamp(event_data['ts'])
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("""
                    INSERT OR IGNORE INTO raw_events 
                    (ts, src, url, title, text, topic_guess, metrics_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    normalized_ts,
                    event_data['src'],
                    event_data['url'],
                    event_data.get('title'),
                    event_data.get('text'),
                    event_data['topic_guess'],
                    event_data['metrics_json']
                ))
                
                conn.commit()
                return cursor.rowcount > 0
                
        except sqlite3.Error as e:
            logger.error(f"Failed to insert event: {e}")
            return False
    
    def get_stats(self) -> Dict[str, int]:
        """Get basic statistics from raw_events table."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("""
                    SELECT src, COUNT(*) as count 
                    FROM raw_events 
                    GROUP BY src
                """)
                
                stats = {row[0]: row[1] for row in cursor.fetchall()}
                return stats
                
        except sqlite3.Error as e:
            logger.error(f"Failed to get stats: {e}")
            return {}
    
    def close(self) -> None:
        """Close database connection (not needed for SQLite but good practice)."""
        pass