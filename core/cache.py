# core/cache.py
import json
import logging
import hashlib
import time
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

class CacheManager:
    """Simple file-based cache with TTL and fingerprinting for API calls."""
    
    def __init__(self, cache_dir: str = "./cache", ttl_hours: int = 3):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        self.ttl_seconds = ttl_hours * 3600
        
        logger.info(f"Cache initialized at {self.cache_dir} with TTL {ttl_hours}h")
    
    def _generate_fingerprint(self, src: str, keyword: str, page: int = 1) -> str:
        """
        Generate unique fingerprint for cache key.
        
        Args:
            src: Source identifier (github, hn, reddit, ph)
            keyword: Search keyword/topic
            page: Page number for pagination
            
        Returns:
            str: MD5 hash fingerprint
        """
        cache_key = f"{src}:{keyword}:{page}".lower()
        return hashlib.md5(cache_key.encode()).hexdigest()
    
    def _get_cache_file_path(self, fingerprint: str) -> Path:
        """Get cache file path for given fingerprint."""
        return self.cache_dir / f"{fingerprint}.json"
    
    def _parse_cached_time(self, cached_at: str) -> datetime:
        """
        Parse cached timestamp with support for both Z and explicit UTC formats.
        
        Args:
            cached_at: Timestamp string from cache
            
        Returns:
            datetime: Parsed datetime in UTC
        """
        try:
            # Handle both "Z" and explicit "+00:00" UTC formats
            normalized_time = cached_at.replace("Z", "+00:00")
            return datetime.fromisoformat(normalized_time)
        except ValueError:
            # Fallback to current time if parsing fails
            logger.warning(f"Failed to parse cached timestamp: {cached_at}")
            return datetime.now(timezone.utc)
    
    def get_cached_data(self, src: str, keyword: str, page: int = 1) -> Optional[Dict[str, Any]]:
        """
        Retrieve cached data if valid and not expired.
        
        Args:
            src: Source identifier
            keyword: Search keyword
            page: Page number
            
        Returns:
            Dict with cached data if valid, None if expired/missing
        """
        fingerprint = self._generate_fingerprint(src, keyword, page)
        cache_file = self._get_cache_file_path(fingerprint)
        
        if not cache_file.exists():
            return None
        
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                cached_data = json.load(f)
            
            # Check if cache is still valid
            cached_time = self._parse_cached_time(cached_data['cached_at'])
            if datetime.now(timezone.utc) - cached_time > timedelta(seconds=self.ttl_seconds):
                logger.debug(f"Cache expired for {src}:{keyword}:{page}")
                cache_file.unlink()  # Remove expired cache
                return None
            
            logger.debug(f"Cache hit for {src}:{keyword}:{page}")
            return cached_data['data']
            
        except (json.JSONDecodeError, KeyError, OSError) as e:
            logger.warning(f"Failed to read cache for {src}:{keyword}:{page}: {e}")
            # Clean up corrupted cache file
            try:
                cache_file.unlink()
            except OSError:
                pass
            return None
    
    def set_cached_data(self, src: str, keyword: str, page: int, data: Dict[str, Any]) -> None:
        """
        Cache data with current timestamp.
        
        Args:
            src: Source identifier
            keyword: Search keyword
            page: Page number
            data: Data to cache
        """
        fingerprint = self._generate_fingerprint(src, keyword, page)
        cache_file = self._get_cache_file_path(fingerprint)
        
        try:
            cache_entry = {
                'cached_at': datetime.now(timezone.utc).isoformat(),
                'src': src,
                'keyword': keyword,
                'page': page,
                'data': data
            }
            
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_entry, f, indent=2, ensure_ascii=False)
            
            logger.debug(f"Cached data for {src}:{keyword}:{page}")
            
        except (OSError, TypeError) as e:
            logger.error(f"Failed to cache data for {src}:{keyword}:{page}: {e}")
    
    def is_cached(self, src: str, keyword: str, page: int = 1) -> bool:
        """
        Check if data is cached and valid (lightweight check).
        
        Args:
            src: Source identifier
            keyword: Search keyword
            page: Page number
            
        Returns:
            bool: True if valid cache exists
        """
        fingerprint = self._generate_fingerprint(src, keyword, page)
        cache_file = self._get_cache_file_path(fingerprint)
        
        if not cache_file.exists():
            return False
        
        try:
            # Lightweight check: just read the timestamp
            with open(cache_file, 'r', encoding='utf-8') as f:
                cached_data = json.load(f)
            
            cached_time = self._parse_cached_time(cached_data['cached_at'])
            return datetime.now(timezone.utc) - cached_time <= timedelta(seconds=self.ttl_seconds)
            
        except Exception:
            return False
    
    def clear_expired(self) -> int:
        """
        Clear all expired cache files.
        
        Returns:
            int: Number of files removed
        """
        removed_count = 0
        current_time = datetime.now(timezone.utc)
        
        for cache_file in self.cache_dir.glob("*.json"):
            try:
                with open(cache_file, 'r', encoding='utf-8') as f:
                    cached_data = json.load(f)
                
                cached_time = self._parse_cached_time(cached_data['cached_at'])
                if current_time - cached_time > timedelta(seconds=self.ttl_seconds):
                    cache_file.unlink()
                    removed_count += 1
                    
            except Exception:
                # Remove corrupted cache files
                try:
                    cache_file.unlink()
                    removed_count += 1
                except OSError:
                    pass
        
        if removed_count > 0:
            logger.info(f"Cleared {removed_count} expired cache files")
        
        return removed_count
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        total_files = len(list(self.cache_dir.glob("*.json")))
        expired_count = 0
        valid_count = 0
        
        current_time = datetime.now(timezone.utc)
        
        for cache_file in self.cache_dir.glob("*.json"):
            try:
                with open(cache_file, 'r', encoding='utf-8') as f:
                    cached_data = json.load(f)
                
                cached_time = self._parse_cached_time(cached_data['cached_at'])
                if current_time - cached_time > timedelta(seconds=self.ttl_seconds):
                    expired_count += 1
                else:
                    valid_count += 1
                    
            except Exception:
                expired_count += 1
        
        return {
            'total_files': total_files,
            'valid_files': valid_count,
            'expired_files': expired_count,
            'cache_dir': str(self.cache_dir),
            'ttl_hours': self.ttl_seconds // 3600
        }