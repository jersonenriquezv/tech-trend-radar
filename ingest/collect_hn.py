# ingest/collect_hn.py
import json
import logging
import requests
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

class HackerNewsCollector:
    """Collects recent Hacker News stories matching topics using Firebase API."""
    
    def __init__(self, days_limit: int = 7):
        self.base_url = "https://hacker-news.firebaseio.com/v0"
        self.days_limit = days_limit
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'jerdev-trend-radar'
        })
        
        # HN API endpoints
        self.top_stories_url = f"{self.base_url}/topstories.json"
        self.new_stories_url = f"{self.base_url}/newstories.json"
        self.item_url = f"{self.base_url}/item"
        
        logger.info(f"Hacker News collector initialized with {days_limit} days limit")
    
    def _get_story_ids(self, story_type: str = "top", limit: int = 100) -> List[int]:
        """
        Get list of story IDs from HN API.
        
        Args:
            story_type: 'top' or 'new' stories
            limit: Maximum number of story IDs to return
            
        Returns:
            List of story IDs
        """
        try:
            url = self.top_stories_url if story_type == "top" else self.new_stories_url
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            story_ids = response.json()
            return story_ids[:limit]
            
        except requests.RequestException as e:
            logger.error(f"Failed to get {story_type} stories: {e}")
            return []
    
    def _get_story_details(self, story_id: int) -> Optional[Dict[str, Any]]:
        """
        Get detailed story information by ID.
        
        Args:
            story_id: HN story ID
            
        Returns:
            Story details or None if failed
        """
        try:
            url = f"{self.item_url}/{story_id}.json"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            story_data = response.json()
            
            # Filter out deleted/dead stories
            if story_data.get('deleted') or story_data.get('dead'):
                return None
                
            return story_data
            
        except requests.RequestException as e:
            logger.error(f"Failed to get story {story_id}: {e}")
            return None
    
    def _filter_recent_stories(self, stories: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Filter stories to only include recent ones.
        
        Args:
            stories: List of story data from HN API
            
        Returns:
            List of recent stories
        """
        cutoff_time = datetime.now(timezone.utc) - timedelta(days=self.days_limit)
        recent_stories = []
        
        for story in stories:
            # HN uses Unix timestamp
            story_time = story.get('time')
            if not story_time:
                continue
                
            story_datetime = datetime.fromtimestamp(story_time, tz=timezone.utc)
            if story_datetime >= cutoff_time:
                recent_stories.append(story)
        
        return recent_stories
    
    def _extract_metrics(self, story: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract metrics from story data.
        
        Args:
            story: Story data from HN API
            
        Returns:
            Dict with flat metrics structure
        """
        return {
            'score': story.get('score', 0),
            'comments': story.get('descendants', 0),
            'item_id': story.get('id'),
            'time': story.get('time'),
            'by': story.get('by'),
            'type': story.get('type')
        }
    
    def _validate_url(self, url: str) -> bool:
        """Validate that URL is well-formed HTTP/HTTPS."""
        try:
            parsed = urlparse(url)
            return parsed.scheme in ('http', 'https') and parsed.netloc
        except Exception:
            return False
    
    def _get_story_url(self, story: Dict[str, Any]) -> str:
        """
        Get the URL for a story (external link or HN discussion).
        
        Args:
            story: Story data from HN API
            
        Returns:
            URL string
        """
        # Prefer external URL if available
        if story.get('url'):
            return story['url']
        
        # Fallback to HN discussion URL
        story_id = story.get('id')
        return f"https://news.ycombinator.com/item?id={story_id}"
    
    def _get_story_text(self, story: Dict[str, Any]) -> str:
        """
        Get story text content when available.
        
        Args:
            story: Story data from HN API
            
        Returns:
            Story text content or empty string
        """
        # HN stories can have 'text' field for self-posts
        text = story.get('text', '')
        
        # Clean HTML tags if present (HN uses HTML in text)
        if text and '<' in text:
            import re
            # Simple HTML tag removal
            text = re.sub(r'<[^>]+>', '', text)
            text = re.sub(r'&[^;]+;', ' ', text)  # Basic HTML entities
        
        return text.strip()
    
    def collect_for_keyword(self, keyword: str, max_stories: int = 50) -> List[Dict[str, Any]]:
        """
        Collect HN stories matching a keyword.
        
        Args:
            keyword: Search keyword/topic
            max_stories: Maximum stories to collect
            
        Returns:
            List of story events ready for database insertion
        """
        collected_events = []
        seen_urls = set()  # For deduplication
        
        # Get both top and new stories for better coverage
        story_types = ["top", "new"]
        
        for story_type in story_types:
            logger.info(f"Collecting {story_type} HN stories for keyword '{keyword}'")
            
            story_ids = self._get_story_ids(story_type, limit=max_stories)
            if not story_ids:
                continue
            
            stories = []
            for story_id in story_ids:
                story_data = self._get_story_details(story_id)
                if story_data:
                    stories.append(story_data)
                
                # Small delay to be respectful to HN API
                import time
                time.sleep(0.1)
            
            # Filter to recent stories
            recent_stories = self._filter_recent_stories(stories)
            
            # Filter by keyword in title
            keyword_lower = keyword.lower()
            matching_stories = []
            
            for story in recent_stories:
                title = story.get('title', '').lower()
                if keyword_lower in title:
                    matching_stories.append(story)
            
            # Convert to events with deduplication
            for story in matching_stories:
                story_url = self._get_story_url(story)
                
                # Skip if URL already seen (deduplication)
                if story_url in seen_urls:
                    continue
                
                # Validate URL
                if not self._validate_url(story_url):
                    continue
                
                # Extract metrics
                metrics = self._extract_metrics(story)
                
                # Get story text content
                story_text = self._get_story_text(story)
                
                event = {
                    'ts': datetime.now(timezone.utc).isoformat(),
                    'src': 'hn',
                    'url': story_url,
                    'title': story.get('title', ''),
                    'text': story_text,
                    'topic_guess': keyword,  # Will be updated by matcher
                    'metrics_json': json.dumps(metrics)
                }
                
                collected_events.append(event)
                seen_urls.add(story_url)  # Mark URL as seen
                
                # Stop if we have enough stories
                if len(collected_events) >= max_stories:
                    break
            
            logger.info(f"Collected {len(matching_stories)} matching stories from {story_type} for '{keyword}'")
            
            # Stop if we have enough stories
            if len(collected_events) >= max_stories:
                break
        
        logger.info(f"Total collected for '{keyword}': {len(collected_events)} events (deduplicated)")
        return collected_events
    
    def get_collector_stats(self) -> Dict[str, Any]:
        """Get collector statistics."""
        return {
            'base_url': self.base_url,
            'user_agent': self.session.headers.get('User-Agent'),
            'days_limit': self.days_limit
        }