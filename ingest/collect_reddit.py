# ingest/collect_reddit.py
import os
import json
import logging
import requests
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

class RedditCollector:
    """Collects recent Reddit posts matching topics using OAuth API."""
    
    def __init__(self, days_limit: int = 7):
        self.days_limit = days_limit
        self.client_id = os.getenv('REDDIT_CLIENT_ID')
        self.client_secret = os.getenv('REDDIT_SECRET')
        self.user_agent = os.getenv('REDDIT_USER_AGENT', 'jerdev-trend-radar')
        
        if not all([self.client_id, self.client_secret]):
            raise ValueError("Reddit credentials required. Set REDDIT_CLIENT_ID and REDDIT_SECRET environment variables.")
        
        # Reddit API endpoints
        self.auth_url = "https://www.reddit.com/api/v1/access_token"
        self.base_url = "https://oauth.reddit.com"
        
        # Subreddits to monitor
        self.subreddits = [
            'programming',
            'devops', 
            'dataengineering',
            'machinelearning',
            'javascript',
            'rust'
        ]
        
        self.access_token = None
        self.token_expires = None
        
        logger.info(f"Reddit collector initialized with {days_limit} days limit")
    
    def _get_access_token(self) -> bool:
        """
        Get OAuth access token from Reddit using client_credentials only.
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            auth = requests.auth.HTTPBasicAuth(self.client_id, self.client_secret)
            data = {
                'grant_type': 'client_credentials'
            }
            headers = {
                'User-Agent': self.user_agent
            }
            
            response = requests.post(
                self.auth_url,
                auth=auth,
                data=data,
                headers=headers,
                timeout=10
            )
            
            if response.status_code == 200:
                token_data = response.json()
                self.access_token = token_data['access_token']
                # Reddit tokens typically last 1 hour
                self.token_expires = datetime.now(timezone.utc) + timedelta(seconds=token_data.get('expires_in', 3600))
                logger.info("Reddit access token obtained successfully")
                return True
            else:
                logger.error(f"Failed to get Reddit token: {response.status_code} - {response.text}")
                return False
                
        except requests.RequestException as e:
            logger.error(f"Request failed while getting Reddit token: {e}")
            return False
    
    def _is_token_valid(self) -> bool:
        """Check if current access token is still valid."""
        if not self.access_token or not self.token_expires:
            return False
        return datetime.now(timezone.utc) < self.token_expires
    
    def _get_headers(self) -> Dict[str, str]:
        """Get headers for Reddit API requests."""
        if not self._is_token_valid():
            if not self._get_access_token():
                raise Exception("Failed to get valid Reddit access token")
        
        return {
            'Authorization': f'bearer {self.access_token}',
            'User-Agent': self.user_agent
        }
    
    def _get_subreddit_posts(self, subreddit: str, sort: str = 'new', limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get posts from a specific subreddit.
        
        Args:
            subreddit: Subreddit name
            sort: Sort order ('new', 'hot', 'top')
            limit: Maximum posts to retrieve
            
        Returns:
            List of post data
        """
        try:
            url = f"{self.base_url}/r/{subreddit}/{sort}.json"
            params = {
                'limit': min(limit, 100),
                'raw_json': 1
            }
            
            response = requests.get(
                url,
                headers=self._get_headers(),
                params=params,
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                posts = []
                for child in data.get('data', {}).get('children', []):
                    post_data = child.get('data', {})
                    posts.append(post_data)
                return posts
            elif response.status_code == 401:
                logger.warning("Reddit token expired, refreshing...")
                self.access_token = None
                return self._get_subreddit_posts(subreddit, sort, limit)
            else:
                logger.error(f"Reddit API error: {response.status_code} - {response.text}")
                return []
                
        except requests.RequestException as e:
            logger.error(f"Request failed for subreddit {subreddit}: {e}")
            return []
    
    def _filter_recent_posts(self, posts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Filter posts to only include recent ones.
        
        Args:
            posts: List of post data from Reddit API
            
        Returns:
            List of recent posts
        """
        cutoff_time = datetime.now(timezone.utc) - timedelta(days=self.days_limit)
        recent_posts = []
        
        for post in posts:
            # Reddit uses Unix timestamp
            created_utc = post.get('created_utc')
            if not created_utc:
                continue
                
            post_datetime = datetime.fromtimestamp(created_utc, tz=timezone.utc)
            if post_datetime >= cutoff_time:
                recent_posts.append(post)
        
        return recent_posts
    
    def _extract_metrics(self, post: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract metrics from post data.
        
        Args:
            post: Post data from Reddit API
            
        Returns:
            Dict with flat metrics structure
        """
        return {
            'ups': post.get('ups', 0),
            'num_comments': post.get('num_comments', 0),
            'subreddit': post.get('subreddit', ''),
            'created_utc': post.get('created_utc'),
            'score': post.get('score', 0),
            'upvote_ratio': post.get('upvote_ratio', 0),
            'author': post.get('author', ''),
            'is_self': post.get('is_self', False)
        }
    
    def _validate_url(self, url: str) -> bool:
        """Validate that URL is well-formed HTTP/HTTPS."""
        try:
            parsed = urlparse(url)
            return parsed.scheme in ('http', 'https') and parsed.netloc
        except Exception:
            return False
    
    def _get_post_url(self, post: Dict[str, Any]) -> str:
        """
        Get the URL for a post (external link or Reddit discussion).
        
        Args:
            post: Post data from Reddit API
            
        Returns:
            URL string
        """
        # For link posts, use the external URL
        if not post.get('is_self') and post.get('url'):
            return post['url']
        
        # For self posts, use Reddit discussion URL
        permalink = post.get('permalink', '')
        if permalink:
            return f"https://reddit.com{permalink}"
        
        # Fallback
        return f"https://reddit.com/r/{post.get('subreddit', '')}/comments/{post.get('id', '')}"
    
    def _get_post_text(self, post: Dict[str, Any]) -> str:
        """
        Get post text content.
        
        Args:
            post: Post data from Reddit API
            
        Returns:
            Post text content or empty string
        """
        # For self posts, get the selftext
        if post.get('is_self'):
            return post.get('selftext', '')
        
        # For link posts, get the title as text
        return post.get('title', '')
    
    def collect_for_keyword(self, keyword: str, max_posts: int = 50) -> List[Dict[str, Any]]:
        """
        Collect Reddit posts matching a keyword.
        
        Args:
            keyword: Search keyword/topic
            max_posts: Maximum posts to collect
            
        Returns:
            List of post events ready for database insertion
        """
        collected_events = []
        seen_urls = set()  # For deduplication
        
        for subreddit in self.subreddits:
            logger.info(f"Collecting Reddit posts from r/{subreddit} for keyword '{keyword}'")
            
            # Get both new and hot posts for better coverage
            for sort in ['new', 'hot']:
                posts = self._get_subreddit_posts(subreddit, sort, limit=50)
                if not posts:
                    continue
                
                # Filter to recent posts
                recent_posts = self._filter_recent_posts(posts)
                
                # Filter by keyword in title
                keyword_lower = keyword.lower()
                matching_posts = []
                
                for post in recent_posts:
                    title = post.get('title', '').lower()
                    if keyword_lower in title:
                        matching_posts.append(post)
                
                # Convert to events with deduplication
                for post in matching_posts:
                    post_url = self._get_post_url(post)
                    
                    # Skip if URL already seen (deduplication)
                    if post_url in seen_urls:
                        continue
                    
                    # Validate URL
                    if not self._validate_url(post_url):
                        continue
                    
                    # Extract metrics
                    metrics = self._extract_metrics(post)
                    
                    # Get post text content
                    post_text = self._get_post_text(post)
                    
                    event = {
                        'ts': datetime.now(timezone.utc).isoformat(),
                        'src': 'reddit',
                        'url': post_url,
                        'title': post.get('title', ''),
                        'text': post_text,
                        'topic_guess': keyword,  # Will be updated by matcher
                        'metrics_json': json.dumps(metrics)
                    }
                    
                    collected_events.append(event)
                    seen_urls.add(post_url)  # Mark URL as seen
                    
                    # Stop if we have enough posts
                    if len(collected_events) >= max_posts:
                        break
                
                logger.info(f"Collected {len(matching_posts)} matching posts from r/{subreddit} ({sort}) for '{keyword}'")
                
                # Stop if we have enough posts
                if len(collected_events) >= max_posts:
                    break
            
            # Stop if we have enough posts
            if len(collected_events) >= max_posts:
                break
        
        logger.info(f"Total collected for '{keyword}': {len(collected_events)} events (deduplicated)")
        return collected_events
    
    def get_collector_stats(self) -> Dict[str, Any]:
        """Get collector statistics."""
        return {
            'subreddits': self.subreddits,
            'user_agent': self.user_agent,
            'days_limit': self.days_limit,
            'token_valid': self._is_token_valid(),
            'token_expires': self.token_expires.isoformat() if self.token_expires else None
        }