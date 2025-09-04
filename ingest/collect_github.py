# ingest/collect_github.py
import os
import json
import logging
import requests
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

class GitHubCollector:
    """Collects recent GitHub repositories matching topics with rate limiting and filtering."""
    
    def __init__(self, token: Optional[str] = None):
        self.token = token or os.getenv('GITHUB_TOKEN')
        if not self.token:
            raise ValueError("GitHub token required. Set GITHUB_TOKEN environment variable.")
        
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'token {self.token}',
            'Accept': 'application/vnd.github.v3+json',
            'User-Agent': 'jerdev-trend-radar'
        })
        
        # Rate limiting tracking
        self.rate_limit_remaining = None
        self.rate_limit_reset = None
        
        logger.info("GitHub collector initialized")
    
    def _check_rate_limit(self) -> bool:
        """Check current rate limit status."""
        try:
            response = self.session.get('https://api.github.com/rate_limit')
            response.raise_for_status()
            
            rate_data = response.json()['resources']['search']
            self.rate_limit_remaining = rate_data['remaining']
            self.rate_limit_reset = rate_data['reset']
            
            if self.rate_limit_remaining <= 0:
                reset_time = datetime.fromtimestamp(self.rate_limit_reset, tz=timezone.utc)
                wait_time = reset_time - datetime.now(timezone.utc)
                logger.warning(f"Rate limit exceeded. Reset at {reset_time}, wait {wait_time}")
                return False
            
            logger.debug(f"Rate limit: {self.rate_limit_remaining} requests remaining")
            return True
            
        except Exception as e:
            logger.error(f"Failed to check rate limit: {e}")
            return False
    
    def _search_repositories(self, keyword: str, page: int = 1, per_page: int = 100) -> Optional[Dict[str, Any]]:
        """
        Search GitHub repositories with keyword filtering.
        
        Args:
            keyword: Search keyword/topic
            page: Page number for pagination
            per_page: Results per page (max 100)
            
        Returns:
            Dict with search results or None if failed
        """
        if not self._check_rate_limit():
            return None
        
        # Build search query: keyword in name, description, or readme
        query = f'{keyword} in:name,description,readme'
        
        params = {
            'q': query,
            'sort': 'updated',
            'order': 'desc',
            'per_page': min(per_page, 100),
            'page': page
        }
        
        try:
            response = self.session.get(
                'https://api.github.com/search/repositories',
                params=params
            )
            
            if response.status_code == 429:  # Rate limited
                logger.warning(f"Rate limited for keyword '{keyword}' page {page}")
                return None
            elif response.status_code == 401:  # Unauthorized
                logger.error("GitHub token invalid or expired")
                return None
            elif response.status_code != 200:
                logger.error(f"GitHub API error: {response.status_code} - {response.text}")
                return None
            
            return response.json()
            
        except requests.RequestException as e:
            logger.error(f"Request failed for keyword '{keyword}' page {page}: {e}")
            return None
    
    def _filter_recent_repositories(self, repositories: List[Dict[str, Any]], days_limit: int = 7) -> List[Dict[str, Any]]:
        """
        Filter repositories to only include recent ones.
        
        Args:
            repositories: List of repository data from GitHub API
            days_limit: Maximum age in days
            
        Returns:
            List of recent repositories
        """
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_limit)
        recent_repos = []
        
        for repo in repositories:
            # Check pushed_at (last activity) or created_at
            pushed_at = repo.get('pushed_at')
            created_at = repo.get('created_at')
            
            if pushed_at:
                last_activity = datetime.fromisoformat(pushed_at.replace('Z', '+00:00'))
            elif created_at:
                last_activity = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
            else:
                continue
            
            if last_activity >= cutoff_date:
                recent_repos.append(repo)
        
        return recent_repos
    
    def _extract_metrics(self, repo: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract metrics from repository data.
        
        Args:
            repo: Repository data from GitHub API
            
        Returns:
            Dict with flat metrics structure
        """
        return {
            'stars': repo.get('stargazers_count', 0),
            'forks': repo.get('forks_count', 0),
            'watchers': repo.get('watchers_count', 0),
            'language': repo.get('language'),
            'pushed_at': repo.get('pushed_at'),
            'created_at': repo.get('created_at'),
            'full_name': repo.get('full_name'),
            'description': repo.get('description', ''),
            'topics': repo.get('topics', [])
        }
    
    def _validate_url(self, url: str) -> bool:
        """Validate that URL is well-formed HTTP/HTTPS."""
        try:
            parsed = urlparse(url)
            return parsed.scheme in ('http', 'https') and parsed.netloc
        except Exception:
            return False
    
    def collect_for_keyword(self, keyword: str, max_pages: int = 2) -> List[Dict[str, Any]]:
        """
        Collect repositories for a specific keyword.
        
        Args:
            keyword: Search keyword/topic
            max_pages: Maximum pages to collect
            
        Returns:
            List of repository events ready for database insertion
        """
        collected_events = []
        
        for page in range(1, max_pages + 1):
            logger.info(f"Collecting GitHub repositories for '{keyword}' page {page}")
            
            search_results = self._search_repositories(keyword, page)
            if not search_results:
                logger.warning(f"No results for '{keyword}' page {page}")
                break
            
            repositories = search_results.get('items', [])
            if not repositories:
                logger.info(f"No more repositories for '{keyword}' page {page}")
                break
            
            # Filter to recent repositories only
            recent_repos = self._filter_recent_repositories(repositories)
            
            for repo in recent_repos:
                # Validate repository URL
                repo_url = repo.get('html_url')
                if not repo_url or not self._validate_url(repo_url):
                    continue
                
                # Extract metrics
                metrics = self._extract_metrics(repo)
                
                event = {
                    'ts': datetime.now(timezone.utc).isoformat(),
                    'src': 'github',
                    'url': repo_url,
                    'title': repo.get('name', ''),
                    'text': repo.get('description', ''),
                    'topic_guess': keyword,  # Will be updated by matcher
                    'metrics_json': json.dumps(metrics)
                }
                
                collected_events.append(event)
            
            logger.info(f"Collected {len(recent_repos)} recent repositories for '{keyword}' page {page}")
            
            # Check if we should continue to next page
            if len(repositories) < 100:  # Last page
                break
        
        logger.info(f"Total collected for '{keyword}': {len(collected_events)} events")
        return collected_events
    
    def get_collector_stats(self) -> Dict[str, Any]:
        """Get collector statistics and rate limit info."""
        return {
            'rate_limit_remaining': self.rate_limit_remaining,
            'rate_limit_reset': self.rate_limit_reset,
            'reset_time': datetime.fromtimestamp(self.rate_limit_reset, tz=timezone.utc).isoformat() if self.rate_limit_reset else None
        }