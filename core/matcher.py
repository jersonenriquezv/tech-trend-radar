# core/matcher.py
import json
import logging
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse
from datetime import datetime

logger = logging.getLogger(__name__)

class TopicMatcher:
    """Matches events against topics and aliases with anti-noise rules."""
    
    # Anti-noise rules for ambiguous topics (v1 conservative approach)
    ANTI_NOISE_RULES = {
        "rust": ["rustlang"],
        "go": ["golang"],
        "bun": ["bunjs"],
        "ray": ["ray.io"],
        "spark": ["apache spark", "pyspark"],
        "kafka": ["apache kafka", "kafka streams"]
    }
    
    def __init__(self, topics_file: str = "config/topics.json"):
        self.topics_file = Path(topics_file)
        self.topics_data = self._load_topics()
        self._build_topic_index()
    
    def _load_topics(self) -> Dict:
        """Load topics configuration from JSON file - handles both array and dict formats."""
        try:
            with open(self.topics_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
                # Handle both formats: array of topics or dict with "topics" key
                if isinstance(data, list):
                    topics_list = data
                    logger.info(f"Loaded {len(topics_list)} topics from {self.topics_file} (array format)")
                elif isinstance(data, dict) and "topics" in data:
                    topics_list = data["topics"]
                    logger.info(f"Loaded {len(topics_list)} topics from {self.topics_file} (dict format)")
                else:
                    logger.warning(f"Unexpected format in {self.topics_file}, using empty topics list")
                    topics_list = []
                
                return {"topics": topics_list}
                
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.error(f"Failed to load topics: {e}")
            return {"topics": []}
    
    def _build_topic_index(self) -> None:
        """Build internal index with precompiled regex patterns for efficient topic matching."""
        self.topic_patterns = []
        
        for topic_info in self.topics_data.get('topics', []):
            topic = topic_info.get('topic', '').lower()
            aliases = [alias.lower() for alias in topic_info.get('aliases', [])]
            category = topic_info.get('category', '')
            
            # Apply anti-noise rules
            if topic in self.ANTI_NOISE_RULES:
                # Use only the specific allowed patterns for ambiguous topics
                allowed_patterns = self.ANTI_NOISE_RULES[topic]
                for pattern in allowed_patterns:
                    # Precompile regex with lookarounds for non-alphanumeric boundaries
                    regex_pattern = rf'(?<![A-Za-z0-9]){re.escape(pattern)}(?![A-Za-z0-9])'
                    compiled_pattern = re.compile(regex_pattern, re.IGNORECASE)
                    
                    self.topic_patterns.append({
                        'pattern': pattern,
                        'compiled_regex': compiled_pattern,
                        'topic': topic,
                        'category': category,
                        'is_exact_topic': False,
                        'length': len(pattern)
                    })
            else:
                # Normal topic matching
                regex_pattern = rf'(?<![A-Za-z0-9]){re.escape(topic)}(?![A-Za-z0-9])'
                compiled_pattern = re.compile(regex_pattern, re.IGNORECASE)
                
                self.topic_patterns.append({
                    'pattern': topic,
                    'compiled_regex': compiled_pattern,
                    'topic': topic,
                    'category': category,
                    'is_exact_topic': True,
                    'length': len(topic)
                })
                
                # Add aliases
                for alias in aliases:
                    regex_pattern = rf'(?<![A-Za-z0-9]){re.escape(alias)}(?![A-Za-z0-9])'
                    compiled_pattern = re.compile(regex_pattern, re.IGNORECASE)
                    
                    self.topic_patterns.append({
                        'pattern': alias,
                        'compiled_regex': compiled_pattern,
                        'topic': topic,
                        'category': category,
                        'is_exact_topic': False,
                        'length': len(alias)
                    })
        
        logger.info(f"Built {len(self.topic_patterns)} regex patterns for topic matching")
    
    def find_best_match(self, title: str, text: str = "") -> Optional[Tuple[str, str]]:
        """
        Find the best matching topic for given title and text.
        
        Args:
            title: Event title to match against
            text: Additional text content (optional)
            
        Returns:
            Tuple of (topic, category) if match found, None otherwise
        """
        if not title:
            return None
            
        content = f"{title} {text}".lower()
        matches = []
        
        for pattern_info in self.topic_patterns:
            compiled_regex = pattern_info['compiled_regex']
            
            # Use precompiled regex for efficient matching
            if compiled_regex.search(content):
                matches.append(pattern_info)
        
        if not matches:
            return None
        
        # Select best match based on priority rules:
        # 1. Exact topic match over alias
        # 2. If tie, longer pattern wins
        best_match = max(matches, key=lambda x: (x['is_exact_topic'], x['length']))
        
        return best_match['topic'], best_match['category']
    
    def validate_url(self, url: str) -> bool:
        """
        Validate that URL is well-formed HTTP/HTTPS.
        
        Args:
            url: URL string to validate
            
        Returns:
            bool: True if valid HTTP/HTTPS URL
        """
        try:
            parsed = urlparse(url)
            return parsed.scheme in ('http', 'https') and parsed.netloc
        except Exception:
            return False
    
    def get_topics_for_run(self, max_topics: int = 80, category_rotation: int = 0) -> List[Dict]:
        """
        Get topics for a run with category rotation and internal category rotation.
        
        Args:
            max_topics: Maximum number of topics to return
            category_rotation: Rotation offset for category selection
            
        Returns:
            List of topic dictionaries
        """
        all_topics = self.topics_data.get('topics', [])
        
        if len(all_topics) <= max_topics:
            return all_topics
        
        # Group by category
        categories = {}
        for topic in all_topics:
            cat = topic.get('category', 'general')
            if cat not in categories:
                categories[cat] = []
            categories[cat].append(topic)
        
        # Calculate topics per category
        topics_per_category = max_topics // len(categories)
        remaining = max_topics % len(categories)
        
        # Rotate starting category
        category_names = list(categories.keys())
        start_idx = category_rotation % len(category_names)
        rotated_categories = category_names[start_idx:] + category_names[:start_idx]
        
        # Add internal rotation within each category to avoid bias
        day_of_year = datetime.now().timetuple().tm_yday
        
        selected_topics = []
        for i, cat in enumerate(rotated_categories):
            cat_topics = categories[cat]
            
            # Apply internal rotation within category to avoid always taking first topics
            internal_offset = (category_rotation + day_of_year) % len(cat_topics)
            rotated_cat_topics = cat_topics[internal_offset:] + cat_topics[:internal_offset]
            
            # Add extra topic to first few categories if there are remaining
            extra = 1 if i < remaining else 0
            selected_topics.extend(rotated_cat_topics[:topics_per_category + extra])
        
        return selected_topics[:max_topics]