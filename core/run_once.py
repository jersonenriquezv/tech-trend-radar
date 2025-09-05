# core/run_once.py
import os
import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Any

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))

from core.db import TrendRadarDB
from core.matcher import TopicMatcher
from core.cache import CacheManager
from ingest.collect_github import GitHubCollector
from ingest.collect_hn import HackerNewsCollector

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TrendRadarRunner:
    """Orchestrates a complete run of the Tech Trend Radar system."""
    
    def __init__(self):
        self.db = TrendRadarDB()
        self.matcher = TopicMatcher()
        self.cache = CacheManager()
        
        # Initialize collectors
        self.github_collector = GitHubCollector()
        self.hn_collector = HackerNewsCollector()
        
        # Configuration from environment
        self.max_topics = int(os.getenv('MAX_TOPICS_PER_RUN', 80))
        self.page_limit = int(os.getenv('PER_SOURCE_PAGE_LIMIT', 2))
        
        logger.info(f"Trend Radar Runner initialized - Max topics: {self.max_topics}, Page limit: {self.page_limit}")
    
    def run_once(self) -> Dict[str, Any]:
        """
        Execute a complete run of the system.
        
        Returns:
            Dict with run statistics
        """
        start_time = datetime.now(timezone.utc)
        logger.info("Starting Tech Trend Radar run...")
        
        # Get topics for this run
        topics = self.matcher.get_topics_for_run(self.max_topics)
        logger.info(f"Selected {len(topics)} topics for this run")
        
        # Initialize statistics
        stats = {
            'start_time': start_time.isoformat(),
            'topics_processed': len(topics),
            'sources': {
                'github': {'collected': 0, 'inserted': 0, 'duplicates': 0, 'no_match': 0},
                'hn': {'collected': 0, 'inserted': 0, 'duplicates': 0, 'no_match': 0}
            },
            'total_events': 0
        }
        
        # Process each topic
        for i, topic_info in enumerate(topics, 1):
            topic = topic_info['topic']
            logger.info(f"Processing topic {i}/{len(topics)}: {topic}")
            
            # Collect from GitHub
            try:
                github_events = self.github_collector.collect_for_keyword(
                    topic, max_pages=self.page_limit
                )
                stats['sources']['github']['collected'] += len(github_events)
                
                # Process GitHub events
                for event in github_events:
                    result = self._process_event(event, 'github')
                    stats['sources']['github'][result] += 1
                    
            except Exception as e:
                logger.error(f"GitHub collection failed for topic '{topic}': {e}")
            
            # Collect from Hacker News
            try:
                hn_events = self.hn_collector.collect_for_keyword(
                    topic, max_stories=50
                )
                stats['sources']['hn']['collected'] += len(hn_events)
                
                # Process HN events
                for event in hn_events:
                    result = self._process_event(event, 'hn')
                    stats['sources']['hn'][result] += 1
                    
            except Exception as e:
                logger.error(f"HN collection failed for topic '{topic}': {e}")
        
        # Calculate totals
        stats['total_events'] = (
            stats['sources']['github']['inserted'] + 
            stats['sources']['hn']['inserted']
        )
        
        end_time = datetime.now(timezone.utc)
        stats['end_time'] = end_time.isoformat()
        stats['duration_seconds'] = (end_time - start_time).total_seconds()
        
        # Print summary
        self._print_summary(stats)
        
        return stats
    
    def _process_event(self, event: Dict[str, Any], source: str) -> str:
        """
        Process a single event: match topic, validate, and insert.
        
        Args:
            event: Event data from collector
            source: Source identifier
            
        Returns:
            Result: 'inserted', 'duplicates', or 'no_match'
        """
        # Find best topic match
        match_result = self.matcher.find_best_match(event['title'], event['text'])
        
        if not match_result:
            return 'no_match'
        
        topic, category = match_result
        
        # Update event with matched topic
        event['topic_guess'] = topic
        
        # Validate URL
        if not self.matcher.validate_url(event['url']):
            return 'no_match'
        
        # Validate metrics JSON
        try:
            json.loads(event['metrics_json'])
        except json.JSONDecodeError:
            return 'no_match'
        
        # Insert into database
        if self.db.insert_event(event):
            return 'inserted'
        else:
            return 'duplicates'
    
    def _print_summary(self, stats: Dict[str, Any]) -> None:
        """Print a summary of the run results."""
        print("\n" + "="*60)
        print("TECH TREND RADAR - RUN SUMMARY")
        print("="*60)
        
        print(f"Start time: {stats['start_time']}")
        print(f"End time: {stats['end_time']}")
        print(f"Duration: {stats['duration_seconds']:.1f} seconds")
        print(f"Topics processed: {stats['topics_processed']}")
        
        print("\nSOURCE BREAKDOWN:")
        for source, data in stats['sources'].items():
            print(f"  {source.upper()}:")
            print(f"    Collected: {data['collected']}")
            print(f"    Inserted: {data['inserted']}")
            print(f"    Duplicates: {data['duplicates']}")
            print(f"    No match: {data['no_match']}")
        
        print(f"\nTOTAL EVENTS INSERTED: {stats['total_events']}")
        print("="*60)

def main():
    """Main entry point for the runner."""
    try:
        runner = TrendRadarRunner()
        stats = runner.run_once()
        
        # Exit with success code
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"Run failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()