import re
import json
import pandas as pd
from datetime import datetime
from src.FavIconExtractor import FavIconExtractor
from src.LinkFinder import LinkFinder
from src.RedisManager import RedisManager
from src.TextExtractor import TextExtractor
from src.TitleExtractor import TitleExtractor
from src.ParquetManager import ParquetManager

def is_valid_url(url):
    """Validate URL format."""
    return bool(re.match(r'^(https?://)([a-zA-Z0-9.-]+\.[a-zA-Z]{2,})(:[0-9]+)?(/.*)?$', url, re.IGNORECASE))

class Spider:
    
    def __init__(self, data_file: str = "data/data.parquet"):
        self.redis_manager = RedisManager()
        self.parquet_manager = ParquetManager(data_file)

        # Cache URLs in memory to reduce Redis calls
        self.crawled_urls = set(self.redis_manager.get_crawled())
        self.queue_urls = set(self.redis_manager.get_queue())

    def make_json(self, url, favico, title, headings, content, page_filter):
        """Create a JSON object from extracted data."""
        return {
            "url": url,
            "favicon": favico or "",
            "title": title or "",
            "headings": json.dumps(headings, ensure_ascii=False) if isinstance(headings, list) else str(headings) or "",
            "content": json.dumps(content, ensure_ascii=False) if isinstance(content, list) else str(content) or "",
            "filters": page_filter or "all",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    
    def crawl_page(self, thread_name, url):
        """Crawl a webpage, extract data, and store results."""
        if url in self.crawled_urls:
            print(f"🔁 {url} already crawled.")
            return  # Skip already crawled URLs
        if url.endswith('/home'):
            return

        print(f"🕷️ {thread_name} crawling: {url}")
        print(f"🔗 Queue: {len(self.queue_urls)} | Crawled: {len(self.crawled_urls)}")

        # Remove URL from queue before fetching
        self.redis_manager.delete_queue_url(url)
        self.queue_urls.discard(url)

        try:
            # Create extractor instances
            link_finder = LinkFinder(url)
            favicon_extractor = FavIconExtractor(url)
            text_extractor = TextExtractor(url)
            title_extractor = TitleExtractor(url)

            # Extract Data
            links = link_finder.get_links()  
            favico = favicon_extractor.get_favicon()
            title = title_extractor.get_title()
            headings = text_extractor.extract_headings()
            content = text_extractor.extract_contents()
            page_filter = text_extractor.extract_filters()

            # Store new links in queue
            for link in links:
                if link not in self.crawled_urls and is_valid_url(link):
                    self.redis_manager.add_queue_url(link)
                    self.queue_urls.add(link)

            # Skip empty pages
            if not title and not headings and not content:
                print(f"⚠️ Skipping empty page: {url}")
                return

            # Save extracted data
            json_data = self.make_json(url, favico, title, headings, content, page_filter)
            df = pd.DataFrame([json_data])  # Convert to DataFrame
            self.parquet_manager.write_data(df)

            # Mark the page as crawled
            self.redis_manager.add_crawled_url(url)
            self.crawled_urls.add(url)

        except Exception as e:
            print(f"❌ Error crawling {url}: {e}")

    def clear_data(self):
        """Clear Redis data."""
        self.redis_manager.clear_data()
        print("Data cleared from Redis.")