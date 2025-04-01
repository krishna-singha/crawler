import redis
from config.config import START_URL

class RedisManager:
    _instance = None  # Singleton instance

    def __new__(cls, *args, **kwargs):
        """Ensure only one instance of RedisManager is created."""
        if not cls._instance:
            cls._instance = super(RedisManager, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self, host="localhost", port=6379, decode_responses=True):
        """Initialize Redis connection."""
        if not hasattr(self, 'r'):  # Avoid reinitializing the connection
            try:
                self.r = redis.Redis(host=host, port=port, decode_responses=decode_responses)
                self.r.ping()  # Test connection
                print("✅ Connected to Redis")

                # Ensure START_URL is added if necessary
                self.ensure_start_url()

            except redis.ConnectionError as e:
                print(f"❌ Redis connection error: {e}")
                exit("Redis is required for this application. Exiting...")

    def ensure_start_url(self):
        """Ensure START_URL is added if queue and crawled lists are empty."""
        if not self.get_queue() and not self.get_crawled():
            print(f"🏁 No URLs found, adding START_URL: {START_URL}")
            self.add_queue_url(START_URL)

    def normalize_url(self, url):
        """Normalize URLs by removing trailing slashes."""
        if url.endswith('/'):
            return url.rstrip('/')
        return url

    def is_crawled(self, url):
        """Check if the URL has already been crawled."""
        return self.r.sismember("crawled", url)

    def add_queue_url(self, url):
        """Add a URL to the queue if it's not already queued or crawled."""
        normalized_url = self.normalize_url(url)

        # Check if the normalized URL is already in the crawled or queue sets
        if self.is_crawled(normalized_url) or self.r.sismember("queue_set", normalized_url):
            print(f"🔁 Skipping duplicate URL: {url}")
            return False

        with self.r.pipeline() as pipe:
            pipe.sadd("queue_set", normalized_url)  # Track normalized URL in a set to prevent duplicates
            pipe.rpush("queue", normalized_url)  # Add to FIFO queue
            pipe.execute()
        print(f"📌 Added to queue: {url}")
        return True

    def add_crawled_url(self, url):
        """Move a URL from the queue to the crawled set."""
        normalized_url = self.normalize_url(url)
        with self.r.pipeline() as pipe:
            pipe.srem("queue_set", normalized_url)  # Remove from queue tracking
            pipe.sadd("crawled", normalized_url)  # Mark as crawled
            pipe.lrem("queue", 0, normalized_url)  # Ensure removal from queue
            pipe.execute()
        print(f"✅ Marked as crawled: {url}")

    def get_queue(self):
        """Retrieve all queued URLs."""
        return self.r.lrange("queue", 0, -1)

    def get_crawled(self):
        """Retrieve all crawled URLs."""
        return self.r.smembers("crawled")

    def delete_queue_url(self, url):
        """Remove a URL from the queue."""
        normalized_url = self.normalize_url(url)
        with self.r.pipeline() as pipe:
            pipe.lrem("queue", 0, normalized_url)
            pipe.srem("queue_set", normalized_url)
            pipe.execute()
        print(f"🗑️ Removed from queue: {url}")

    def clear_data(self):
        """Clear both queue and crawled data."""
        self.r.delete("queue", "queue_set", "crawled")
        print("🧹 Cleared all Redis data.")

    def get_all_data(self):
        """Retrieve all queued and crawled URLs in a single call."""
        queue_list, crawled_set = self.r.lrange("queue", 0, -1), self.r.smembers("crawled")
        
        print(f"📜 Queued URLs: {queue_list}")
        print(f"✅ Crawled URLs: {crawled_set}")
        return queue_list, crawled_set
