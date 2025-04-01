import redis

class RedisManager:
    def __init__(self, host="localhost", port=6379, decode_responses=True):
        """Initialize Redis connection."""
        try:
            self.r = redis.Redis(host=host, port=port, decode_responses=decode_responses)
            self.r.ping()  # Test connection
            print("✅ Connected to Redis")
        except redis.ConnectionError as e:
            print(f"❌ Redis connection error: {e}")
            exit("Redis is required for this application. Exiting...")

    def clear_data(self):
        """Clear both queue and crawled data."""
        self.r.delete("queue", "queue_set", "crawled")

    def get_all_data(self):
        """Retrieve all queued and crawled URLs in a single call."""
        with self.r.pipeline() as pipe:
            pipe.lrange("queue", 0, -1)
            pipe.smembers("crawled")
            queue_list, crawled_set = pipe.execute()
        
        print(f"📜 Queued URLs: {queue_list}")
        print(f"✅ Crawled URLs: {crawled_set}")
        return queue_list, crawled_set

#Uncomment to test
if __name__ == "__main__":
    redis_manager = RedisManager()
    redis_manager.clear_data()  # Reset for testing
    redis_manager.get_all_data()
