import threading
import logging
from queue import Queue, Empty
from src.Spider import Spider
from src.RedisManager import RedisManager
from config.config import START_URL, NUMBER_OF_THREADS

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(threadName)s - %(message)s")

class Crawler:
    def __init__(self, start_url=START_URL, number_of_threads=NUMBER_OF_THREADS):
        self.queue = Queue()
        self.redis_manager = RedisManager()
        self.start_url = start_url
        self.number_of_threads = number_of_threads
        self.spider = Spider()
        self.threads = []

    def create_workers(self):
        """Create worker threads to process the queue."""
        for _ in range(self.number_of_threads):
            t = threading.Thread(target=self.work, daemon=True)
            t.start()
            self.threads.append(t)

    def work(self):
        """Worker thread that processes URLs from the queue."""
        while True:
            try:
                url = self.queue.get(timeout=5)  # Prevent infinite blocking
            except Empty:
                if self.queue.empty():
                    break  # Exit when no jobs remain
                continue
            
            if url is None:  
                break  # Stop thread when `None` is received
            
            try:
                logging.info(f"Crawling: {url}")
                self.spider.crawl_page(threading.current_thread().name, url)
            except Exception as e:
                logging.error(f"❌ Error crawling {url}: {e}")
            finally:
                self.queue.task_done()

    def load_queue(self):
        """Load URLs from Redis queue with error handling."""
        try:
            return self.redis_manager.get_queue()
        except Exception as e:
            logging.error(f"❌ Failed to fetch queue from Redis: {e}")
            return []

    def create_jobs(self):
        """Load jobs from Redis queue into the local queue."""
        links = self.load_queue()
        if not links:
            logging.info("✅ No links in queue, exiting...")
            return False  

        for link in links:
            self.queue.put(link)

        return True

    def crawl(self):
        """Main crawl loop that loads jobs and processes them."""
        self.create_workers()

        while True:
            if not self.create_jobs():
                logging.info("✅ No links to process. Exiting...")
                break  

            self.queue.join()  # Ensure all tasks are marked as done

        self.stop_workers()

        for thread in self.threads:
            if thread.is_alive():
                thread.join()  # Ensure all threads exit before quitting

    def stop_workers(self):
        """Stop worker threads by sending `None` signals to the queue."""
        for _ in range(self.number_of_threads):
            self.queue.put(None)

        for thread in self.threads:
            thread.join()  # Wait for all workers to finish

if __name__ == "__main__":
    try:
        crawler = Crawler()
        crawler.crawl()
    except KeyboardInterrupt:
        logging.info("\n🛑 Process interrupted. Stopping workers...")
        crawler.stop_workers()
