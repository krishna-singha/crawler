import sys
import os
import pytest
from unittest.mock import patch
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.Spider import Spider

@pytest.fixture
def spider():
    with patch("src.Spider.RedisManager") as MockRedisManager, \
         patch("src.Spider.ParquetManager") as MockParquetManager:

        mock_redis = MockRedisManager.return_value
        mock_redis.get_crawled.return_value = []
        mock_redis.get_queue.return_value = []

        mock_parquet = MockParquetManager.return_value
        mock_parquet.write_data.return_value = None

        spider = Spider()
        return spider


@patch("src.Spider.TitleExtractor")
@patch("src.Spider.TextExtractor")
@patch("src.Spider.FavIconExtractor")
@patch("src.Spider.LinkFinder")
def test_crawl_page_basic(
    mock_link_finder,
    mock_favicon_extractor,
    mock_text_extractor,
    mock_title_extractor,
    spider,
):
    url = "https://example.com"

    # Mocks for extractors
    mock_link_finder.return_value.get_links.return_value = ["https://example.com/page2"]
    mock_favicon_extractor.return_value.get_favicon.return_value = "https://example.com/favicon.ico"
    mock_title_extractor.return_value.get_title.return_value = "Example Title"
    mock_text_extractor.return_value.extract_headings.return_value = ["Heading"]
    mock_text_extractor.return_value.extract_contents.return_value = ["Some content"]
    mock_text_extractor.return_value.extract_filters.return_value = "test"

    # Call method under test
    spider.crawl_page("Thread-1", url)

    # Assertions
    assert url in spider.crawled_urls
    spider.redis_manager.add_crawled_url.assert_called_with(url)
    spider.parquet_manager.write_data.assert_called_once()
    spider.redis_manager.add_queue_url.assert_called_with("https://example.com/page2")


def test_make_json_structure(spider):
    data = spider.make_json(
        url="https://test.com",
        favico="https://test.com/favicon.ico",
        title="Test Title",
        headings=["H1", "H2"],
        content=["Some content"],
        page_filter="test"
    )
    assert data["url"] == "https://test.com"
    assert data["title"] == "Test Title"
    assert data["filters"] == "test"
    assert isinstance(data["headings"], str)
    assert isinstance(data["content"], str)
    assert "timestamp" in data


def test_skip_duplicate_url(spider):
    url = "https://already-crawled.com"
    spider.crawled_urls.add(url)

    with patch("builtins.print") as mock_print:
        spider.crawl_page("Thread-1", url)
        mock_print.assert_any_call(f"🔁 {url} already crawled.")
