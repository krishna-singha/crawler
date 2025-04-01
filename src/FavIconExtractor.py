import httpx
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

class FavIconExtractor:
    def __init__(self, url: str):
        self.url = self._get_base_url(url)
        self.soup = self._get_soup()

    def _get_base_url(self, url: str) -> str:
        """Extracts and returns the base URL (scheme + domain)."""
        parsed_url = urlparse(url)
        return f"{parsed_url.scheme}://{parsed_url.netloc}"

    def _get_soup(self):
        """Fetch the webpage using httpx and return a BeautifulSoup object."""
        try:
            response = httpx.get(self.url, timeout=10, follow_redirects=True)
            response.raise_for_status()
            return BeautifulSoup(response.text, "html.parser")
        except httpx.RequestError as e:
            print(f"Error fetching page: {e}")
            return BeautifulSoup("", "html.parser")

    def get_favicon(self) -> str | None:
        """Extracts the favicon URL, handling both absolute and relative URLs."""
        if not self.soup:
            return None

        # Look for the favicon in <link> tags
        favicon_link = self.soup.find("link", rel=lambda rel: rel and "icon" in rel.lower())
        if favicon_link and "href" in favicon_link.attrs:
            return urljoin(self.url, favicon_link["href"])

        # Fallback: Common default location for favicons
        return None

# Example Usage:
url = "https://library.iitkgp.ac.in"
fav_extractor = FavIconExtractor(url)
print(fav_extractor.get_favicon())