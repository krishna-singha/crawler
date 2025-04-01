import httpx
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from src.DomainExtractor import DomainExtractor

class LinkFinder:
    def __init__(self, url: str):
        self.url = url
        self.domain_extractor = DomainExtractor(url)
        self.domain = self.domain_extractor.get_domain_name()
        self.soup = self._get_soup()
        self.links = self._extract_links() if self.soup else set()

    def _get_soup(self):
        """Fetch the webpage using httpx and return a BeautifulSoup object."""
        try:
            response = httpx.get(self.url, timeout=10, follow_redirects=True)
            response.raise_for_status()
            return BeautifulSoup(response.text, "html.parser")
        except httpx.RequestError as e:
            print(f"⚠️ Error fetching page {self.url}: {e}")  # Print instead of logging
            return None

    def _extract_links(self):
        """Extract and return all valid links from the webpage."""
        links = set()

        for link in self.soup.find_all("a", href=True):
            href = link["href"].strip()
            full_url = urljoin(self.url, href)  # Convert relative URLs to absolute

            # Ensure the link belongs to the same domain
            if full_url.startswith("http") and self.domain in full_url:
                links.add(full_url)

        return links

    def get_links(self):
        """Return the extracted links."""
        return self.links

# # Example Usage:
# url = "https://www.iitkgp.ac.in/"
# finder = LinkFinder(url)
# print(finder.get_links())
