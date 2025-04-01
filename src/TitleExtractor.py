import httpx
import re
from bs4 import BeautifulSoup

class TitleExtractor:
    def __init__(self, url: str):
        self.url = url
        self.soup = self._get_soup()

    def clean_title(self, title: str) -> str:
        """Clean the title by removing unwanted characters and special symbols."""
        title = re.sub(r"[:=]", "", title)
        return title.strip()

    def _get_soup(self):
        """Fetch the webpage using httpx and return a BeautifulSoup object."""
        try:
            response = httpx.get(self.url, timeout=10, follow_redirects=True)
            response.raise_for_status()
            return BeautifulSoup(response.text, "html.parser")
        except httpx.RequestError as e:
            print(f"Error fetching page: {e}")
            return None

    def get_title(self):
        """Extract and return the cleaned title of the page."""
        if not self.soup:
            return "Title Not Found"
        
        title = self.soup.title.string if self.soup.title else ""
        return self.clean_title(title)

# # Example Usage:
# if __name__ == "__main__":
#     url = "https://gateoffice.iitkgp.ac.in/law"
#     extractor = TitleExtractor(url)
#     print(extractor.get_title())
