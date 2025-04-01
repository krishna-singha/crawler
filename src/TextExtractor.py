import httpx
import re
from bs4 import BeautifulSoup


class TextExtractor:
    def __init__(self, url: str):
        self.url = url
        self.anchor_list = {word.lower() for word in {"more", "show", "hide", "read", "click", "here", "link", "view", "details", "visit", "website", "download", "apply", "submit", "check", "explore", "register", "help", "feedback", "report", "next", "previous", "proceed", "expand", "collapse", "edit", "checkout"}}

        # Filters (corrected hall list and other categories)
        self.filters = {
            "halls": {
                "hall", "hostel", "residence", "atal bihari vajpayee", "azad", "b r ambedkar", "gokhale",
                "homi j bhabha", "jagdish chandra bose", "nehru", "lalbahadur sastry", "lala lajpat rai",
                "madan mohan malviya", "megnad saha", "mother teresa", "nivedita", "patel", "radha krishnan",
                "rani laxmibai", "rajendra prasad", "sam", "savitribai phule", "sarojini naidu", "vsrc",
                "vidyasagar", "zakir hussain"
            },
            "departments": {"department", "dept", "school"},
            "faculties": {"professor", "faculty", "instructor", "teacher", "ta", "teaching assistant"},
            "courses": {"course", "subject", "module", "class", "lecture", "lab", "tutorial"},
            "exams": {"test", "exam", "mid sem", "end sem", "mid-sem", "end-sem", "mid-semester", "end-semester"},
            "gymkhana": {"gymkhana", "sports", "athletics", "games", "tournament", "competition"},
            "societies": {"society", "club", "cell"},
        }

        self.soup = self._get_soup()

    def _get_soup(self):
        """Fetch the webpage using httpx and return a BeautifulSoup object if it's HTML."""
        try:
            response = httpx.get(self.url, timeout=10, follow_redirects=True)
            response.raise_for_status()

            content_type = response.headers.get("Content-Type", "").lower()
            if "text/html" not in content_type:
                print(f"Skipping non-HTML content: {self.url} ({content_type})")
                return None

            soup = BeautifulSoup(response.text, "html.parser")
            self._clean_html(soup)
            return soup

        except httpx.RequestError as e:
            print(f"Error fetching page: {e}")
            return None
        except httpx.HTTPStatusError as e:
            print(f"HTTP error {e.response.status_code} for {self.url}")
            return None

    def _clean_html(self, soup):
        """Remove unnecessary HTML tags."""
        for tag in soup(["head", "header", "footer", "script", "style", "i"]):
            tag.decompose()

    def _clean_text(self, text):
        """Normalize spaces and remove unwanted characters, keep original case."""
        text = re.sub(r"\s+", " ", text).strip()  # Normalize spaces
        text = re.sub(r"['\"\+\(\)]", "", text)  # Remove unwanted characters
        return text if text else None

    def _contains_skip_word(self, text):
        """Check if a text contains any of the predefined anchor words (case-insensitive)."""
        text_lower = text.lower()
        return any(skip_word in text_lower for skip_word in self.anchor_list)
    
    def _is_numeric_heading(self, text):
        """Check if a heading is purely numeric or mostly consists of numbers."""
        return text.isdigit() or bool(re.fullmatch(r"[0-9\s|.,-]+", text))

    def extract_headings(self):
        """
        Extract and return all clean headings (H1 to H6), skipping:
        - Headings that contain a <button> inside.
        - Headings that are fully numeric or mostly numbers.
        - Duplicate headings.
        """
        if self.soup is None:
            print("Warning: BeautifulSoup object is not initialized.")
            return []

        headings_set = set()  # Track unique headings
        extracted_headings = []  # Maintain order

        for tag in self.soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6"]):
            if tag.find("button"):  # Skip headings containing a button
                continue
            
            text = tag.get_text(" ", strip=True)
            cleaned_text = self._clean_text(text)

            # Skip if it's empty or numeric
            if not cleaned_text or self._is_numeric_heading(cleaned_text):
                continue

            # Skip duplicates
            if cleaned_text not in headings_set:
                headings_set.add(cleaned_text)
                extracted_headings.append(cleaned_text)

        return extracted_headings

    def extract_contents(self):
        """
        Extract and return clean paragraphs, skipping:
        - Those with anchor words.
        - Those that are fully numeric or mostly numbers.
        - Duplicate content.
        """
        if not self.soup:
            print("Warning: BeautifulSoup object is not initialized.")
            return []

        contents = set()  # Use a set to store unique content
        extracted_list = []  # Final list to maintain order

        for tag in self.soup.find_all("p"):
            # Skip if any anchor inside contains a skip word
            if any(self._contains_skip_word(a.get_text(strip=True)) for a in tag.find_all("a")):
                continue

            cleaned_text = self._clean_text(tag.get_text(" ", strip=True))

            # Skip if cleaned_text is None or numeric content
            if cleaned_text and self._is_numeric_heading(cleaned_text):
                continue

            # Skip if it's empty or already added
            if cleaned_text and cleaned_text not in contents:
                contents.add(cleaned_text)
                extracted_list.append(cleaned_text)

        return extracted_list



    def extract_filters(self):
        """Extract and return all category filters based on headings and content."""
        headings = self.extract_headings()
        contents = self.extract_contents()
        
        if not headings and not contents:
            return ["all"]

        matched_filters = set()  # Use set to avoid duplicates
        all_matched = False 

        # First pass: Exact word match in headings (case-insensitive)
        for category, filter_words in self.filters.items():
            for heading in headings:
                for filter_word in filter_words:
                    if re.search(r'\b' + re.escape(filter_word) + r'\b', heading, re.IGNORECASE):
                        matched_filters.add(f"{category}-head")
                        all_matched = True


        # Second pass: Check if any filter word exists in full combined text (case-insensitive)
        full_text = " ".join(contents)
        for category, filter_words in self.filters.items():
            for filter_word in filter_words:
                if re.search(r'\b' + re.escape(filter_word) + r'\b', full_text, re.IGNORECASE):
                    # add all filters if any filter word is found
                    matched_filters.add(f"{category}-cont")
                    all_matched = True
                
        if all_matched:
            matched_filters.add("all")

        return list(matched_filters) if matched_filters else ["all"]


# # ✅ Example Usage:
# if __name__ == "__main__":
#     url = "https://www.iitkgp.ac.in/"
#     extractor = TextExtractor(url)
#     print("\n Headings:", extractor.extract_headings())
#     print("\n Contents:", extractor.extract_contents())
#     print("\n Filter:", extractor.extract_filters())
