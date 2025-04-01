import tldextract

class DomainExtractor:
    def __init__(self, url: str):
        self.url = url

    def get_domain_name(self):
        """Extracts the main domain (e.g., 'example.com')."""
        try:
            extracted = tldextract.extract(self.url)
            return f"{extracted.domain}.{extracted.suffix}" if extracted.suffix else None
        except Exception as e:
            print(f"Error extracting domain name: {e}")
            return None

    def get_subdomain_name(self):
        """Extracts the subdomain (e.g., 'sub.example.com', or 'example.com' if no subdomain)."""
        try:
            extracted = tldextract.extract(self.url)
            subdomain = extracted.subdomain.replace('www.', '') if extracted.subdomain else ""
            return f"{subdomain}.{extracted.domain}.{extracted.suffix}" if extracted.suffix else None
        except Exception as e:
            print(f"Error extracting subdomain name: {e}")
            return None

# # Example Usage:
# url = "https://www.erp.iitkgp.ac.in"
# extractor = DomainExtractor(url)

# domain_name = extractor.get_domain_name()
# sub_domain_name = extractor.get_subdomain_name()

# print(f"Domain: {domain_name}")  # Expected: iitkgp.ac.in
# print(f"Subdomain: {sub_domain_name}")  # Expected: erp.iitkgp.ac.in
