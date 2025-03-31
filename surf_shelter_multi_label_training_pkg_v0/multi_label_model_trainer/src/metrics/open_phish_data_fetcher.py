import requests
import logging
from ..utils import URLCleaner

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class OpenPhishDataFetcher:
    def __init__(self, feed_url='https://www.openphish.com/feed.txt'):
        """
        Initializes the OpenPhishDataFetcher with the specified feed URL.
        """
        self.feed_url = feed_url
        self.url_cleaner = URLCleaner()
        self.phishing_urls = self._fetch_urls()

    def _fetch_urls(self):
        """
        Fetches the phishing URLs from the OpenPhish feed.

        :return: set of cleaned phishing URLs.
        """
        try:
            response = requests.get(self.feed_url)
            response.raise_for_status()
            urls = {self.url_cleaner.clean_url(url.strip()) for url in response.text.splitlines() if url.strip()}
            if not urls:
                logger.warning("No phishing URLs found in the feed.")
            return urls
        except requests.RequestException as e:
            logger.error(f"Error fetching data from OpenPhish: {e}")
            return set()
        
    def has_phishing_trace(self, url):
        """
        Determines if the provided URL closely matches any of the fetched phishing URLs using fuzzy matching.
        Returns True if a similar phishing URL is found; otherwise, False.

        :param url: str, the URL to check against the phishing URLs.
        :return: bool, True if a phishing trace is found, otherwise False.
        """
        cleaned_url = self.url_cleaner.clean_url(url)
        for phishing_url in self.phishing_urls:
            if self.url_cleaner.compare_urls(cleaned_url, phishing_url, 90): # Set Fuzz ratio to be 90
                self.has_phishing_trace = True
                return True
        return False
        
# # Example usage:
# if __name__ == "__main__":
#     open_phish_data_fetcher = OpenPhishDataFetcher()
#     test_url = "https://vg.tgip.my.id/"
#     print(f"URL: {test_url} is labelled phishing by Open Phish: {open_phish_data_fetcher.has_phishing_trace(test_url)}")
