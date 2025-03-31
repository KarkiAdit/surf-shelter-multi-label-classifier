import os
import requests


class SafeBrowsingDataFetcher:
    """
    Evaluates a website's safety using the Google Safe Browsing API, classifying it based on detected threats.

    This class checks a given URL for various threat types, including social engineering,
    phishing, suspicious content, malware, and unwanted software. It aggregates the threat counts
    and classifies the website as clickbait, pay fraud, or harmful based on predefined thresholds.

    Attributes:
        website_url (str): The URL of the website to evaluate.
        _threats_info (dict): A dictionary storing the count of each detected threat type.
        is_clickbait_content (bool): Indicates if the website is classified as clickbait content.
        is_payfraud_content (bool): Indicates if the website is classified as pay fraud content.
        is_harmful_content (bool): Indicates if the website is classified as harmful content.
        threat_threshold (int): The threshold for classifying a website based on threat counts.
    """

    def __init__(self, url):
        """
        Initializes the SafeBrowsingDataFetcher with the provided URL and sets up initial attributes.

        Args:
            url (str): The URL of the website to evaluate for threats.
        """
        self.website_url = url
        self._threats_info = {
            "SOCIAL_ENGINEERING": 0,
            "PHISHING": 0,
            "SUSPICIOUS": 0,
            "MALWARE": 0,
            "UNWANTED_SOFTWARE": 0,
        }
        self.is_clickbait_content = False  # True if Threat type is SOCIAL_ENGINEERING
        self.is_payfraud_content = (
            False  # True if Threat type is PHISHING and SUSPICIOUS
        )
        self.is_harmful_content = (
            False  # True if Threat type is MALWARE and UNWANTED_SOFTWARE
        )
        self.threat_threshold = 3  # Threshold based on social security research
        self._evaluate_url_using_safe_browsing_api()

    def _evaluate_url_using_safe_browsing_api(self):
        """
        Sends a request to the Google Safe Browsing API to check for threats associated with the URL.

        This method updates the internal threat information and calls `_update_threat_findings`
        to classify the website based on the API response.

        Raises:
            requests.exceptions.RequestException: If an error occurs during the API request.
        """
        params = {
            "key": os.getenv(
                "GOOGLE_SAFE_BROWSING_KEY"
            )  # Your API key should be set in environment variables
        }
        payload = {
            "client": {
                "clientId": "surf-shelter-data-server-engine",
                "clientVersion": "0.0",
            },
            "threatInfo": {
                "platformTypes": ["ANY_PLATFORM"],
                "threatEntryTypes": ["URL"],
                "threatEntries": [{"url": self.website_url}],
            },
        }

        try:
            response = requests.post(
                "https://safebrowsing.googleapis.com/v4/threatMatches:find",
                params=params,
                json=payload,
            )
            response.raise_for_status()
            data = response.json()

            if "matches" in data:
                # Aggregate threat counts
                for match in data["matches"]:
                    threat_type = match["threatType"]
                    if threat_type in self._threats_info:
                        self._threats_info[threat_type] += 1
                # Based on the thresholds, update the attributes
                self._update_threat_findings()
            else:
                print("The URL is safe.")
        except requests.exceptions.RequestException as e:
            print(f"Error checking URL with Google Safe Browsing API: {e}")

    def _update_threat_findings(self):
        """
        Classifies the website based on aggregated threat counts and predefined thresholds.

        This method updates the `is_clickbait_content`, `is_payfraud_content`, and
        `is_harmful_content` attributes based on the counts of detected threats.

        Classification rules:
            - Clickbait: SOCIAL_ENGINEERING count exceeds threshold.
            - Pay fraud: PHISHING and SUSPICIOUS counts both exceed threshold.
            - Harmful: MALWARE and UNWANTED_SOFTWARE counts both exceed threshold.
        """
        if self._threats_info["SOCIAL_ENGINEERING"] >= self.threat_threshold:
            self.is_clickbait_content = True
        if (
            self._threats_info["PHISHING"] >= self.threat_threshold
            and self._threats_info["SUSPICIOUS"] >= self.threat_threshold
        ):
            self.is_payfraud_content = True
        if (
            self._threats_info["MALWARE"] >= self.threat_threshold
            and self._threats_info["UNWANTED_SOFTWARE"] >= self.threat_threshold
        ):
            self.is_harmful_content = True


# # Test function
# if __name__ == "main":
#     url = "http://example.com"
#     comparator = SafeBrowsingDataFetcher(url)

#     # Access the results of classification
#     print(f"Is Clickbait Content: {comparator.is_clickbait_content}")
#     print(f"Is Payfraud Content: {comparator.is_payfraud_content}")
#     print(f"Is Harmful Content: {comparator.is_harmful_content}")
