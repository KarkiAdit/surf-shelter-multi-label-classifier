import os
import requests


class SafeBrowsingDataFetcher:
    """
    Fetches and analyzes website safety data using the Google Safe Browsing API.

    This class queries the Google Safe Browsing API to detect various threat types associated with a given URL.
    It then classifies the URL as potentially clickbait/pay fraud or harmful based on aggregated threat counts and predefined thresholds.

    Attributes:
        website_url (str): The URL to be evaluated.
        _threats_info (dict): A dictionary storing the count of detected threat types.
        is_clickbait_content (bool): Indicates if the URL is classified as clickbait content.
        is_payfraud_content (bool): Indicates if the URL is classified as pay fraud content.
        is_harmful_content (bool): Indicates if the URL is classified as harmful content.
        threat_threshold (int): The threshold for classifying a URL based on aggregated threat counts.
    """

    def __init__(self, url):
        """
        Initializes the SafeBrowsingDataFetcher with the provided URL and sets up initial attributes.

        Args:
            url (str): The URL of the website to evaluate for threats.
        """
        self.website_url = url
        self.threat_threshold = 2  # Threshold based on social security research
        self._threats_info = {
            "THREAT_TYPE_UNSPECIFIED": 0,
            "SOCIAL_ENGINEERING": 0,
            "MALWARE": 0,
            "UNWANTED_SOFTWARE": 0,
            "POTENTIALLY_HARMFUL_APPLICATION": 0,
        }
        self.is_clickbait_content = (
            False  # True if Threat type is THREAT_TYPE_UNSPECIFIED within the threshold
        )
        self.is_payfraud_content = (
            False  # True if Threat type is THREAT_TYPE_UNSPECIFIED
        )
        self.is_harmful_content = False  # True if Threat type is SOCIAL_ENGINEERING, MALWARE, UNWANTED_SOFTWARE, POTENTIALLY_HARMFUL_APPLICATION
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
            )  # API key from the environment variables
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
                "threatTypes": [
                    "THREAT_TYPE_UNSPECIFIED",
                    "SOCIAL_ENGINEERING",
                    "MALWARE",
                    "UNWANTED_SOFTWARE",
                    "POTENTIALLY_HARMFUL_APPLICATION"
                ],
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
            - Clickbait and Pay fraud: If THREAT_TYPE_UNSPECIFIED count exceeds threshold.
            - Harmful: If any of MALWARE, UNWANTED_SOFTWARE, POTENTIALLY_HARMFUL_APPLICATION,
              or SOCIAL_ENGINEERING counts exceed threshold.
        """
        if self._threats_info["THREAT_TYPE_UNSPECIFIED"] >= self.threat_threshold:
            self.is_clickbait_content = True
            self.is_payfraud_content = True
        if (
            self._threats_info["MALWARE"] >= self.threat_threshold
            or self._threats_info["UNWANTED_SOFTWARE"] >= self.threat_threshold
            or self._threats_info["POTENTIALLY_HARMFUL_APPLICATION"]
            >= self.threat_threshold
            or self._threats_info["SOCIAL_ENGINEERING"] >= self.threat_threshold
        ):
            self.is_harmful_content = True


# # Test function
# if __name__ == "__main__":
#     url = "http://malicious-domain.com/download/malware.exe"
#     comparator = SafeBrowsingDataFetcher(url)

#     # Access the results of classification
#     print(f"Is Clickbait Content: {comparator.is_clickbait_content}")
#     print(f"Is Payfraud Content: {comparator.is_payfraud_content}")
#     print(f"Is Harmful Content: {comparator.is_harmful_content}")
