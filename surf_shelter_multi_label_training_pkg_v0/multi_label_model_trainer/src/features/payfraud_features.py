# surf_shelter_multi_label_training_pkg_v0/multi_label_model_trainer/src/features/payfraud_features.py

import os
import re
import numpy as np
import base64
import requests
import json
from urllib.parse import urlparse

try:
    import Levenshtein
except ImportError:
    print("Warning: python-Levenshtein library not found. Typosquatting score calculation might be limited.")
    Levenshtein = None

try:
    from ..utils import HTMLParser, TextSimilarityAnalyzer, URLCleaner
except ImportError:
    print("Warning: Could not import helper classes from ..utils. Ensure they exist and structure is correct.")
    # Define dummy classes if import fails
    class HTMLParser:
        def __init__(self, content): pass
        def get_scripts(self): return {"embedded_scripts": [], "external_scripts": []}
        def get_forms(self): return []
        def get_form_fields(self, form): return {}
    class TextSimilarityAnalyzer:
        def __init__(self): pass
        def compute_similarity_score(self, text, text_list): return 0.0
        def model(self):
            class DummyModel:
                def encode(self, texts): return np.zeros((len(texts), 10))
            return DummyModel()
    class URLCleaner:
        def clean_url(self, url): return url
        def normalize_url(self, url): return url
        def extract_url_components(self, url): return urlparse(url)

# --- Constants ---

TRUSTED_BRAND_DOMAINS = [
    "paypal.com", "amazon.com", "google.com", "apple.com", "microsoft.com",
    "facebook.com", "instagram.com", "bankofamerica.com", "chase.com",
    "wellsfargo.com", "netflix.com", "ebay.com", "shopify.com"
]
COMMON_TLDS = {'.com', '.org', '.net', '.gov', '.edu', '.io', '.co', '.info'}
STANDARD_PAYMENT_FIELDS = {
    'card', 'ccnum', 'cc-number', 'cardnumber', 'creditcard', 'card_number',
    'exp', 'expiry', 'exp-month', 'exp-year', 'cc-exp', 'expiration',
    'cvc', 'cvv', 'csc', 'cc-csc', 'securitycode', 'card_code',
    'name', 'cardholder', 'card-holder', 'cc-name',
    'address', 'street', 'city', 'state', 'zip', 'postal', 'country'
}
SUSPICIOUS_PAYMENT_FIELDS = {
    'ssn', 'social', 'sin', 'taxid', 'pin', 'atmpin', 'dob', 'birthdate',
    'maiden', 'mother', 'account', 'bankaccount', 'iban', 'routing', 'sortcode',
    'pass', 'pwd', 'secret'
}

# --- Data File Paths & API Config ---
BASE_DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "data"))
MALWARE_JS_SAMPLES_DIR = os.path.join(BASE_DATA_DIR, "malware_js_samples")

PHISHTANK_API_URL = "http://checkurl.phishtank.com/checkurl/"
PHISHTANK_API_KEY = os.environ.get('PHISHTANK_API_KEY', None)
PHISHTANK_API_TIMEOUT = 10


# --- Feature Extractor Class ---

class PayFraudFeatureExtractor:
    """
    Analyzes web content (URL, HTML, JS) to compute feature scores
    indicative of payment fraud characteristics. Uses PhishTank API for checks.
    """

    

    def __init__(self, url, html_content, javascript_content_list=None, css_content=None, ssl_info=None, redirect_history=None):
        """
        Initializes the PayFraudFeatureExtractor.
        """
        self.url = url
        self.html_content = html_content if html_content else ""
        self.javascript_content_list = javascript_content_list if javascript_content_list is not None else []
        self.css_content = css_content
        self.ssl_info = ssl_info
        self.redirect_history = redirect_history

        self.html_parser = HTMLParser(self.html_content)
        self.similarity_analyzer = TextSimilarityAnalyzer()
        self.url_cleaner = URLCleaner()

        self._load_malware_samples()
        self._initialize_values()

    def _initialize_values(self):
        """Initializes key attributes by parsing URL and HTML."""
        try:
            self.cleaned_url = self.url_cleaner.clean_url(self.url)
            parsed_url = urlparse(self.cleaned_url)
            self.hostname = parsed_url.hostname if parsed_url.hostname else ""
            parts = self.hostname.split('.')
            self.tld = f".{parts[-1]}" if len(parts) > 1 else ""
            self.sld = parts[-2] if len(parts) > 1 else self.hostname
            self.domain_name = f"{self.sld}{self.tld}" if self.sld and self.tld else self.hostname

        except Exception as e:
            print(f"Warning: Error parsing URL '{self.url}': {e}")
            self.cleaned_url = self.url
            self.hostname = ""
            self.tld = ""
            self.sld = ""
            self.domain_name = ""

        if not self.javascript_content_list and self.html_content:
             scripts_data = self.html_parser.get_scripts()
             self.javascript_content_list = scripts_data.get("embedded_scripts", [])

        self.forms = self.html_parser.get_forms() if self.html_content else []


    def _load_malware_samples(self):
        """Loads known malicious JavaScript samples from the specified directory."""
        if hasattr(self, 'malware_samples'):
             return
        self.malware_samples = []
        if not os.path.isdir(MALWARE_JS_SAMPLES_DIR):
            print(f"Info: Malware JS samples directory not found: {MALWARE_JS_SAMPLES_DIR}.")
            return

        print(f"Loading malware samples from: {MALWARE_JS_SAMPLES_DIR}")
        loaded_count = 0
        try:
            for filename in os.listdir(MALWARE_JS_SAMPLES_DIR):
                if filename.lower().endswith((".js", ".txt")):
                    filepath = os.path.join(MALWARE_JS_SAMPLES_DIR, filename)
                    try:
                        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                            content = f.read()
                            if content.strip():
                                self.malware_samples.append(content)
                                loaded_count += 1
                    except Exception as e:
                        print(f"Warning: Could not read malware sample '{filename}': {e}")
            if loaded_count == 0:
                 print(f"Warning: Malware samples directory exists but no valid samples loaded.")
            else:
                 print(f"Loaded {loaded_count} malware samples.")
        except OSError as e:
             print(f"Error accessing malware samples directory {MALWARE_JS_SAMPLES_DIR}: {e}")


    def _check_phishtank_api(self):
        """Checks the current URL against the PhishTank API."""
        if not self.cleaned_url:
            return False, "URL not available"

        try:
            encoded_url = base64.urlsafe_b64encode(self.cleaned_url.encode()).decode()
        except Exception as e:
            print(f"PhishTank Error: Could not base64 encode URL '{self.cleaned_url}': {e}")
            return False, "URL Encoding Error"

        payload = {
            'url': encoded_url,
            'format': 'json'
        }
        if PHISHTANK_API_KEY:
            payload['app_key'] = PHISHTANK_API_KEY
        else:
             pass # Optionally print info about missing key

        try:
            response = requests.post(PHISHTANK_API_URL, data=payload, timeout=PHISHTANK_API_TIMEOUT)
            response.raise_for_status()
            results = response.json()

            if results.get('results', {}).get('in_database') and \
               results.get('results', {}).get('verified') and \
               results.get('results', {}).get('valid'):
                print(f"PhishTank API: MATCH FOUND for URL: {self.cleaned_url}")
                return True, "Match Found"
            else:
                return False, "No Match"

        except requests.exceptions.Timeout:
            print(f"PhishTank API Error: Request timed out for URL {self.cleaned_url}")
            return False, "Timeout"
        except requests.exceptions.RequestException as e:
            print(f"PhishTank API Error: Request failed for URL {self.cleaned_url}: {e}")
            return False, f"Request Error: {e}"
        except json.JSONDecodeError as e:
            print(f"PhishTank API Error: Could not decode JSON response for URL {self.cleaned_url}: {e}")
            return False, "JSON Decode Error"
        except Exception as e:
             print(f"PhishTank API Error: An unexpected error occurred: {e}")
             return False, f"Unexpected Error: {e}"


    def _compute_typosquatting_score(self):
        """
        Computes a score based on domain characteristics potentially indicating typosquatting.
        """
        if not self.hostname: return 0.0
        if not Levenshtein: min_distance = float('inf')
        else: min_distance = float('inf')
        score = 0.0
        normalized_similarity_score = 0.0
        target_sld_tld = self.domain_name
        if Levenshtein and target_sld_tld and TRUSTED_BRAND_DOMAINS:
            try:
                for trusted_domain in TRUSTED_BRAND_DOMAINS:
                    distance = Levenshtein.distance(target_sld_tld.lower(), trusted_domain.lower())
                    min_distance = min(min_distance, distance)
                if min_distance <= 2 and len(target_sld_tld) > 5 :
                    normalized_similarity_score = max(0.0, 1.0 - (min_distance / 3.0))
                    score += 0.6 * normalized_similarity_score
            except Exception as e:
                print(f"Warning: Error during Levenshtein comparison: {e}")
                score += 0.1
        if self.tld and self.tld.lower() not in COMMON_TLDS: score += 0.2
        if self.sld and re.search(r"[\d-]", self.sld):
            is_common_pattern = re.search(r"-(login|secure|account|service|support|online)$", self.sld, re.IGNORECASE)
            is_known_brand_pattern = any(brand_sld in self.sld.lower() for brand_sld in ['3m'])
            if not is_common_pattern and not is_known_brand_pattern: score += 0.2
        return min(1.0, score)


    def _compute_ssl_certificate_score(self):
        """
        Computes a score based on SSL certificate anomalies and PhishTank API status.
        """
        score = 0.0
        phishtank_hit, _ = self._check_phishtank_api()
        if phishtank_hit:
             score = 1.0

        ssl_penalty = 0.0
        if self.ssl_info:
            if not self.ssl_info.get('is_present', True):
                ssl_penalty = 1.0
            else:
                 if not self.ssl_info.get('is_valid', True): ssl_penalty += 0.5
                 if self.ssl_info.get('is_self_signed', False): ssl_penalty += 0.8
                 if self.hostname and not self.ssl_info.get('subject_matches_hostname', True): ssl_penalty += 0.4
                 elif not self.hostname and 'subject_matches_hostname' in self.ssl_info and not self.ssl_info['subject_matches_hostname']: ssl_penalty += 0.4
                 if not self.ssl_info.get('validity_period_ok', True): ssl_penalty += 0.2
        elif not phishtank_hit:
             return 0.0

        final_score = max(score, min(1.0, ssl_penalty))
        return final_score


    def _compute_obfuscated_js_score(self):
        """
        Computes a score based on JS obfuscation heuristics and similarity to known malware.
        """
        if not self.javascript_content_list: return 0.0
        combined_js = "\n".join(self.javascript_content_list)
        if not combined_js.strip(): return 0.0
        heuristic_score = 0.0
        if re.search(r'eval\(|String\.fromCharCode|document\.write\(|unescape\(|atob\(|btoa\(', combined_js): heuristic_score += 0.2
        if re.search(r'[a-zA-Z0-9+/=]{80,}', combined_js): heuristic_score += 0.15
        if re.search(r'\bvar\s+[a-zA-Z0-9]{8,}\d+[a-zA-Z0-9]*\b', combined_js) or \
           re.search(r'\b[a-z]{2,}[0-9]+[a-z0-9]*\s*=', combined_js): heuristic_score += 0.15
        heuristic_score = min(1.0, heuristic_score)
        similarity_score = 0.0
        if not hasattr(self, 'malware_samples') or not self.malware_samples: pass
        else:
            try:
                similarity_score = self.similarity_analyzer.compute_similarity_score(combined_js, self.malware_samples)
            except Exception as e:
                print(f"Warning: Error during JS similarity analysis: {e}")
                similarity_score = 0.1
        if not hasattr(self, 'malware_samples') or not self.malware_samples: final_score = heuristic_score
        else: final_score = (0.7 * similarity_score) + (0.3 * heuristic_score)
        return min(1.0, final_score)


    def _compute_aggressive_forms_score(self):
        """
        Computes a score based on suspicious fields in payment forms.
        """
        if not self.forms: return 0.0
        max_form_suspicion_score = 0.0
        for form_element in self.forms:
            if not hasattr(form_element, 'find_all') or not hasattr(form_element, 'get'): continue
            form_fields = self.html_parser.get_form_fields(form_element)
            if not form_fields: continue
            field_identifiers = set()
            for name_or_id, element in form_fields.items():
                 field_identifiers.add(str(name_or_id).lower())
                 input_tag = element
                 if input_tag and hasattr(input_tag, 'get'):
                      if input_tag.get('name'): field_identifiers.add(str(input_tag.get('name')).lower())
                      if input_tag.get('id'): field_identifiers.add(str(input_tag.get('id')).lower())
            field_identifiers.discard('')
            is_payment_form = False
            payment_keywords = {'card', 'payment', 'checkout', 'cc', 'credit', 'secure', 'billing', 'charge', 'purchase', 'order', 'donate'}
            if any(pf in identifier for identifier in field_identifiers for pf in payment_keywords): is_payment_form = True
            form_id = str(form_element.get('id', '')).lower()
            form_name = str(form_element.get('name', '')).lower()
            form_action = str(form_element.get('action', '')).lower()
            if any(kw in attr for attr in [form_id, form_name, form_action] for kw in payment_keywords): is_payment_form = True
            if not is_payment_form: continue
            suspicious_fields_found = field_identifiers.intersection(SUSPICIOUS_PAYMENT_FIELDS)
            standard_fields_found = field_identifiers.intersection(STANDARD_PAYMENT_FIELDS)
            current_form_score = 0.0
            num_suspicious = len(suspicious_fields_found)
            if num_suspicious > 0: current_form_score = 0.5 + min(0.5, 0.2 * (num_suspicious - 1))
            if num_suspicious > 0 and not standard_fields_found and len(field_identifiers) > num_suspicious: current_form_score = min(1.0, current_form_score + 0.1)
            max_form_suspicion_score = max(max_form_suspicion_score, current_form_score)
        return max_form_suspicion_score


    def _compute_redirect_chain_score(self):
        """
        Computes a score based on the length of redirect chains. Relies on `redirect_history` input.
        """
        if not self.redirect_history or len(self.redirect_history) <= 1: return 0.0
        chain_length = len(self.redirect_history)
        max_expected_redirects = 3.0
        score = min(1.0, max(0.0, (chain_length - 1) / max_expected_redirects))
        return score


    def extract_features(self):
        """
        Extracts all pay-fraud related feature scores and returns them as a list.
        """
        typo_score = self._compute_typosquatting_score()
        ssl_phishtank_score = self._compute_ssl_certificate_score()
        js_score = self._compute_obfuscated_js_score()
        form_score = self._compute_aggressive_forms_score()
        redirect_score = self._compute_redirect_chain_score()

        print("-" * 20)
        print(f"Feature Scores Summary (URL: {self.url}):")
        print(f"  1. Typosquatting:      {typo_score:.3f}")
        print(f"  2. SSL/PhishTank API:  {ssl_phishtank_score:.3f}")
        print(f"  3. Obfuscated JS:      {js_score:.3f}")
        print(f"  4. Aggressive Forms:   {form_score:.3f}")
        print(f"  5. Redirect Chain:     {redirect_score:.3f}")
        print("-" * 20)

        return [
            typo_score,
            ssl_phishtank_score,
            js_score,
            form_score,
            redirect_score,
        ]

# --- Test Function ---
# if __name__ == "__main__":
#     print("--- Running PayFraudFeatureExtractor Test ---")

#     print("\n--- Test Case 1: Known Phish URL (using API) ---")
#     test_url_1 = "http://phishtank.com/" # Replace with actual known phish for better testing
#     test_html_1 = "<html><body><form><input name='username'></form></body></html>"
#     test_js_list_1 = ["eval('console.log(\"phishy\")');"]
#     test_ssl_info_1 = None
#     test_redirects_1 = []
#     extractor1 = PayFraudFeatureExtractor(test_url_1, test_html_1, test_js_list_1, ssl_info=test_ssl_info_1, redirect_history=test_redirects_1)
#     features1 = extractor1.extract_features()
#     print(f"Test Case 1 Features: {features1}")

#     print("\n--- Test Case 2: Likely Benign ---")
#     test_url_2 = "https://google.com/"
#     test_html_2 = """<html><head><title>Google</title></head></html>"""
#     test_js_list_2 = ["console.log('Google');"]
#     test_ssl_info_2 = { 'is_present': True, 'is_valid': True, 'is_self_signed': False,
#         'subject_matches_hostname': True, 'validity_period_ok': True }
#     test_redirects_2 = [test_url_2]
#     extractor2 = PayFraudFeatureExtractor(test_url_2, test_html_2, test_js_list_2, ssl_info=test_ssl_info_2, redirect_history=test_redirects_2)
#     features2 = extractor2.extract_features()
#     print(f"Test Case 2 Features: {features2}")

#     print("\n--- Test Case 3: Suspicious SSL / Typosquatting ---")
#     test_url_3 = "https://paypa1.com/login"
#     test_html_3 = "<html><body>Login Form</body></html>"
#     test_js_list_3 = []
#     test_ssl_info_3 = { 'is_present': True, 'is_valid': False, 'is_self_signed': False,
#         'subject_matches_hostname': True, 'validity_period_ok': True }
#     test_redirects_3 = []
#     extractor3 = PayFraudFeatureExtractor(test_url_3, test_html_3, test_js_list_3, ssl_info=test_ssl_info_3, redirect_history=test_redirects_3)
#     features3 = extractor3.extract_features()
#     print(f"Test Case 3 Features: {features3}")

#     print("\n--- PayFraudFeatureExtractor Test Complete ---")
