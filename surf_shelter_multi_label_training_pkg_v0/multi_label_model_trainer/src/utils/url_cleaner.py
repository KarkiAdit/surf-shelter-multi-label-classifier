import re
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
from url_normalize import url_normalize

class URLCleaner:
    """
    A utility class for normalizing and analyzing URLs.

    This class provides methods to:
    - Normalize URLs to a standard format.
    - Extract components from a URL.
    - Remove default ports from URLs.
    - Sort query parameters alphabetically.
    - Remove duplicate slashes from the path.
    - Remove fragments from URLs.
    """

    @staticmethod
    def normalize_url(raw_url):
        """
        Normalize the given URL to a standard format.

        Args:
            raw_url (str): The raw URL to normalize.

        Returns:
            str: The normalized URL.
        """
        return url_normalize(raw_url)

    @staticmethod
    def extract_url_components(url):
        """
        Extract relevant components from the URL.

        Args:
            url (str): The URL to analyze.

        Returns:
            dict: A dictionary containing the scheme, netloc, path, params, query, and fragment of the URL.
        """
        parsed_url = urlparse(url)
        return {
            'scheme': parsed_url.scheme,
            'netloc': parsed_url.netloc,
            'path': parsed_url.path,
            'params': parsed_url.params,
            'query': parsed_url.query,
            'fragment': parsed_url.fragment
        }

    @staticmethod
    def remove_default_port(url):
        """
        Remove the default port from the URL if present.

        Args:
            url (str): The URL from which to remove the default port.

        Returns:
            str: The URL without the default port.
        """
        parsed_url = urlparse(url)
        netloc = parsed_url.netloc
        if parsed_url.scheme == 'http' and parsed_url.port == 80:
            netloc = netloc.replace(':80', '')
        elif parsed_url.scheme == 'https' and parsed_url.port == 443:
            netloc = netloc.replace(':443', '')
        return urlunparse(parsed_url._replace(netloc=netloc))

    @staticmethod
    def sort_query_parameters(url):
        """
        Sort the query parameters of the URL alphabetically by key.

        Args:
            url (str): The URL whose query parameters need to be sorted.

        Returns:
            str: The URL with sorted query parameters.
        """
        parsed_url = urlparse(url)
        query_params = parse_qsl(parsed_url.query, keep_blank_values=True)
        sorted_query = urlencode(sorted(query_params))
        return urlunparse(parsed_url._replace(query=sorted_query))

    @staticmethod
    def remove_duplicate_slashes(url):
        """
        Remove duplicate slashes from the path of the URL.

        Args:
            url (str): The URL from which to remove duplicate slashes.

        Returns:
            str: The URL with duplicate slashes removed.
        """
        parsed_url = urlparse(url)
        normalized_path = re.sub(r'/{2,}', '/', parsed_url.path)
        return urlunparse(parsed_url._replace(path=normalized_path))

    @staticmethod
    def remove_fragment(url):
        """
        Remove the fragment from the URL.

        Args:
            url (str): The URL from which to remove the fragment.

        Returns:
            str: The URL without the fragment.
        """
        parsed_url = urlparse(url)
        return urlunparse(parsed_url._replace(fragment=''))

# # Test function
# if __name__ == "__main__":
#     raw_url = "HTTP://www.Example.com:80//a/../b/./c%7E2?b=2&a=1#section"
#     cleaner = URLCleaner()
#     normalized_url = cleaner.normalize_url(raw_url)
#     components = cleaner.extract_url_components(normalized_url)
#     url_without_default_port = cleaner.remove_default_port(normalized_url)
#     url_with_sorted_query = cleaner.sort_query_parameters(url_without_default_port)
#     url_without_duplicate_slashes = cleaner.remove_duplicate_slashes(url_with_sorted_query)
#     final_url = cleaner.remove_fragment(url_without_duplicate_slashes)
#     print("Raw URL:", raw_url)
#     print("Normalized URL:", normalized_url)
#     print("URL Components:", components)
#     print("URL without Default Port:", url_without_default_port)
#     print("URL with Sorted Query Parameters:", url_with_sorted_query)
#     print("URL without Duplicate Slashes:", url_without_duplicate_slashes)
#     print("Final URL without Fragment:", final_url)
