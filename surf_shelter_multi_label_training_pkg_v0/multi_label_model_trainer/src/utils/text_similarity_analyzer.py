import pandas as pd
import matplotlib.pyplot as plt
from .batch_data_retriever import fetch_content
import re
from urllib.parse import urlparse
from sentence_transformers import SentenceTransformer
import numpy as np


class TextSimilarityAnalyzer:
    """
    Labels text similarity by comparing two sets of text using SBERT.
    """

    def __init__(self, model_name="distilbert-base-nli-stsb-mean-tokens"):
        """
        Initializes the TextSimilarityLabeller.

        Args:
            model_name (str): The name of the Sentence Transformer model to use.
        """
        self.model = SentenceTransformer(model_name)

    def calculate_similarity(self, embeddings1, embeddings2):
        """
        Calculates the maximum cosine similarity between two sets of embeddings.

        Args:
            embeddings1 (numpy.ndarray): The embeddings of the first set of text.
            embeddings2 (numpy.ndarray): The embeddings of the second set of text.

        Returns:
            float: The maximum cosine similarity.
        """
        similarity_matrix = np.inner(embeddings1, embeddings2)
        return np.max(similarity_matrix) if similarity_matrix.size > 0 else 0

    def url_matching_content(self, url, content_list, similarity_threshold=0.75):
        """
        Labels the URL as matching or not matching the content based on similarity.

        Args:
            url (str): The URL to analyze.
            content_list (list): A list of strings representing the content of the webpage (e.g., headings, title).
            similarity_threshold (float): The threshold for similarity to consider the URL matching.

        Returns:
            tuple: A tuple containing a boolean indicating if the URL matches and the maximum similarity score.
        """
        if not content_list:
            return False, 0.0  # Return default values if content is empty

        url_components = self._extract_url_components(url)
        content_embeddings = self.model.encode(content_list)  # Safe to encode now
        url_component_embeddings = self.model.encode(url_components)

        max_similarity = self.calculate_similarity(
            content_embeddings, url_component_embeddings
        )
        return max_similarity >= similarity_threshold, max_similarity

    def _extract_url_components(self, url):
        """
        Helper function to extract relevant components from the URL.

        Args:
            url (str): The URL to analyze.

        Returns:
            list: A list of URL components (e.g., path, query parameters).
        """
        parsed_url = urlparse(url)
        components = []
        if parsed_url.path:
            components.append(parsed_url.path)
        if parsed_url.query:
            components.append(parsed_url.query)
        if parsed_url.netloc:
            components.append(parsed_url.netloc)

        # Extract words from the path and query parameters
        words_from_path = re.findall(r"\w+", parsed_url.path)
        words_from_query = re.findall(r"\w+", parsed_url.query)
        components.extend(words_from_path)
        components.extend(words_from_query)

        return components


def analyze_similarity(url_content_pairs):
    """Analyze similarity using SBERT models."""
    models = ["all-MiniLM-L6-v2", "distilbert-base-nli-stsb-mean-tokens"]
    results = {"Model": [], "URL": [], "Similarity": [], "Match": []}
    for model_name in models:
        analyzer = TextSimilarityAnalyzer(model_name=model_name)
        for url, content_list in url_content_pairs:
            match, similarity = analyzer.url_matching_content(url, content_list)
            results["Model"].append(model_name)
            results["URL"].append(url)
            results["Similarity"].append(similarity)
            results["Match"].append(match)
    results_df = pd.DataFrame(results)
    # Plot Similarity Scores
    plt.figure(figsize=(14, 12))
    for model in models:
        subset = results_df[results_df["Model"] == model]
        plt.plot(
            subset["URL"][:50], subset["Similarity"][:50], label=f"{model}"
        )  # Limit to 50 URLs for readability
    plt.xlabel("URL (subset of 50)")
    plt.ylabel("Similarity Score")
    plt.title("SBERT Model Similarity Comparison")
    plt.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig("sbert_similarity_comparison.png")  # Save the plot
    plt.show()  # Show the plot in the output
    # Print Summary
    print(results_df.head(20))  # Print first 20 results for preview


def main():
    """Main function to fetch content and analyze similarity."""
    url_content_pairs = fetch_content()
    analyze_similarity(url_content_pairs)


# # Run the main function
# if __name__ == "__main__":
#     main()
