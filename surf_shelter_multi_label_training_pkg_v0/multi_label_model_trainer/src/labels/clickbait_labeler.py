import numpy as np
import pandas as pd
from ..features import ClickbaitFeatureExtractor


class ClickbaitLabeler:
    """
    Creates a labeled dataset for clickbait detection using multiple
    `ClickbaitFeatureExtractor` variations with soft voting.

    Attributes:
        kaggle_dataset_similarity_thresholds (list of float): Similarity thresholds
            for the Kaggle dataset to determine clickbait characteristics.
        voting_weights (list of float): Weights assigned to each feature extractor
            variation during the voting process. Defaults to equal weights if not provided.
        classifier_threshold (float): Threshold for classifying content as clickbait.
            Defaults to 0.5.
    """

    def __init__(
        self,
        kaggle_dataset_similarity_thresholds,
        voting_weights=None,
        classifier_threshold=0.5,
    ):
        """
        Initializes the ClickbaitLabeler with specified similarity thresholds,
        voting weights, and classification threshold.

        Args:
            kaggle_dataset_similarity_thresholds (list of float): Similarity thresholds
                for the Kaggle dataset to determine clickbait characteristics.
            voting_weights (list of float, optional): Weights assigned to each feature
                extractor variation during the voting process. Defaults to equal weights
                if not provided.
            classifier_threshold (float, optional): Threshold for classifying content
                as clickbait. Defaults to 0.5.
        """

        self.kaggle_dataset_similarity_thresholds = kaggle_dataset_similarity_thresholds
        self.voting_weights = (
            voting_weights
            if voting_weights
            else [1.0] * len(kaggle_dataset_similarity_thresholds)
        )
        self.classifier_threshold = classifier_threshold

    def _calculate_base_features(self, url, html_content):
        """
        Calculates the base features that do not depend on the similarity threshold.

        Args:
            url (str): The URL of the web content.
            html_content (str): The HTML content of the web page.

        Returns:
            list of float: The first three extracted feature scores.
        """
        extractor = ClickbaitFeatureExtractor(url, html_content)
        base_features = extractor.extract_features()
        return base_features[:3]  # Return the first 3 features

    def _calculate_additional_properties_score(
        self, url, html_content, kaggle_dataset_similarity_threshold
    ):
        """
        Calculates the additional properties score using a specific similarity threshold.

        Args:
            url (str): The URL of the web content.
            html_content (str): The HTML content of the web page.
            kaggle_dataset_similarity_threshold (float): The similarity threshold
                for determining clickbait characteristics.

        Returns:
            float: The additional properties score.
        """
        extractor = ClickbaitFeatureExtractor(url, html_content)
        return extractor.compute_additional_properties_score(
            similarity_threshold=kaggle_dataset_similarity_threshold
        )

    def label_urls_simplified(self, urls, html_contents):
        """
        Labels URLs with a simplified dataset structure: url, f1, f2, f3, f4, label.

        Args:
            urls (list of str): List of URLs to be labeled.
            html_contents (list of str): Corresponding list of HTML contents for each URL.

        Returns:
            pandas.DataFrame: DataFrame containing the labeled data with columns:
                'url', 'url_html_similarity_score', 'fear_mongering_score',
                'grammatical_errors_score', 'additional_properties_score', 'label'.

        Raises:
            ValueError: If the lengths of `urls` and `html_contents` do not match.
        """
        if len(urls) != len(html_contents):
            raise ValueError("URLs and HTML content lists must have the same length.")
        results = []
        for url, html_content in zip(urls, html_contents):
            base_features = self._calculate_base_features(
                url, html_content
            )  # Calculate f1, f2, f3 once
            for similarity_threshold in self.kaggle_dataset_similarity_thresholds:
                additional_properties_score = (
                    self._calculate_additional_properties_score(
                        url, html_content, similarity_threshold
                    )
                )  # Calculate f4 for each threshold
                features = base_features + [
                    additional_properties_score
                ]  # Combine f1, f2, f3, f4
                # Calculate the label based on the average of all features
                clickbait_score = np.mean(features)
                label = 1 if clickbait_score >= self.classifier_threshold else 0
                result = {
                    "url": url,
                    "url_html_similarity_score": features[0],
                    "fear_mongering_score": features[1],
                    "grammatical_errors_score": features[2],
                    "additional_properties_score": features[3],
                    "label": label,
                }
                results.append(result)
        return pd.DataFrame(results)

    def label_urls_in_batches_simplified(self, urls, html_contents, batch_size=100):
        """
        Labels URLs in batches using the simplified dataset structure.

        Args:
            urls (list of str): List of URLs to be labeled.
            html_contents (list of str): Corresponding list of HTML contents for each URL.
            batch_size (int, optional): Number of URLs to process in each batch. Defaults to 100.

        Returns:
            pandas.DataFrame: DataFrame containing the labeled data for all batches.

        Raises:
            ValueError: If the lengths of `urls` and `html_contents` do not match.
        """
        if len(urls) != len(html_contents):
            raise ValueError("URLs and HTML content lists must have the same length.")
        results = []
        for i in range(0, len(urls), batch_size):
            batch_urls = urls[i : i + batch_size]
            batch_html_contents = html_contents[i : i + batch_size]
            batch_results = self.label_urls_simplified(batch_urls, batch_html_contents)
            results.append(batch_results)
        return pd.concat(results, ignore_index=True)


# # Example Usage:
# if __name__ == "__main__":
#     # Example URLs and HTML content (replace with your actual data)
#     urls = ["https://example.com/clickbait", "https://example.com/normal"]
#     html_contents = [
#         "<html><head><title>You Won't Believe This!</title></head><body><h1>Shocking Discovery!</h1></body></html>",
#         "<html><head><title>A Normal Article</title></head><body><p>This is a normal article.</p></body></html>",
#     ]
#     # Create variations of feature extractors (customize as needed)
#     kaggle_thresholds = [0.5, 0.75]  # different thresholds
#     # Initialize the labeler
#     labeler = ClickbaitLabeler(
#         kaggle_thresholds, voting_weights=[1, 1], classifier_threshold=0.5
#     )
#     # Label the URLs
#     simplified_labeled_data = labeler.label_urls_simplified(urls, html_contents)
#     simplified_labeled_data.to_csv("labeled_clickbait_data.csv", index=False)
#     # Print or save the labeled data
#     print(simplified_labeled_data)
#     # Example batch processing:
#     num_urls = 3
#     urls = [f"https://example.com/{i}" for i in range(num_urls)]
#     html_contents = ["<html><body><p>Article {i}</p></body></html>" for i in range(num_urls)]
#     simplified_labeled_data_batches = labeler.label_urls_in_batches_simplified(urls, html_contents, batch_size=2)
#     print(simplified_labeled_data_batches)
#     # Save the dataset
#     simplified_labeled_data_batches.to_csv("labeled_clickbait_data_batches.csv", index=False)
