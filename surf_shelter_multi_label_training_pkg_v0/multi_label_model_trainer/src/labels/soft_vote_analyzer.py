import numpy as np
from sklearn.preprocessing import StandardScaler


class SoftVoteAnalyzer:
    """
    Classifies URLs by combining multiple feature scores using soft voting.

    Features are weighted and combined into a final score for each URL. The classification threshold is 0.5.

    Attributes:
        weights (np.array): Weights for each feature.
        threshold (float): Threshold to classify URLs.
        scaler (StandardScaler): Scaler for feature normalization.
    """

    def __init__(self, weights=None, threshold=0.5):
        self.weights = np.array(weights) if weights is not None else None
        self.threshold = threshold
        self.scaler = StandardScaler()

    def fit_scaler(self, features):
        """
        Fits scaler to the feature data and initializes default weights if none provided.

        Args:
            features (np.array): Array of feature scores.
        """
        self.scaler.fit(features)
        if self.weights is None:
            self.weights = np.ones(features.shape[1]) / features.shape[1]

    def label_batch(self, extractors):
        """
        Labels URLs based on features extracted by provided extractor objects.

        Args:
            extractors (list): List of feature extractor objects.

        Returns:
            tuple: (labels, scores)
                labels (list): Binary labels (1 or 0).
                scores (list): Computed soft voting scores.
        """
        features = np.array([ext.extract_features() for ext in extractors])
        norm_features = self.scaler.transform(features)
        scores = norm_features @ self.weights
        labels = (scores >= self.threshold).astype(int)
        return labels.tolist(), scores.tolist()


# # Test method
# if __name__ == "__main__":
#     from ..features import ClickbaitFeatureExtractor
#     # Create a sample of random 10 urls and html_contents
#     urls = [f"https://example.com/{i}" for i in range(5)]
#     html_contents = [f"<html><body><p>Article {i}</p></body></html>" for i in range(5)]
#     extractors = [
#         ClickbaitFeatureExtractor(url, html) for url, html in zip(urls, html_contents)
#     ]
#     analyzer = SoftVoteAnalyzer(
#         weights=[0.2, 0.2, 0.2, 0.4]
#     )  # Assign higher weight to last feature as it is a direct indicative of clickbait
#     analyzer.fit_scaler(np.array([ext.extract_features() for ext in extractors]))
#     labels, scores = analyzer.label_batch(extractors)
#     print(f"Labels: {labels}")
#     print(f"Scores: {scores}")
