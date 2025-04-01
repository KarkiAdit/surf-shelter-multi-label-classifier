from ..utils import TextSimilarityAnalyzer, HTMLParser
from transformers import pipeline
import numpy as np
import matplotlib.pyplot as plt
import language_tool_python
import pandas as pd
import os


class ClickbaitFeatureExtractor:
    """
    Analyzes web content to compute various feature scores indicative of clickbait characteristics.

    This class evaluates a given URL and its associated HTML content to extract and quantify features
    such as:
    - Similarity between the URL and HTML content.
    - Presence of fear-mongering language.
    - Frequency of grammatical errors.
    - Alignment with known clickbait patterns.

    Attributes:
        url (str): The URL of the web content to be analyzed.
        html_parser (HTMLParser): Parses the provided HTML content.
        similarity_analyzer (TextSimilarityAnalyzer): Assesses textual similarities.
        fear_mongering_detector (pipeline): Detects fear-mongering language using a pre-trained model.
        title (str): Extracted title from the HTML content.
        headings (list of str): Extracted headings from the HTML content.
        meta_tags (list of str): Extracted meta tag content from the HTML.
        cleaned_sentences (list of str): Cleaned and tokenized sentences from the HTML content.
        no_of_grammatical_errors (int): Count of grammatical errors detected in the content.

    Methods:
        extract_features(): Computes and returns a list of feature scores related to clickbait tendencies.
    """

    def __init__(self, url, html_content):
        self.url = url
        self.html_parser = HTMLParser(html_content)
        self.similarity_analyzer = TextSimilarityAnalyzer()
        self.fear_mongering_detector = pipeline(
            "text-classification", model="Falconsai/fear_mongering_detection"
        )
        # Initialize values (parse HTML)
        self._initialize_values()

    def _initialize_values(self):
        """
        Initializes key attributes by extracting and processing relevant content from the parsed HTML.
        """
        # Extract the title, defaulting to "N/A" if not found
        self.title = self.html_parser.get_title().get("title", "N/A")
        # Extract and flatten all headings (H1-H6) into a single list
        headings_dict = self.html_parser.get_headings()
        self.headings = sum(headings_dict.values(), [])
        # Extract and flatten meta tag content
        meta_tags_dict = self.html_parser.get_meta_tags()
        self.meta_tags = sum(meta_tags_dict.values(), [])
        # Extract cleaned text and get tokenized sentences
        self.cleaned_sentences = self.html_parser.get_clean_text().get("sentences", [])
        # Placeholder for the number of grammatical errors (to be calculated later)
        self.no_of_grammatical_errors = 0

    def _compute_url_html_similarity_score(self):
        """
        Computes the similarity score between the URL and extracted HTML content (title + headings).

        :return: float, similarity score between URL and content (0.0 to 1.0).
        """
        content_list = [self.title] + self.headings if self.headings else [self.title]
        url_html_analysis_info = self.similarity_analyzer.url_matching_content(
            self.url, content_list
        )
        similarity_score = url_html_analysis_info[1]
        print(
            f"The url and headings are contextually similar? {url_html_analysis_info[0]}"
        )
        return similarity_score

    def _compute_fear_mongering_score(self, plot=False):
        """
        Computes an average fear-mongering confidence score for all headings and optionally plots the scores.

        :param plot: bool, whether to plot the fear-mongering scores
        :return: float, average fear-mongering confidence score (0.0 to 1.0)
        """
        if not self.headings:
            return 0.0  # No headings, no fear-mongering
        predictions = self.fear_mongering_detector(self.headings)
        scores = [
            pred["score"] for pred in predictions if pred["label"] == "Fear_Mongering"
        ]
        avg_score = np.mean(scores) if scores else 0.0
        print(
            f"Fear Mongering Score (0 to 1, where 1 indicates higher contextual fear mongering): {avg_score}"
        )
        if plot:
            self.plot_label_inputs_scores(
                x_label="Fear Mongering Confidence Score",
                y_label="Headings",
                title="Fear Mongering Score by Headings",
                inputs=self.headings,
                scores=scores,
                color="red",
            )
        return avg_score

    def _compute_grammatical_errors_score(self):
        """
        Computes a grammatical errors score based on the cleaned text stored in the class.

        :return: A score representing the average number of grammar mistakes per sentence (0.0 to 1.0).
        """
        if not self.cleaned_sentences:
            return 0.0  # No sentences, no grammar mistakes
        tool = language_tool_python.LanguageTool("en-US")  # English language model
        # Count total grammatical errors across all sentences
        self.no_of_grammatical_errors = sum(
            len(tool.check(sentence)) for sentence in self.cleaned_sentences
        )
        # Close the tool after use
        tool.close()
        # Return average error per sentence
        return (
            self.no_of_grammatical_errors / len(self.cleaned_sentences)
            if self.cleaned_sentences
            else 0.0
        )

    def compute_additional_properties_score(
        self, csv_file_name="clickbait_data.csv", similarity_threshold=0.75
    ):
        """
        Computes a score predicting whether content is clickbait based on similarity to headlines
        from a clickbait dataset. The function studies properties like "Shock and Outrage",
        "Headline That Tells You How to Feel", and "Encourage Social Sharing".

        Uses Sentence-BERT (SBERT) to compare content against dataset headlines and classify
        content as clickbait based on a similarity threshold. The dataset has 32,000 headlines,
        half labeled clickbait (1) and half non-clickbait (0).

        :param csv_file_path: str, path to CSV with 'headings' and 'labels' (0 for non-clickbait, 1 for clickbait).
        :param similarity_threshold: float, the threshold above which content is considered clickbait (default: 0.75).

        :return: float, clickbait prediction score (0.0 to 1.0), where 1.0 means all content is clickbait.

        Dataset Info:
        - 32,000 rows, with equal distribution of clickbait and non-clickbait headlines.
        - Headlines from sources like 'New York Times', 'BuzzFeed', 'ViralNova', etc.
        - Common patterns include "Shock and Outrage", "Headline That Tells You How to Feel", and "Encourage Social Sharing".
        """
        # Construct content list
        content_list = (
            [self.title] + self.meta_tags + self.headings
            if self.headings
            else [self.title]
        )
        # Load the clickbait dataset
        clickbait_df = self.get_clickbait_data(csv_file_name)
        headings = clickbait_df["headline"].tolist()
        labels = clickbait_df["clickbait"].tolist()
        clickbait_count = 0
        total_content = len(content_list)
        for content in content_list:
            content_embedding = self.similarity_analyzer.model.encode(
                [content]
            )  # Encoding the content
            heading_embeddings = self.similarity_analyzer.model.encode(
                headings
            )  # Encoding all headings
            # Calculate the similarity between the content and each heading in the dataset
            similarities = [
                self.similarity_analyzer.calculate_similarity(
                    content_embedding, heading_embedding
                )
                for heading_embedding in heading_embeddings
            ]
            # Ensure we are working with a proper array/list for comparisons
            similarities = np.array(similarities)
            # Find the maximum similarity score and map it to a clickbait label
            max_similarity = np.max(similarities)
            max_similarity_idx = np.argmax(similarities)
            # Predict clickbait based on the similarity threshold
            if (
                max_similarity >= similarity_threshold
                and labels[max_similarity_idx] == 1
            ):
                clickbait_count += 1
        # Calculate the clickbait prediction score
        clickbait_prediction_score = (
            clickbait_count / total_content if total_content > 0 else 0.0
        )
        return clickbait_prediction_score

    def plot_label_inputs_scores(
        self, x_label, y_label, title, inputs, scores, color="blue"
    ):
        """
        Plots a horizontal bar chart for given inputs and their associated scores.

        :param x_label: str, label for the x-axis
        :param y_label: str, label for the y-axis
        :param title: str, title of the plot
        :param inputs: list of str, names of inputs (e.g., headings, URLs)
        :param scores: list of float, scores associated with each input
        :param color: str, color of the bars (default: 'blue')
        """
        if not inputs or not scores:
            print("No data to plot.")
            return
        inputs = [
            inp[:50] + "..." if len(inp) > 50 else inp for inp in inputs
        ]  # Truncate long inputs
        plt.figure(figsize=(10, 5))
        plt.barh(inputs, scores, color=color, alpha=0.7)
        plt.xlabel(x_label)
        plt.ylabel(y_label)
        plt.title(title)
        plt.gca().invert_yaxis()  # Invert y-axis for readability
        plt.show()

    def get_clickbait_data(self, filename):
        """
        Retrieves the clickbait dataset from a specified CSV file located in the 'data' directory.

        The method calculates the absolute path to the CSV file by determining the directory of the current script
        and resolving the file's relative location based on the provided filename. It then reads the CSV file into a
        pandas DataFrame.

        :param filename: str, the name of the CSV file containing the clickbait dataset (default is "clickbait_data.csv").
                        The file should be located in the 'data' directory, which is at the root of the project.
        :return: pd.DataFrame, a DataFrame containing the clickbait dataset with two columns: 'headings' and 'labels'.
                'headings' are the content headlines, and 'labels' are binary values indicating clickbait (1) or non-clickbait (0).
        """
        # Get the directory of the current script
        current_dir = os.path.dirname(os.path.abspath(__file__))
        # Construct the full path to the specified CSV file
        file_path = os.path.join(current_dir, "..", "..", "data", filename)
        file_path = os.path.abspath(file_path)  # Ensure it's an absolute path
        # Read the CSV using the calculated path
        return pd.read_csv(file_path)

    def extract_features(self):
        """
        Extracts various feature scores from the content and returns them as a list.

        The features include:
        1. URL-HTML Similarity Score
        2. Fear-Mongering Score
        3. Grammatical Errors Score
        4. Additional Properties Score

        :return: list
            A list containing the computed feature scores in the above order.
        """
        return [
            self._compute_url_html_similarity_score(),
            self._compute_fear_mongering_score(),
            self._compute_grammatical_errors_score(),
            self.compute_additional_properties_score(),
        ]


# The test function
# if __name__ == "__main__":
#     # Dummy URL and HTML content with three headings
#     url = "https://example.com/shocking-news"
#     html_content = """
#     <html>
#         <head><title>Breaking: You Won't Believe What Happened!</title></head>
#         <body>
#             <h1>This New Discovery Will Change Everything!</h1>
#             <h2>Experts Are Warning About an Upcoming Crisis</h2>
#             <h3>Find Out the Hidden Truth Behind These Events</h3>
#         </body>
#     </html>
#     """
#     # Initialize the extractor
#     extractor = ClickbaitFeatureExtractor(url, html_content)
#     clickbait_features = extractor.extract_features()
#     print(f"All features: {clickbait_features}")
