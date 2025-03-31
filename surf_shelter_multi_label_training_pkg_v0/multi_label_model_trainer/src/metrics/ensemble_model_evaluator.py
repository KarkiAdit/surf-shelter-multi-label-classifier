from .safe_browsing_data_fetcher import SafeBrowsingDataFetcher

class EnsembleModelEvaluator:
    """
    A class to evaluate the performance of multiple ensemble learning models such as XGBoost, 
    Bagged SVM, Random Forest, and others by calculating key performance metrics 
    like F1 score, accuracy, and precision. Additionally, it can plot the performance 
    metrics side by side to compare the models' effectiveness.

    Attributes:
    models (list): A list of trained ensemble learning models (e.g., XGBoost, Random Forest, Bagged SVM).
    
    Methods:
    evaluate_multiple_urls(urls, true_labels, model_names):
        Evaluates the performance of multiple models on a list of URLs and computes key performance metrics.
        It also plots side-by-side bar charts to visualize the results.
        
    plot_metrics(metrics_dict):
        Plots side-by-side bar charts displaying the performance metrics (F1 score, accuracy, precision) 
        for each model.
    """

    def __init__(self, models):
        """
        Initializes the EnsembleModelEvaluator with a list of trained ensemble models.

        Args:
        models (list): A list of trained ensemble learning models (e.g., XGBoost, Random Forest, Bagged SVM).
        """
        pass

    def evaluate_multiple_urls(self, urls, true_labels, model_names):
        """
        Evaluates the performance of multiple models on a list of URLs and computes 
        key performance metrics (F1 score, accuracy, precision). It also visualizes the 
        performance metrics in side-by-side bar charts.

        Args:
        urls (list of str): A list of URLs to be evaluated by the models.
        true_labels (list of int): A list of true labels corresponding to the URLs.
                                   1 for malicious, 0 for safe.
        model_names (list of str): A list of model names to display in the plot titles.

        Returns:
        dict: A dictionary containing the computed performance metrics for each model
              ('F1 Score', 'Accuracy', 'Precision') and model predictions.
        """
        pass
    
    def plot_metrics(self, metrics_dict):
        """
        Plots side-by-side bar charts of the performance metrics (F1 score, accuracy, precision)
        for each model.

        Args:
        metrics_dict (dict): A dictionary containing performance metrics ('F1 Score', 'Accuracy', 'Precision') 
                                for each model.
        """
        pass