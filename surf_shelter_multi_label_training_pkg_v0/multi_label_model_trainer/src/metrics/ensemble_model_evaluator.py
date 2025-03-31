import matplotlib.pyplot as plt
import numpy as np
from sklearn.metrics import f1_score, accuracy_score, precision_score
import warnings

# Attempt to use a visually appealing style for the plot
try:
    plt.style.use('seaborn-v0_8-colorblind')
except OSError:
    # Fallback if seaborn styles are not available
    warnings.warn("Seaborn style not found. Using default Matplotlib style.")
    pass

class EnsembleModelEvaluator:
    """
    A class to evaluate the performance of multiple ensemble learning models such as XGBoost,
    Bagged SVM, Random Forest, and others by calculating key performance metrics
    like F1 score, accuracy, and precision. Additionally, it can plot the performance
    metrics side by side to compare the models' effectiveness.

    Attributes:
    models (list): A list of trained ensemble learning models (e.g., XGBoost, Random Forest, Bagged SVM).
                   Each model object must have a `predict(X)` method.
    """

    def __init__(self, models):
        """
        Initializes the EnsembleModelEvaluator with a list of trained ensemble models.

        Args:
        models (list): A list of trained ensemble learning models (e.g., XGBoost, Random Forest, Bagged SVM).
        """
        if not isinstance(models, list) or not models:
            raise ValueError("Input 'models' must be a non-empty list.")

        for i, model in enumerate(models):
            if not hasattr(model, 'predict') or not callable(model.predict):
                raise TypeError(f"Model at index {i} does not have a callable 'predict' method.")

        self.models = models

    def evaluate_multiple_models(self, X_test, y_true, model_names):
        """
        Evaluates the performance of multiple models on a given dataset (features)
        and computes key performance metrics (F1 score, accuracy, precision).
        It also visualizes the performance metrics in side-by-side bar charts.

        Args:
        X_test (np.ndarray or pd.DataFrame): The feature matrix for the test data.
                                             This should be the output of your
                                             feature extraction process for the URLs.
        y_true (list or np.ndarray): A list or array of true labels corresponding
                                     to the samples in X_test (e.g., 1 for malicious,
                                     0 for safe).
        model_names (list of str): A list of names for the models, corresponding
                                   to the order of models passed during initialization.
                                   Used for labeling the plot.

        Returns:
        dict: A dictionary where keys are model names. Each value is another
              dictionary containing the computed performance metrics
              ('F1 Score', 'Accuracy', 'Precision') and the model's predictions
              ('Predictions').
        """
        if len(model_names) != len(self.models):
            raise ValueError("The number of model names must match the number of models.")
        if len(y_true) != X_test.shape[0]:
             raise ValueError("The number of true labels must match the number of samples in X_test.")

        results = {}

        for model, name in zip(self.models, model_names):
            try:
                predictions = model.predict(X_test)

                # Ensure labels are comparable if necessary (e.g., binary)
                # predictions = (predictions > 0.5).astype(int) # Example if model outputs probabilities

                f1 = f1_score(y_true, predictions, zero_division=0)
                accuracy = accuracy_score(y_true, predictions)
                precision = precision_score(y_true, predictions, zero_division=0)

                results[name] = {
                    'F1 Score': f1,
                    'Accuracy': accuracy,
                    'Precision': precision,
                    'Predictions': predictions
                }
            except Exception as e:
                # Store error information if a model fails
                results[name] = {
                    'F1 Score': np.nan,
                    'Accuracy': np.nan,
                    'Precision': np.nan,
                    'Predictions': None,
                    'Error': str(e)
                }
                warnings.warn(f"Error evaluating model {name}: {e}")


        self.plot_metrics(results)

        return results

    def _add_value_labels(self, ax, spacing=5):
        """Helper function to add labels above bars in a bar chart."""
        for container in ax.containers:
            for rect in container:
                y_value = rect.get_height()
                # Don't plot labels for NaN values
                if np.isnan(y_value):
                    continue

                x_value = rect.get_x() + rect.get_width() / 2

                label = f"{y_value:.3f}"
                va = 'bottom' # Default alignment is bottom (label above bar)
                
                # If value is very high, place label inside bar near top
                if y_value > ax.get_ylim()[1] * 0.95 : 
                    va = 'top'
                    spacing = -spacing # Put inside

                ax.annotate(
                    label,
                    (x_value, y_value),
                    xytext=(0, spacing if va=='bottom' else -spacing), # Offset label
                    textcoords="offset points",
                    ha='center',
                    va=va,
                    fontsize=8,
                    rotation=45,
                    color='black' # Ensure label visibility
                )


    def plot_metrics(self, metrics_dict):
        """
        Plots side-by-side bar charts of the performance metrics (F1 score, accuracy, precision)
        for each model.

        Args:
        metrics_dict (dict): A dictionary containing performance metrics ('F1 Score', 'Accuracy', 'Precision')
                             for each model, as returned by `evaluate_multiple_models`.
        """
        model_names_all = list(metrics_dict.keys())
        
        # Prepare data, keeping track of potential NaNs from errors
        f1_scores = [metrics_dict[name].get('F1 Score', np.nan) for name in model_names_all]
        accuracy_scores = [metrics_dict[name].get('Accuracy', np.nan) for name in model_names_all]
        precision_scores = [metrics_dict[name].get('Precision', np.nan) for name in model_names_all]

        if not model_names_all:
            print("No models found in metrics dictionary.")
            return
        
        # Check if there's any valid data to plot
        valid_data_exists = any(not np.isnan(score) for score in accuracy_scores)
        if not valid_data_exists:
            print("No valid metrics found to plot (all are NaN). Check for evaluation errors.")
            return

        x = np.arange(len(model_names_all))
        width = 0.25

        # Define colors - using a colormap for distinct colors
        colors = plt.cm.viridis(np.linspace(0.3, 0.9, 3))

        fig, ax = plt.subplots(figsize=(max(6, len(model_names_all) * 1.5), 7)) # Dynamic width

        rects1 = ax.bar(x - width, f1_scores, width, label='F1 Score', color=colors[0], edgecolor='grey', zorder=2)
        rects2 = ax.bar(x, accuracy_scores, width, label='Accuracy', color=colors[1], edgecolor='grey', zorder=2)
        rects3 = ax.bar(x + width, precision_scores, width, label='Precision', color=colors[2], edgecolor='grey', zorder=2)

        ax.set_ylabel('Score', fontsize=12)
        ax.set_xlabel('Model', fontsize=12)
        ax.set_title('Ensemble Model Performance Comparison', fontsize=16, fontweight='bold', pad=20)
        ax.set_xticks(x)
        ax.set_xticklabels(model_names_all, rotation=45, ha='right', fontsize=10)
        ax.set_ylim(0, 1.1) # Set Y limit slightly above 1.0 for labels
        ax.legend(fontsize=10, title="Metrics", title_fontsize='11', loc='upper right')

        # Add value labels using the helper function
        self._add_value_labels(ax, spacing=5)

        # Improve aesthetics
        ax.yaxis.grid(True, linestyle='--', which='major', color='grey', alpha=0.5, zorder=1) # Grid behind bars
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['left'].set_color('grey')
        ax.spines['bottom'].set_color('grey')
        ax.tick_params(axis='x', colors='grey')
        ax.tick_params(axis='y', colors='grey')
        ax.yaxis.label.set_color('grey')
        ax.xaxis.label.set_color('grey')
        ax.title.set_color('grey')

        fig.tight_layout()
        plt.show()