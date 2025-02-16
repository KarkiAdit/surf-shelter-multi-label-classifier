"""
Surf Shelter Multi-Label Model Trainer

This package provides `SSMultiLabelClassifier`, a machine learning model 
for classifying website content into multiple risk categories, including 
clickbait, pay fraud, and harmful content.

For implementation details, refer to `src/`.

### Exposed Class:
- `SSMultiLabelClassifier`: Main entry point for multi-label classification.

All other modules are internal and should not be accessed directly.
"""

from src import SSMultiLabelClassifier

__all__ = ["SSMultiLabelClassifier"]
