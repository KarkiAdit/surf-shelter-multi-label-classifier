"""
This module contains feature extraction classes for different threat types 
(clickbait, harmful content, and payfraud).

It provides a centralized location for importing feature extraction classes 
for use in other parts of the project, such as training and prediction.
"""

from .clickbait_features import ClickbaitFeatureExtractor
from .harmful_content_features import HarmfulContentFeatureExtractor
from .payfraud_features import PayfraudFeatureExtractor

__all__ = [
    "ClickbaitFeatureExtractor",
    "HarmfulContentFeatureExtractor",
    "PayfraudFeatureExtractor",
]