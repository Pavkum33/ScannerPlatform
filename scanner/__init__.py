"""
Marubozu → Doji Pattern Scanner Package
"""

from .dhan_client import DhanClient
from .scanner_engine import ScannerEngine
from .pattern_detector import PatternDetector, Candle
from .aggregator import TimeframeAggregator

__version__ = "1.0.0"
__author__ = "Your Name"

__all__ = [
    "DhanClient",
    "ScannerEngine",
    "PatternDetector",
    "Candle",
    "TimeframeAggregator"
]