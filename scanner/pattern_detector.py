"""
Pattern detection logic for Marubozu → Doji patterns
Handles both bullish and bearish variants
"""

from dataclasses import dataclass
from typing import Tuple, Dict, Optional
import logging

logger = logging.getLogger(__name__)

@dataclass
class Candle:
    """Represents a single OHLC candle"""
    date: object
    open: float
    high: float
    low: float
    close: float
    volume: float = 0

    @property
    def body(self) -> float:
        """Calculate candle body size"""
        return abs(self.close - self.open)

    @property
    def range(self) -> float:
        """Calculate candle range (high - low)"""
        return self.high - self.low

    @property
    def body_pct(self) -> float:
        """Calculate body percentage of range"""
        if self.range == 0:
            return 0
        return (self.body / self.range) * 100

    @property
    def is_bullish(self) -> bool:
        """Check if candle is bullish"""
        return self.close > self.open

    @property
    def is_bearish(self) -> bool:
        """Check if candle is bearish"""
        return self.close < self.open

    @property
    def body_move_pct(self) -> float:
        """Calculate body move percentage relative to open price"""
        if self.open == 0:
            return 0
        return (self.body / self.open) * 100


class PatternDetector:
    """
    Detects Marubozu → Doji patterns with configurable thresholds
    """

    def __init__(self,
                 marubozu_threshold: float = 0.8,  # 80% body of range
                 doji_threshold: float = 0.20):     # 20% body of range (more strict)
        """
        Initialize pattern detector with thresholds

        Args:
            marubozu_threshold: Minimum body % for Marubozu (0.8 = 80%)
            doji_threshold: Maximum body % for Doji (0.20 = 20%, made more strict)
        """
        self.marubozu_threshold = marubozu_threshold * 100  # Convert to percentage
        self.doji_threshold = doji_threshold * 100  # Convert to percentage

    def is_marubozu(self, candle: Candle) -> Tuple[bool, float]:
        """
        Check if candle is a Marubozu

        Args:
            candle: Candle object to check

        Returns:
            Tuple of (is_marubozu, body_percentage)
        """
        if candle.range == 0:
            logger.debug(f"Skipping {candle.date}: range is 0")
            return False, 0.0

        body_pct = candle.body_pct

        is_marubozu = body_pct >= self.marubozu_threshold

        if is_marubozu:
            logger.debug(f"Marubozu detected on {candle.date}: body_pct={body_pct:.2f}%")

        return is_marubozu, body_pct

    def is_doji(self, candle: Candle) -> Tuple[bool, float]:
        """
        Check if candle is a Doji

        Args:
            candle: Candle object to check

        Returns:
            Tuple of (is_doji, body_percentage)
        """
        if candle.range == 0:
            logger.debug(f"Skipping {candle.date}: range is 0")
            return False, 0.0

        body_pct = candle.body_pct

        is_doji = body_pct < self.doji_threshold

        if is_doji:
            logger.debug(f"Doji detected on {candle.date}: body_pct={body_pct:.2f}%")

        return is_doji, body_pct

    def matches_marubozu_doji(self, c1: Candle, c2: Candle) -> Tuple[bool, Dict]:
        """
        Check if two consecutive candles form Marubozu → Doji pattern

        Pattern requirements:
        1. First candle is Marubozu (body >= 80% of range)
        2. Second candle is Doji (body < 20% of range)
        3. Doji high breaks above Marubozu high
        4. Doji closes inside Marubozu body (rejection)

        Args:
            c1: First candle (potential Marubozu)
            c2: Second candle (potential Doji)

        Returns:
            Tuple of (pattern_found, pattern_details)
        """
        # Check if first candle is Marubozu
        maru_ok, maru_pct = self.is_marubozu(c1)
        if not maru_ok:
            return False, {}

        # Check if second candle is Doji
        doji_ok, doji_pct = self.is_doji(c2)
        if not doji_ok:
            return False, {}

        # Check breakout direction based on Marubozu type
        if c1.is_bullish:  # Bullish Marubozu
            # For bullish: Doji high must break above Marubozu high
            if c2.high <= c1.high:
                logger.debug(f"Pattern failed: Doji high {c2.high} doesn't break Marubozu high {c1.high}")
                return False, {}
            # For bullish: open1 < close2 < close1
            closes_inside = c1.open < c2.close < c1.close
            direction = 'bullish'
        else:  # Bearish Marubozu
            # For bearish: Doji low must break below Marubozu low
            if c2.low >= c1.low:
                logger.debug(f"Pattern failed: Doji low {c2.low} doesn't break Marubozu low {c1.low}")
                return False, {}
            # For bearish: close1 < close2 < open1
            closes_inside = c1.close < c2.close < c1.open
            direction = 'bearish'

        if not closes_inside:
            logger.debug(f"Pattern failed: Doji doesn't close inside Marubozu body")
            return False, {}

        # Pattern matched!
        pattern_details = {
            'direction': direction,
            'marubozu_body_pct': round(maru_pct, 2),
            'doji_body_pct': round(doji_pct, 2),
            'marubozu_body_move_pct': round(c1.body_move_pct, 2),
            'breakout_amount': round(c2.high - c1.high, 2),
            'rejection_strength': self._calculate_rejection_strength(c1, c2, direction)
        }

        logger.info(f"Pattern found: {direction} Marubozu→Doji on {c1.date} to {c2.date}")
        return True, pattern_details

    def _calculate_rejection_strength(self, c1: Candle, c2: Candle, direction: str) -> float:
        """
        Calculate how strongly the Doji rejected the breakout

        Args:
            c1: Marubozu candle
            c2: Doji candle
            direction: 'bullish' or 'bearish'

        Returns:
            Rejection strength percentage (0-100)
        """
        if direction == 'bullish':
            # How far did close2 retrace from the high
            if c2.high == c2.low:
                return 0
            rejection = ((c2.high - c2.close) / (c2.high - c2.low)) * 100
        else:
            # For bearish, measure from low
            if c2.high == c2.low:
                return 0
            rejection = ((c2.close - c2.low) / (c2.high - c2.low)) * 100

        return round(rejection, 2)

    def filter_by_min_body_move(self, candle: Candle, min_body_move_pct: float) -> bool:
        """
        Filter candle by minimum body move percentage

        Args:
            candle: Candle to check
            min_body_move_pct: Minimum body move % required

        Returns:
            True if candle passes filter
        """
        return candle.body_move_pct >= min_body_move_pct


class ExtendedPatternDetector(PatternDetector):
    """
    Extended pattern detector with additional pattern types
    (For future expansion)
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def is_hammer(self, candle: Candle) -> Tuple[bool, Dict]:
        """Check for hammer pattern (future implementation)"""
        # Placeholder for future pattern
        return False, {}

    def is_shooting_star(self, candle: Candle) -> Tuple[bool, Dict]:
        """Check for shooting star pattern (future implementation)"""
        # Placeholder for future pattern
        return False, {}

    def is_engulfing(self, c1: Candle, c2: Candle) -> Tuple[bool, Dict]:
        """Check for engulfing pattern (future implementation)"""
        # Placeholder for future pattern
        return False, {}