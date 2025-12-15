"""
Rate limiting utilities for controlling alert/notification frequency.
"""

import time


class ErrorAlertRateLimiter:
    """
    Rate limiter for error alerts to prevent spam during incidents.

    Uses sliding window to track alerts per endpoint. Allows max N alerts
    per endpoint per time window (default: 4 per minute).

    Algorithm:
        - Maintain dict of endpoint -> list of timestamps
        - On should_alert(): prune expired timestamps, check if under limit
        - If under limit: add timestamp, return True
        - If at/over limit: return False

    Args:
        limit: Max alerts per endpoint per window
        window: Time window in seconds

    Attributes:
        limit: Max alerts per endpoint per window
        window: Time window in seconds
        _alerts: Dict mapping endpoint to list of alert timestamps
    """

    def __init__(self, limit: int = 4, window: int = 60):
        self.limit = limit
        self.window = window
        self._alerts = {}  # endpoint -> [timestamp, timestamp, ...]

    def should_alert(self, endpoint: str) -> bool:
        """
        Check if we should send an alert for this endpoint.

        Algorithm:
            1. Get current time
            2. Remove expired timestamps for this endpoint
            3. If under limit, add current timestamp and return True
            4. If at/over limit, return False

        Args:
            endpoint: Normalized endpoint path (e.g., "GET /recommend/{user_id}")

        Returns:
            True if alert should be sent, False if rate limited
        """
        now = time.time()
        cutoff = now - self.window

        # Get or create timestamp list for endpoint
        if endpoint not in self._alerts:
            self._alerts[endpoint] = []

        # Remove expired timestamps
        self._alerts[endpoint] = [ts for ts in self._alerts[endpoint] if ts > cutoff]

        # Check if under limit
        if len(self._alerts[endpoint]) < self.limit:
            self._alerts[endpoint].append(now)
            return True

        return False

    def reset(self, endpoint: str = None):
        """
        Reset rate limiter state.

        Args:
            endpoint: If provided, reset only this endpoint. Otherwise reset all.
        """
        if endpoint:
            self._alerts.pop(endpoint, None)
        else:
            self._alerts.clear()


if __name__ == "__main__":
    # Test the rate limiter
    limiter = ErrorAlertRateLimiter(limit=4, window=60)

    endpoint = "GET /recommend/{user_id}"

    # First 4 should pass
    for i in range(4):
        result = limiter.should_alert(endpoint)
        print(f"Alert {i+1}: {result}")  # Should be True

    # 5th should be rate limited
    result = limiter.should_alert(endpoint)
    print(f"Alert 5: {result}")  # Should be False

    # Different endpoint should pass
    result = limiter.should_alert("GET /health")
    print(f"Different endpoint: {result}")  # Should be True
