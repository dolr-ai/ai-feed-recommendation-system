from datetime import datetime, timedelta

LATENCY_BUCKETS = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]


class HourlyMetricsStore:
    """
    Stores request metrics in hourly buckets for 24-hour aggregation.

    Algorithm:
        - Each hour has a dict of endpoints
        - Each endpoint has: count, latency_sum, histogram buckets
        - On get_summary(): aggregate all hours, compute percentiles
        - Auto-prunes data older than 24 hours

    Note: No lock needed - uvicorn workers are separate processes,
    and within each process asyncio is single-threaded.
    """

    def __init__(self):
        self._data = {}

    def record(self, endpoint: str, latency_seconds: float, status_code: int):
        """
        Record a single request's latency and status for the current hour.

        Algorithm:
            1. Get current hour key (UTC)
            2. Initialize hour bucket if new, prune old data
            3. Initialize endpoint stats if new (count, latency, histogram, errors)
            4. Increment request count and accumulate latency
            5. Classify status code: 4xx -> errors_4xx, 5xx -> errors_5xx
            6. Place latency in appropriate histogram bucket

        Args:
            endpoint: Normalized endpoint path (e.g., "GET /health")
            latency_seconds: Request duration in seconds
            status_code: HTTP response status code (e.g., 200, 404, 500)
        """
        hour_key = datetime.utcnow().strftime("%Y-%m-%dT%H:00Z")

        if hour_key not in self._data:
            self._data[hour_key] = {}
            self._prune_old_hours()

        if endpoint not in self._data[hour_key]:
            self._data[hour_key][endpoint] = {
                "count": 0, "latency_sum": 0.0,
                "buckets": [0] * len(LATENCY_BUCKETS),
                "errors_4xx": 0,
                "errors_5xx": 0,
            }

        stats = self._data[hour_key][endpoint]
        stats["count"] += 1
        stats["latency_sum"] += latency_seconds

        if 400 <= status_code < 500:
            stats["errors_4xx"] += 1
        elif status_code >= 500:
            stats["errors_5xx"] += 1

        for i, bound in enumerate(LATENCY_BUCKETS):
            if latency_seconds <= bound:
                stats["buckets"][i] += 1
                break
        else:
            stats["buckets"][-1] += 1

    def _prune_old_hours(self):
        """Remove data older than 24 hours."""
        cutoff = (datetime.utcnow() - timedelta(hours=24)).strftime("%Y-%m-%dT%H:00Z")
        self._data = {k: v for k, v in self._data.items() if k >= cutoff}

    def get_summary(self) -> dict:
        """
        Get 24-hour summary with percentiles and error stats per endpoint.

        Algorithm:
            1. Prune old data (remove hours older than 24h)
            2. Aggregate counts across all hours per endpoint (requests, latency, errors)
            3. Compute p50, p90, p99 latencies from aggregated histogram
            4. Calculate error rate as (4xx + 5xx) / total * 100
            5. Return formatted summary with latency and error metrics

        Returns:
            Dict with window, hours_with_data, and per-endpoint stats including:
            - total_requests, errors_4xx, errors_5xx, error_rate_pct
            - avg_latency_ms, p50_ms, p90_ms, p99_ms
        """
        self._prune_old_hours()

        aggregated = {}
        for endpoints in self._data.values():
            for endpoint, stats in endpoints.items():
                if endpoint not in aggregated:
                    aggregated[endpoint] = {
                        "count": 0, "latency_sum": 0.0,
                        "buckets": [0] * len(LATENCY_BUCKETS),
                        "errors_4xx": 0,
                        "errors_5xx": 0,
                    }
                agg = aggregated[endpoint]
                agg["count"] += stats["count"]
                agg["latency_sum"] += stats["latency_sum"]
                agg["errors_4xx"] += stats.get("errors_4xx", 0)
                agg["errors_5xx"] += stats.get("errors_5xx", 0)
                for i, c in enumerate(stats["buckets"]):
                    agg["buckets"][i] += c

        result = {"window": "24h", "hours_with_data": len(self._data), "endpoints": {}}

        for endpoint, stats in aggregated.items():
            if stats["count"] == 0:
                continue
            total_errors = stats["errors_4xx"] + stats["errors_5xx"]
            result["endpoints"][endpoint] = {
                "total_requests": stats["count"],
                "errors_4xx": stats["errors_4xx"],
                "errors_5xx": stats["errors_5xx"],
                "error_rate_pct": round(total_errors / stats["count"] * 100, 2),
                "avg_latency_ms": round((stats["latency_sum"] / stats["count"]) * 1000, 2),
                "p50_ms": round(self._percentile(stats["buckets"], 0.50) * 1000, 2),
                "p90_ms": round(self._percentile(stats["buckets"], 0.90) * 1000, 2),
                "p99_ms": round(self._percentile(stats["buckets"], 0.99) * 1000, 2),
            }

        return result

    def _percentile(self, buckets: list, p: float) -> float:
        """
        Compute percentile via linear interpolation between bucket boundaries.

        Args:
            buckets: List of counts per latency bucket (non-cumulative)
            p: Percentile as fraction (0.50 for p50, 0.99 for p99)

        Returns:
            Estimated latency value at the given percentile (in seconds)
        """
        total = sum(buckets)
        if total == 0:
            return 0.0

        target, cumulative, prev_bound = p * total, 0, 0.0

        for i, count in enumerate(buckets):
            cumulative += count
            if cumulative >= target:
                bound = LATENCY_BUCKETS[i]
                if count == 0:
                    return bound
                fraction = (target - (cumulative - count)) / count
                return prev_bound + fraction * (bound - prev_bound)
            prev_bound = LATENCY_BUCKETS[i]

        return LATENCY_BUCKETS[-1]


# Singleton instance for use across the application
hourly_metrics = HourlyMetricsStore()


if __name__ == "__main__":
    # Test the HourlyMetricsStore with error tracking
    store = HourlyMetricsStore()

    import random

    # Simulate successful requests to /health
    for _ in range(100):
        store.record("GET /health", random.uniform(0.001, 0.01), 200)

    # Simulate mixed requests to /recommend endpoint
    # 90 successful, 5 client errors (4xx), 5 server errors (5xx)
    for _ in range(90):
        store.record("GET /recommend/{user_id}", random.uniform(0.05, 0.3), 200)
    for _ in range(5):
        store.record("GET /recommend/{user_id}", random.uniform(0.01, 0.05), 404)
    for _ in range(5):
        store.record("GET /recommend/{user_id}", random.uniform(0.1, 0.5), 500)

    import json
    print(json.dumps(store.get_summary(), indent=2))
