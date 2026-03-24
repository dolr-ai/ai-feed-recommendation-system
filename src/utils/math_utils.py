import math
from typing import List

import numpy as np


def log_safe(x: float) -> float:
    """log1p of max(0, x). Safe for zero and negative inputs."""
    return math.log1p(max(0.0, x))


def robust_normalize(
    values: List[float],
    lower_pct: float = 5.0,
    upper_pct: float = 95.0,
) -> List[float]:
    """
    Robust min-max normalization:
    1. Clip values to [p_lower, p_upper] percentile range.
    2. Min-max scale the clipped values to [0, 1].
    3. If all values identical (p_high == p_low), return [0.0, ...].
    """
    if not values:
        return []
    arr = np.array(values, dtype=float)
    p_low = float(np.percentile(arr, lower_pct))
    p_high = float(np.percentile(arr, upper_pct))
    if abs(p_high - p_low) < 1e-9:
        return [0.0] * len(values)
    clipped = np.clip(arr, p_low, p_high)
    scaled = (clipped - p_low) / (p_high - p_low)
    return scaled.tolist()


def batch_list(items: List, batch_size: int) -> List[List]:
    """Split a list into batches of batch_size."""
    return [items[i : i + batch_size] for i in range(0, len(items), batch_size)]
