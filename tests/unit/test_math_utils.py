import math

from src.utils.math_utils import batch_list, log_safe, robust_normalize


def test_log_safe_clamps_negative_values():
    assert log_safe(-10) == 0.0
    assert log_safe(0) == 0.0
    assert math.isclose(log_safe(9), math.log1p(9))


def test_robust_normalize_returns_empty_list_for_empty_input():
    assert robust_normalize([]) == []


def test_robust_normalize_returns_all_zeros_for_identical_values():
    assert robust_normalize([5.0, 5.0, 5.0]) == [0.0, 0.0, 0.0]


def test_robust_normalize_clips_outliers_before_scaling():
    values = robust_normalize([0.0, 1.0, 100.0], lower_pct=0.0, upper_pct=50.0)
    assert values == [0.0, 1.0, 1.0]


def test_batch_list_splits_items_into_expected_chunks():
    assert batch_list([1, 2, 3, 4, 5], 2) == [[1, 2], [3, 4], [5]]
