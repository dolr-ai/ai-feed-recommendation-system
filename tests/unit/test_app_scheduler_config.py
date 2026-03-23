from datetime import datetime

from src.core.app import scheduler_next_run_time


def test_scheduler_next_run_time_returns_none_when_startup_run_disabled():
    assert scheduler_next_run_time(False) is None


def test_scheduler_next_run_time_returns_datetime_when_startup_run_enabled():
    assert isinstance(scheduler_next_run_time(True), datetime)
