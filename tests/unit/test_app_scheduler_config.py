from datetime import datetime

from src.core.app import scheduler_next_run_time


def test_scheduler_next_run_time_returns_none_when_startup_run_disabled():
    assert scheduler_next_run_time(False) is None


def test_scheduler_next_run_time_returns_datetime_when_startup_run_enabled():
    assert isinstance(scheduler_next_run_time(True), datetime)


def test_scheduler_next_run_time_applies_startup_delay():
    next_run = scheduler_next_run_time(True, delay_sec=5)
    assert isinstance(next_run, datetime)
    assert next_run > datetime.now()
