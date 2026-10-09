import pytest

from work_queue import WorkQueue


class FakeClock:
    """Manual clock for tests. Time moves only when a test calls advance()."""

    def __init__(self, start: float = 0.0):
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def queue(clock: FakeClock) -> WorkQueue:
    return WorkQueue(max_retries=2, timeout=30, capacity=3, clock=clock)
