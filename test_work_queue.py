from uuid import uuid4

import pytest

from work_queue import JobState, QueueFull, WorkQueue


def reserve_one(queue: WorkQueue, payload: str = "a"):
    """Submit one job and reserve it. Returns (job_id, lease)."""
    job_id = queue.submit(payload)
    lease = queue.reserve()
    return job_id, lease


def test_queue_starts_empty(queue: WorkQueue):
    assert queue.capacity == 3
    assert queue.max_retries == 2


def test_fake_clock_only_moves_when_advanced(clock):
    assert clock() == 0.0
    clock.advance(10)
    assert clock() == 10.0


# --- submit ---
def test_submit_returns_an_id(queue):
    job_id = queue.submit("hello")          # act
    assert job_id is not None               # assert

def test_submit_rejects_when_full(queue):
    for i in range(3):
        job_id = queue.submit(f"job{i}")
        assert job_id is not None

    with pytest.raises(QueueFull):
        queue.submit("job4")


# --- reserve ---
def test_reserve_returns_none_whenempty(queue):
    assert queue.reserve() is None

def test_reserve_returns_lease_with_token_1(queue):
    queue.submit("a")
    lease = queue.reserve()
    assert lease.payload == "a"
    assert lease.token == 1

def test_reserve_fifo(queue):
    first = queue.submit("a")
    second = queue.submit("b")

    lease1 = queue.reserve()
    lease2 = queue.reserve()
    assert lease1.job_id == first
    assert lease2.job_id == second

def test_reserve_duplicate_handout(queue):
    queue.submit("a")
    lease = queue.reserve()
    assert lease is not None
    lease2 = queue.reserve()
    assert lease2 is None

def test_reserve_deadline_clock(queue,clock):
    clock.advance(100)
    job_id = queue.submit("a")
    queue.reserve()
    job = queue._jobs[job_id]
    assert job.reserved_until == 100+30


# --- complete ---
def test_submit_correct_complete(queue):
    job_id = queue.submit("a")
    lease = queue.reserve()
    assert queue.complete(lease.job_id,lease.token) is True
    assert job_id not in queue._jobs            # evicted on completion

def test_complete_unknown_id_is_rejected(queue):
    assert queue.complete(uuid4(), 1) is False

def test_complete_wrong_token_is_rejected(queue):
    job_id, lease = reserve_one(queue)
    assert queue.complete(job_id, lease.token + 1) is False
    assert queue._jobs[job_id].state is JobState.RESERVED

def test_late_complete_after_expiry_is_rejected(queue, clock):
    job_id, lease = reserve_one(queue)
    clock.advance(31)                       # past the 30s deadline, nobody re-reserved
    assert queue.complete(job_id, lease.token) is False
    assert queue._jobs[job_id].state is JobState.RESERVED

def test_complete_exactly_at_deadline_is_rejected(queue, clock):
    job_id, lease = reserve_one(queue)
    clock.advance(30)
    assert queue.complete(job_id, lease.token) is False

def test_complete_pending_job_is_rejected(queue):
    job_id = queue.submit("a")                  # never reserved
    assert queue.complete(job_id, 1) is False
    assert queue._jobs[job_id].state is JobState.PENDING

def test_complete_twice_second_is_rejected(queue):
    job_id, lease = reserve_one(queue)
    assert queue.complete(job_id, lease.token) is True
    assert queue.complete(job_id, lease.token) is False

def test_completed_job_is_never_reserved_again(queue):
    job_id, lease = reserve_one(queue)
    queue.complete(job_id, lease.token)
    assert queue.reserve() is None

def test_complete_frees_capacity(queue):
    for i in range(3):
        queue.submit(f"job{i}")
    with pytest.raises(QueueFull):              # full
        queue.submit("overflow")
    lease = queue.reserve()
    queue.complete(lease.job_id, lease.token)
    assert queue.submit("fits now") is not None


# --- fail ---
def test_fail_within_limit_returns_job_to_pending(queue):
    job_id, lease = reserve_one(queue)
    assert queue.fail(job_id, lease.token) is True
    assert queue._jobs[job_id].state is JobState.PENDING

def test_failed_job_is_reserved_again_with_next_token(queue):
    job_id, lease = reserve_one(queue)
    queue.fail(job_id, lease.token)
    lease2 = queue.reserve()
    assert lease2.job_id == job_id
    assert lease2.token == 2

def test_fail_wrong_token_is_rejected(queue):
    job_id, lease = reserve_one(queue)
    assert queue.fail(job_id, lease.token + 1) is False
    assert queue._jobs[job_id].state is JobState.RESERVED

def test_fail_unknown_id_is_rejected(queue):
    assert queue.fail(uuid4(), 1) is False

def test_fail_pending_job_is_rejected(queue):
    job_id = queue.submit("a")
    assert queue.fail(job_id, 1) is False
    assert queue._jobs[job_id].state is JobState.PENDING

def test_fail_completed_job_is_rejected(queue):
    job_id, lease = reserve_one(queue)
    queue.complete(job_id, lease.token)
    assert queue.fail(job_id, lease.token) is False
    assert job_id not in queue._jobs


# --- dead-letter queue ---
def test_job_goes_to_dlq_after_retries_are_exhausted(queue):
    # max_retries=2 means 1 first attempt + 2 retries = 3 attempts in total
    job_id = queue.submit("a")
    for _ in range(3):
        lease = queue.reserve()
        assert lease is not None
        queue.fail(job_id, lease.token)

    assert queue._jobs[job_id].state is JobState.DEAD
    assert [job.id for job in queue._dlq] == [job_id]
    assert queue.reserve() is None

def test_job_is_not_dead_before_retries_are_exhausted(queue):
    job_id = queue.submit("a")
    for _ in range(2):                          # 2 failures, 1 attempt left
        lease = queue.reserve()
        queue.fail(job_id, lease.token)
    assert queue._jobs[job_id].state is JobState.PENDING
    assert queue._dlq == []

def test_complete_succeeds_on_the_last_allowed_attempt(queue):
    job_id = queue.submit("a")
    for _ in range(2):
        lease = queue.reserve()
        queue.fail(job_id, lease.token)
    lease = queue.reserve()
    assert lease.token == 3                     # last attempt
    assert queue.complete(job_id, lease.token) is True

def test_dead_job_cannot_be_completed_or_failed(queue):
    job_id = queue.submit("a")
    for _ in range(3):
        lease = queue.reserve()
        queue.fail(job_id, lease.token)
    assert queue.complete(job_id, 3) is False
    assert queue.fail(job_id, 3) is False
    assert queue._jobs[job_id].state is JobState.DEAD

def test_dead_job_frees_capacity(queue):
    # kill the only job first, then fill the queue to capacity (3).
    # If the dead job still counted as active, the third submit would be rejected.
    job_id = queue.submit("a")
    for _ in range(3):
        lease = queue.reserve()
        queue.fail(job_id, lease.token)
    assert queue._jobs[job_id].state is JobState.DEAD
    for i in range(3):
        assert queue.submit(f"x{i}") is not None

def test_fail_twice_second_is_rejected(queue):
    job_id, lease = reserve_one(queue)
    assert queue.fail(job_id, lease.token) is True      # back to PENDING
    assert queue.fail(job_id, lease.token) is False     # stale: job is not RESERVED
    queue.reserve()
    assert queue.reserve() is None                      # no duplicate entry in the queue


# --- timeouts and sweep ---
def test_reservation_is_not_expired_before_deadline(queue, clock):
    queue.submit("a")
    queue.reserve()
    clock.advance(29)
    assert queue.reserve() is None

def test_reservation_expires_exactly_at_deadline(queue, clock):
    # rule from state_trans.txt: expired when reserved_until <= now
    job_id, lease = reserve_one(queue)
    clock.advance(30)
    lease2 = queue.reserve()
    assert lease2 is not None
    assert lease2.job_id == job_id

def test_expired_job_is_reserved_again_with_next_token(queue, clock):
    job_id, lease = reserve_one(queue)
    clock.advance(31)
    lease2 = queue.reserve()
    assert lease2.job_id == job_id
    assert lease2.token == 2

def test_expired_job_gets_a_fresh_deadline(queue, clock):
    job_id, _ = reserve_one(queue)
    clock.advance(31)
    queue.reserve()
    assert queue._jobs[job_id].reserved_until == 31 + 30

def test_expiry_counts_as_an_attempt_and_exhausts_retries(queue, clock):
    job_id = queue.submit("a")
    for _ in range(3):                          # 3 attempts, all time out
        assert queue.reserve() is not None
        clock.advance(31)
    assert queue.reserve() is None              # sweep moves it to DEAD
    assert queue._jobs[job_id].state is JobState.DEAD
    assert [job.id for job in queue._dlq] == [job_id]

def test_stale_complete_after_re_reserve_is_rejected(queue, clock):
    job_id, old = reserve_one(queue)
    clock.advance(31)
    new = queue.reserve()                       # worker B holds token 2
    assert queue.complete(job_id, old.token) is False
    assert queue._jobs[job_id].state is JobState.RESERVED
    assert queue.complete(job_id, new.token) is True

def test_stale_fail_after_re_reserve_does_not_steal_the_lease(queue, clock):
    job_id, old = reserve_one(queue)
    clock.advance(31)
    queue.reserve()                             # worker B holds token 2
    assert queue.fail(job_id, old.token) is False
    assert queue._jobs[job_id].state is JobState.RESERVED
    assert queue.reserve() is None              # job is not handed to a third worker

def test_expiry_frees_capacity_when_job_goes_dead(queue, clock):
    queue.submit("a")
    for _ in range(3):
        queue.reserve()
        clock.advance(31)
    queue.reserve()                             # sweep runs, job a is DEAD
    for i in range(3):
        assert queue.submit(f"x{i}") is not None
