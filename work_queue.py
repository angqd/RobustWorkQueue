from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
import threading
import time 
from uuid import UUID,uuid4



class QueueFull(Exception):
    """Raised by submit() when the queue has no free capacity."""


class JobState(StrEnum):
    PENDING = "PENDING"
    RESERVED = "RESERVED"
    COMPLETED = "COMPLETED"
    DEAD = "DEAD"


@dataclass
class Job:
    id: UUID
    payload: str
    state: JobState
    attempts: int
    reserved_until: float | None = None

@dataclass(frozen=True)
class Lease:
    job_id: UUID
    payload: str
    token: int


class WorkQueue:
    """In-memory work queue with leases, retries and a dead-letter queue.

    max_retries: retries after the first attempt, so a job gets at most
        max_retries + 1 attempts. Once attempts exceed max_retries, the job is DEAD.
    capacity: maximum number of active jobs (PENDING + RESERVED). Completed
        and dead jobs do not count against it.
    timeout: seconds a worker holds a lease before the job can be retried.
    clock: returns the current time in seconds. Injected so tests can control time.
    """

    def __init__(
        self,
        max_retries: int,
        timeout: float,
        capacity: int,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.max_retries = max_retries
        self.timeout = timeout
        self.capacity = capacity
        self.clock = clock

        self._lock = threading.Lock()
        self._jobs: dict[UUID, Job] = {}
        self._pending: deque[UUID] = deque()
        self._dlq: list[Job] = []

    def _active_count_locked(self):
        count=0
        for id in self._jobs:
            job = self._jobs[id]
            if job.state == JobState.PENDING or job.state == JobState.RESERVED:
                count+=1
        return count

    def _check_lease_locked(self,job_id,token)->Job|None:
        # valid only while RESERVED, token matches the current attempt, and the deadline has not passed
        job = self._jobs.get(job_id)
        if job is None or job.state is not JobState.RESERVED:
            return None
        if token != job.attempts:
            return None
        if self.clock() >= job.reserved_until:   # same boundary as the sweep: expired at <=
            return None
        return job

    def _sweep_locked(self):
        # start with dict and explore optimisations later
        # analyse jobs via dict and change the states if needed 
        for job in self._jobs.values():
            if job.state == JobState.RESERVED and job.reserved_until<= self.clock():
                if job.attempts <= self.max_retries:
                    job.state = JobState.PENDING
                    self._pending.append(job.id)
                else: # failed 
                    job.state = JobState.DEAD
                    self._dlq.append(job)

    

    def submit(self,payload:str)->UUID:
        """Add a PENDING job and return its id. Raises QueueFull if at capacity."""
        with self._lock:
            if self._active_count_locked() >= self.capacity:
                raise QueueFull(f"queue at capacity ({self.capacity})")
            job_id = uuid4()
            job = Job(job_id,payload,JobState.PENDING,0,None)
            self._jobs[job_id] = job
            self._pending.append(job_id)
            return job_id

        
        # create a job 
    def reserve(self):
        """Claim one PENDING job for a single worker, or return None if none is available."""
        with self._lock:
            self._sweep_locked()

            while self._pending:
                job_id = self._pending.popleft()
                job = self._jobs[job_id]
                if job.state != JobState.PENDING:
                    continue  # stale id, drop it and try the next one
                job.attempts += 1
                job.state = JobState.RESERVED
                job.reserved_until = self.clock() + self.timeout
                return Lease(job_id, job.payload, job.attempts)
            return None

    def complete(self,job_id,token)->bool:
        with self._lock:
            job = self._check_lease_locked(job_id,token)
            if job is not None:
                # evict: the job is done and nothing needs its record
                del self._jobs[job_id]
                return True
            else:
                return False


    def fail(self,job_id,token)->bool:
        with self._lock:
            job = self._check_lease_locked(job_id,token)
            if job is not None:
                if job.attempts <= self.max_retries:
                    job.state = JobState.PENDING
                    self._pending.append(job_id)
                else:
                    job.state = JobState.DEAD
                    self._dlq.append(job)
                return True
            else:
                return False
