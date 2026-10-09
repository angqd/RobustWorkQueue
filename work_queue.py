from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
import threading
import time 
from uuid import UUID,uuid4



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
        # check valid job id 
        if job_id in self._jobs and token == self._jobs[job_id].attempts:
            job = self._jobs[job_id]
            if(job.state == JobState.RESERVED):
                return job
            else:
                return None
        else:
            return None

    def _sweep_locked(self):
        return None # stub for now 
    

    def submit(self,payload:str):
        # lock and check capacity
        with self._lock:
            if self._active_count_locked()< self.capacity :
                    job_id = uuid4()
                    job = Job(job_id,payload,JobState.PENDING,0,None)
                    self._jobs[job_id] = job
                    self._pending.append(job_id)
                    return job_id
            else:
                return None # reject 

        
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
                job.state = JobState.COMPLETED
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
