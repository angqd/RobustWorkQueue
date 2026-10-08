# Design a Robust Work Queue

Design and implement a **work queue** that manages jobs throughout their lifecycle, from submission to successful completion or permanent failure.

## Requirements

The queue must support the following operations:

1. **reserve()** : Claim a pending job for processing and return it to the worker. A reserved job must not be available to other workers simultaneously.

2. **complete(jobId)** : Mark a successfully processed job as completed so it cannot be reserved again.

3. **fail(jobId)** : Report a failed processing attempt. The job should be eligible for retry if it has not exceeded the allowed retry limit.

4. **Timeouts and Retries**: A reserved job must automatically become eligible for retry if the worker fails to complete it within a configured timeout. The system must track the number of processing attempts for each job.

5. **Dead-Letter Queue (DLQ)**: Jobs that exhaust their allowed retry attempts must be moved to a separate dead-letter queue rather than being repeatedly reprocessed.

## Constraints

1. A job can be reserved by only one worker at a time.
2. A job that has been completed must never be returned by `reserve()`.
3. Jobs whose reservation expires must become eligible for retry.
4. A job must be moved to the dead-letter queue once its retry limit is exhausted.
5. The queue must correctly handle repeated failures, expired reservations, and transitions between job states.
6. Assume jobs are processed by multiple workers concurrently. The implementation must prevent inconsistent state transitions.
7. The retry limit, reservation timeout, and queue capacity should be configurable where appropriate.
