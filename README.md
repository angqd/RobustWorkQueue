# Robust Work Queue

An in-memory, thread-safe work queue. Jobs move through `PENDING`, `RESERVED`, `COMPLETED` and `DEAD`. Workers reserve a job with a lease, and the queue retries expired or failed jobs until they exhaust their attempts, then moves them to a dead-letter queue.

Built as a learning project for the problem in [PROBLEM_STATEMENT.md](PROBLEM_STATEMENT.md).

## Run tests

```
python -m venv venv && source venv/bin/activate
pip install pytest
pytest -q
```
