# Robust Work Queue

An in-memory, thread-safe work queue. Jobs move through `PENDING`, `RESERVED`, `COMPLETED` and `DEAD`. Workers reserve a job with a lease, and the queue retries expired or failed jobs until they exhaust their attempts, then moves them to a dead-letter queue.

Built as a learning project for the problem in [PROBLEM_STATEMENT.md](PROBLEM_STATEMENT.md).

## Run tests

```
python -m venv venv && source venv/bin/activate
pip install pytest
pytest -q
```

## Run the visual demo

The demo runs the real `WorkQueue` behind a small local server and shows each job's state as you act on it. It uses only the Python standard library, so there is nothing extra to install. It needs Python 3.11 or newer.

```
cd demo
../venv/bin/python server.py
```

Then open http://127.0.0.1:8765 in a browser. Stop the server with `Ctrl+C`.

What you can do on the page:

- **Submit** jobs, **Reserve** one, then **Complete** or **Fail** it with its token.
- **+1s / +10s / +30s** move the clock, so you can watch a lease expire.
- **Scripted scenarios** run fixed sequences: happy path, expired lease with a late complete, retries ending in the dead-letter queue, and capacity limits.

If the page does not load, check that nothing else is using port 8765. On macOS, check System Settings → Privacy & Security → Local Network, and your firewall, for Python.
