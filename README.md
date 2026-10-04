# Fault-Tolerant Distributed Task Scheduling and Load Balancing System

Real-time Operating Systems, Concurrency and Distributed Systems project using Python.

## Features

- Real OS worker processes using multiprocessing
- Priority, FCFS and Round Robin scheduling modes
- Application-level time-slice scheduling
- Dynamic task arrival
- TCP socket IPC between master and workers
- CPU and RAM monitoring with psutil
- Load-aware worker selection
- Worker failure detection and automatic recovery
- SQLite task history
- Live Flask dashboard
- Manual worker stop/restart controls

## Run locally

```bash
python3 -m pip install -r requirements.txt
python3 app.py
```

Open:

```
http://127.0.0.1:5001
```

For hosted environments, Flask uses the PORT environment variable.

## Architecture

Browser -> Flask Dashboard -> Master Scheduler -> Worker Processes -> Real CPU Tasks

The current implementation is a single-host distributed master-worker environment: multiple independent OS processes communicate through TCP sockets on the same machine.
