# AlgoRepartie

Distributed systems algorithms implemented in C as part of the "Algorithmique Répartie" coursework at Ensimag (Grenoble INP). Processes communicate exclusively over TCP sockets (no shared memory), simulating a real distributed system where every message passes through `connect`/`accept`/`send`/`recv`.

## Projects

### Lab01 — Distributed Mutual Exclusion (Lamport Clocks)

A **live** and **fair** distributed lock built on top of Lamport logical clocks.

- Each process maintains a logical clock and a request queue totally ordered by `(clock, process_id)`.
- A process is granted the critical section only when its request reaches the head of every queue and it has received an acknowledgment from all other processes with a timestamp ≥ its own request.
- This guarantees mutual exclusion, fairness (FIFO request order), and liveness.

Processes are spawned as `./process <id> <test_file>` and compete for a shared critical section (`./critical`), with every socket call intercepted by an `LD_PRELOAD` hook (`hook.c`) to log and verify the communication pattern.

### Lab02 — Reliable Broadcast (Raft & Bracha)

Two broadcast algorithms providing different reliability guarantees, selected via the `PROTOCOL` environment variable:

- **Raft-style broadcast**: a single leader proposes a value; once a majority of followers acknowledge it, the leader commits and the value is delivered everywhere.
- **Bracha reliable broadcast**: a Byzantine-tolerant protocol (`Propose → Echo → Vote → Deliver`) that tolerates up to `f = (n-1)/3` faulty processes while guaranteeing that if one correct process delivers a message, all correct processes eventually do.

## Repository layout

```
Lab01/
  process.c    # distributed lock implementation
  critical.c   # simulated critical section
  hook.c       # LD_PRELOAD hook instrumenting socket calls
  run.pl       # spawns N processes from a test file
  tests/       # sample scenarios
Lab02/
  process.c    # Raft / Bracha broadcast implementation
  run.pl       # spawns N processes from a test file
  tests/       # sample scenarios
```

## Building and running

Each lab has its own `Makefile`.

```bash
# Lab01
cd Lab01
make                 # builds process, critical and hook.so
make check            # runs every test in tests/

# Lab02
cd Lab02
make                              # builds process
PROTOCOL=RAFT   make check_raft   # runs every test with the Raft algorithm
PROTOCOL=BRACHA make check_bracha # runs every test with the Bracha algorithm
```

Test scenarios describe the number of processes and the commands each one executes (lock/wait for Lab01, broadcast for Lab02); see each lab's own `README.md` for the exact input format and expected output.

## Notes

- No shared memory between processes: all coordination happens over TCP sockets.
- `hook.c` and `run.pl` are provided as-is by the course and must not be modified, since they are used to verify that communication goes exclusively through the standard socket API.
- Built and tested on Linux.

## Authors

Group project for the Ensimag ISI "Algorithmique Répartie" course.
