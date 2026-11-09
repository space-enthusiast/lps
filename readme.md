# lps

light pub sub program written in kotlin

## Plan

Each stage creates one problem. Solving it is the next stage.

| Done | # | Build | Problem to hit | Learn |
|---|---|---|---|---|
| ✓ | 1 | Single thread, sync calls | `publish` inside a handler recurses | observer pattern |
| ✓ | 2 | Single thread + queue | Ordering, endless events | event loop |
| ✓ | 3 | Separate publish and worker threads | Race conditions, busy-wait | thread safety, `BlockingQueue` |
|   | 4 | A thread or queue per subscriber | Slow subscribers, unbounded queues | backpressure |
|   | 5 | Save to a file | Where did I stop reading after a restart? | append-only log, offset |
|   | 6 | Multiple processes sharing a file | Concurrent writes, new-message signal | file locks, mmap |
|   | 7 | One TCP broker | Dropped connections, packet boundaries | protocols, framing, keepalive |
|   | 8 (bonus) | Multiple brokers | Duplicates, loops | flooding, dedup, rendezvous |