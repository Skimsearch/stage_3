## Fault-Tolerance Experimental Results (`2026-11-20`)

### Failover Timing Summary (`w2 -> w1` on `ingest-2`)

| Scenario | Run ID | Relevo | `ts_requeued` | `ts_reclaimed` | Desde claim original | Desde requeue |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: |
| Loaded (6 tasks) | `A-worker-2nodes-run04` | `w2 -> w1` | `2026-11-20T19:06:43.272473600Z` | `2026-11-20T19:06:46.902243600Z` | `7503 ms` | `3629 ms` |
| Loaded (6 tasks) | `A-worker-2nodes-run05` | `w2 -> w1` | `2026-11-20T19:06:59.202278900Z` | `2026-11-20T19:07:04.545874400Z` | `7501 ms` | `5343 ms` |
| Loaded (6 tasks) | `A-worker-2nodes-run06` | `w2 -> w1` | `2026-11-20T19:07:22.230662200Z` | `2026-11-20T19:07:22.230662200Z` | `7897 ms` | `0 ms` |
| Idle (2 tasks) | `A-idle-2nodes-run01` | `w2 -> w1` | `2026-11-20T19:08:39.137208500Z` | `2026-11-20T19:08:39.159633200Z` | `1500 ms` | `22 ms` |
| Idle (2 tasks) | `A-idle-2nodes-run02` | `w2 -> w1` | `2026-11-20T19:08:53.258862300Z` | `2026-11-20T19:08:53.258862300Z` | `3482 ms` | `0 ms` |
| Idle (2 tasks) | `A-idle-2nodes-run03` | `w2 -> w1` | `2026-11-20T19:09:10.811181300Z` | `2026-11-20T19:09:10.811181300Z` | `7918 ms` | `0 ms` |

### Worker × Task Matrices by Run

#### `A-worker-2nodes-run04`
| Tarea | `w1` | `w2` |
| :--- | :---: | :---: |
| `ingest-1` | `DONE (1500ms)` | `-` |
| `ingest-2` | `DONE (1501ms)` | `LOST (lease-expired)` |
| `ingest-3` | `DONE (1500ms)` | `-` |
| `ingest-4` | `DONE (1500ms)` | `-` |
| `ingest-5` | `DONE (1501ms)` | `-` |
| `ingest-6` | `DONE (1501ms)` | `-` |

#### `A-worker-2nodes-run05`
| Tarea | `w1` | `w2` |
| :--- | :---: | :---: |
| `ingest-1` | `DONE (1501ms)` | `-` |
| `ingest-2` | `DONE (1501ms)` | `LOST (lease-expired)` |
| `ingest-3` | `DONE (1500ms)` | `-` |
| `ingest-4` | `DONE (1500ms)` | `-` |
| `ingest-5` | `DONE (1500ms)` | `-` |
| `ingest-6` | `DONE (1501ms)` | `-` |

#### `A-worker-2nodes-run06`
| Tarea | `w1` | `w2` |
| :--- | :---: | :---: |
| `ingest-1` | `DONE (1500ms)` | `-` |
| `ingest-2` | `DONE (1500ms)` | `LOST (lease-expired)` |
| `ingest-3` | `DONE (1502ms)` | `-` |
| `ingest-4` | `DONE (1501ms)` | `-` |
| `ingest-5` | `DONE (1501ms)` | `-` |
| `ingest-6` | `DONE (1500ms)` | `-` |

#### `A-idle-2nodes-run01`
| Tarea | `w1` | `w2` |
| :--- | :---: | :---: |
| `ingest-1` | `DONE (1500ms)` | `-` |
| `ingest-2` | `DONE (1500ms)` | `LOST (lease-expired)` |

#### `A-idle-2nodes-run02`
| Tarea | `w1` | `w2` |
| :--- | :---: | :---: |
| `ingest-1` | `DONE (1501ms)` | `-` |
| `ingest-2` | `DONE (1500ms)` | `LOST (lease-expired)` |

#### `A-idle-2nodes-run03`
| Tarea | `w1` | `w2` |
| :--- | :---: | :---: |
| `ingest-1` | `DONE (1502ms)` | `-` |
| `ingest-2` | `DONE (1500ms)` | `LOST (lease-expired)` |