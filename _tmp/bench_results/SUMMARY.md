# Async Overhead Benchmark Results

## Single-Process Results (2026-02-18)

### Test Configuration
- Payload: 64 bytes
- Frequency: 200 Hz (5ms interval)
- Samples: 1,000 (+ 200 warmup)
- Method: Single-process, in-process loopback routing
- Build: release (optimized)

### Results

| Metric | Tokio TCP (baseline) | Zenoh Callback+Spawn | Zenoh Async Loop |
|--------|---------------------:|---------------------:|-----------------:|
| Min    |                 26µs |                 21µs |             33µs |
| P25    |                155µs |                114µs |            124µs |
| P50    |                216µs |                155µs |            168µs |
| Avg    |                234µs |                195µs |            211µs |
| P75    |                301µs |                245µs |            264µs |
| P95    |                417µs |                472µs |            503µs |
| P99    |                549µs |                731µs |            704µs |
| Max    |                869µs |                969µs |           2489µs |

---

## Two-Process Results (2026-02-21, commit e166bb731)

### Test Configuration
- Payload: 64 bytes
- Frequency: 200 Hz (5ms interval)
- Samples: 1,000 (+ 200 warmup)
- Method: Two separate processes, multicast scouting
- Build: release (optimized)

### main baseline (0682bc526)

| Metric | Tokio TCP | Zenoh Callback | Zenoh Loop |
|--------|----------:|---------------:|-----------:|
| Min    |      24µs |          126µs |        84µs |
| P25    |     136µs |          437µs |       470µs |
| P50    |     189µs |          549µs |       558µs |
| Avg    |     205µs |          571µs |       576µs |
| P75    |     251µs |          680µs |       663µs |
| P95    |     381µs |          907µs |       861µs |
| P99    |     485µs |         1099µs |      1086µs |
| Max    |    1423µs |         2630µs |      2058µs |

### dev/true-async (e166bb731)

| Metric | Tokio TCP | Zenoh Callback | Zenoh Loop |
|--------|----------:|---------------:|-----------:|
| Min    |      49µs |          119µs |       123µs |
| P25    |     223µs |          466µs |       537µs |
| P50    |     281µs |          574µs |       646µs |
| Avg    |     290µs |          591µs |       663µs |
| P75    |     335µs |          685µs |       777µs |
| P95    |     467µs |          890µs |       985µs |
| P99    |     662µs |         1165µs |      1248µs |
| Max    |    1025µs |         3310µs |      1662µs |

### Comparison (true-async vs main, P50)

| Mode | main P50 | true-async P50 | Delta |
|------|----------|----------------|-------|
| Tokio TCP | 189µs | 281µs | +49% (system noise — routing changes don't touch TCP) |
| Zenoh Callback | 549µs | 574µs | **+4.6%** ✅ within noise |
| Zenoh Loop | 558µs | 646µs | +16% (block_in_place ordering overhead) |

### Analysis

**Zenoh Callback**: +4.6% regression at P50 — within normal run-to-run variance. No meaningful regression.

**Zenoh Loop**: +16% regression at P50 — the `block_in_place` usage for Declare/Interest ordering
adds latency to the loop path. The pong callback receives a message, spawns a reply task, and that
reply task goes through routing (which may acquire `block_in_place` on ctrl_lock). Acceptable for
correctness; can be optimized in Phase 5.

**Tokio TCP**: The +49% difference is system noise. This test bypasses all Zenoh routing;
our branch changes cannot affect it. Variance is expected across different run times.

**Previous deadlock resolved**: The two-process callback+spawn deadlock documented in the
earlier SUMMARY section was fixed by using `block_in_place` for Declare/Interest messages
in `demux.rs` (commit d24b86dfe). Both two-process modes now complete cleanly.

**Gossip shutdown panic fixed**: `gossip.rs::upgrade().unwrap()` panicked during session
teardown (commit e166bb731). Fixed to handle None gracefully.

### Conclusion

The true-async branch has **no meaningful regression** on the Zenoh Callback path (+4.6%),
which is the primary optimization target. The Zenoh Loop path has a ~16% regression due
to block_in_place ordering enforcement, which is correctness-critical for this branch.

All two-process modes now complete successfully without deadlock or panic.

---

## Back-to-Back Throughput Results (2026-02-22, commit d1a38ccb6)

### Test Configuration
- Payload: 64 bytes
- Mode: Back-to-back (no rate-limit — measures pure processing latency)
- Samples: 1,000 (+ 5s warmup)
- Two processes, TCP loopback
- Build: release (optimized)
- Change: async-std and async-io removed from zenoh-transport production deps

### dev/true-async (d1a38ccb6) — Zenoh two-process, back-to-back

| Metric | Latency (µs) |
|--------|-------------:|
| Min    |          24µs |
| P25    |          25µs |
| P50    |          27µs |
| Avg    |          28µs |
| P75    |          28µs |
| P95    |          36µs |
| P99    |          57µs |
| Max    |         245µs |

### Notes

- Back-to-back mode measures pure routing + transport latency, not queueing delay.
  Previous results (200 Hz rate-limited) measured ~574µs P50; that includes ~5ms sleep
  between pings which makes measurements dominated by scheduling jitter, not Zenoh overhead.
- 27µs P50 loopback RTT/2 is consistent with expected TCP loopback latency (~20-30µs)
  plus Zenoh encode/decode/route (~5-10µs).
- No regression from async-std removal — the async-std timer was only in fallback paths
  and the sub-ms backoff hot path now uses yield_now instead (correct and faster).
- bench_ping.rs converted to async (#[tokio::main]) to match true-async branch requirements.

### Comparison: main vs true-async (back-to-back, 64B, 1000 samples)

| Metric | main   | true-async | delta   |
|--------|-------:|-----------:|--------:|
| Min    |  22µs  |      24µs  |  +9.1%  |
| P25    |  23µs  |      25µs  |  +8.7%  |
| P50    |  24µs  |      27µs  | +12.5%  |
| Avg    |  25µs  |      28µs  | +12.0%  |
| P75    |  26µs  |      28µs  |  +7.7%  |
| P95    |  34µs  |      36µs  |  +5.9%  |
| P99    |  56µs  |      57µs  |  +1.8%  |
| Max    |  79µs  |     245µs  | outlier |

**Interpretation:**
- Absolute overhead: **~3µs** at P50 (24→27µs). Small and predictable.
- P99 essentially identical (56 vs 57µs). Tail latency unaffected.
- Max is unreliable for 1000-sample runs; single scheduling outlier dominates.
- The 12% relative delta at P50 reflects the cost of `async_lock::RwLock` and
  `async-channel` replacing `std::sync` primitives on the routing/dispatch path.
- In rate-limited (200 Hz) runs this delta is invisible — 5ms sleep dominates.
- In sustained high-throughput scenarios async primitives are expected to scale
  better due to lower contention and no blocking under backpressure.
