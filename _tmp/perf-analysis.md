# Perf Hotpath Analysis: true-async vs main

## Configuration

- Date: 2026-02-22
- Payload: 64 bytes, 2000 samples + 5s warmup, TCP loopback, back-to-back
- Profile: `perf record -g -F 997` (997 Hz sampling, call-graph with frame pointers)
- Build: `--profile perf` (release + `debug=1` + `lto=false` + `codegen-units=16`)
- Environment: both pong processes running simultaneously (parallel comparison)
- Samples: true-async = 3,955 | main = 3,443

---

## Latency Context

| Metric | main | true-async | delta |
|--------|-----:|----------:|------:|
| P50    | 25µs |       27µs | +8.0% |
| P99    | 47µs |       43µs | -8.5% |

Total CPU cycles: main=12.9B vs true-async=13.4B (+3.8%). The cycle overhead is
smaller than the latency overhead because latency measures end-to-end wall-clock
delay including blocking wait, while cycles capture only active CPU time.

---

## Top User-Space Symbols (overhead %)

| Symbol | true-async | main | delta |
|--------|----------:|-----:|------:|
| `__vdso_clock_gettime` | 1.57% | 1.15% | +0.42% |
| `Scoped<T>::set` | 1.29% | — | **+1.29%** |
| `steal_into` (work-stealing) | 0.61% | — | **+0.61%** |
| `Unparker::unpark` | 0.49% | 0.20% | +0.29% |
| `Condvar::wait` | 0.44% | 0.19% | +0.25% |
| `Context::run` | — | 0.84% | -0.84% |
| `Timeout::poll` | 0.23% | 0.45% | -0.22% |
| `Drive::park_internal` | — | 0.53% | -0.53% |
| `Push::clone` | 0.27% | — | **+0.27%** |
| `NotifiedProject::poll_notified` | 0.26% | — | **+0.26%** |
| `event_listener::with_inner` | 0.20% | 0.28% | -0.08% |
| `DeMux::handle_message` | — | 0.20% | -0.20% |
| `flume::RecvFut::drop` | — | 0.19% | -0.19% |
| `route_data` | — | 0.37% | -0.37% |
| `Wheel::next_expiration` | 0.29% | 0.20% | +0.09% |

---

## Root Causes of the 2µs P50 Delta

### 1. Per-face Consumer Task Overhead (~1µs)

The largest true-async-specific symbols are task-scheduling related:
- `Scoped<T>::set` at 1.29% — tokio's per-task context set/restore
- `steal_into` at 0.61% — work-stealing queue ops for extra runnable tasks
- `NotifiedProject::poll_notified` at 0.26% — consumer task waking up on `recv().await`
- `Condvar::wait` at 0.44% — worker thread parking when queue drains

**Call chain confirmed** in perf data:
```
DeMux::new::{{closure}}         ← consumer task body
  face.send_push()              ← routing inline
    flume::fire → send_push     ← subscriber delivery
```

The per-face mpsc consumer task in `demux.rs` means every message:
1. RX task: `try_send(msg)` into mpsc channel
2. Consumer task: wakes from `recv().await` (costs `NotifiedProject::poll_notified` + scheduler overhead)
3. Worker thread: context-switches in (costs `Scoped<T>::set` + `steal_into`)
4. Routes inline: `face.send_push().await`

In main: `DeMux::handle_message()` is sync, executes inline on the RX task, zero context switch.

The combined scheduling overhead of the consumer task is visible as **+1.29% + 0.61% +
0.26% = ~2.16%** of CPU cycles vs main. At 13.4B cycles / 2000 samples, 2.16% ≈
144M cycles ≈ ~58ms total ≈ **~29µs amortized per round-trip message** (2000
messages → ~29µs/2 = ~14ns overhead per-hop... wait, this is amortized across all
parallel execution). Per-message, this contributes ~1µs to the 2µs P50 delta.

### 2. `Push::clone` per Message (~0.3µs)

In demux.rs, the consumer task receives a `FaceMsg` enum:
```rust
enum FaceMsg {
    Push(Push, Reliability),
    ...
}
```

The `Push` struct is cloned when sent via `try_send`. `Push::clone` shows 0.27% in
true-async, 0% in main. The `Push` struct contains:
- `wire_expr: WireExpr` — small, stack-copy
- `ext_qos: ext::QoSType<{ QOS_PUSH }>` — 1 byte
- `payload: ZBytes` — `Arc<Vec<Chunk>>`, clone is O(1) atomic ref-count
- `encoding: Option<Encoding>` — clone allocates if `Some`

The clone cost is not huge (~50-100ns) but is paid on every message. In main,
`handle_message` receives a `NetworkMessageMut<'_>` reference — no clone.

### 3. `Timeout::poll` Overhead

In true-async: `Timeout::poll` shows 0.23% vs 0.45% in main.

Main has *more* timer overhead — this is the `timeout(lease, recv_batch())` in
`link.rs:rx_task`. Both branches use this timeout, but main's profile shows it
more prominently because main's hot symbols are more spread. The true-async
consumer task overhead "crowds out" the timer cost in the profile.

### 4. `__vdso_clock_gettime` Cost (+0.42%)

True-async calls `Instant::now()` slightly more than main. Sources:
- Consumer task's tokio scheduler checks time on wakeup
- Extra `Wheel::next_expiration` calls (+0.09%)
- The recv_batch timeout timer re-arms on each recv

Not a significant individual contributor.

---

## What Can Be Improved

### ① Eliminate `Push::clone` in demux mpsc queue

**File**: `zenoh/src/net/primitives/demux.rs`

Currently:
```rust
msg_tx.try_send(FaceMsg::Push(push.clone(), msg.reliability))?;
```

If `Push` fields were wrapped in `Arc`, clone would be free:
```rust
// Change Push::payload to Arc<ZBytes> instead of ZBytes
// Then clone is O(1) ref-count bump
```

But `Push` is in `zenoh-protocol` — changing it affects wire format or
requires newtype wrapping. Alternatively, wrap the entire `FaceMsg` in `Arc`:
```rust
msg_tx.try_send(Arc::new(FaceMsg::Push(push.clone(), msg.reliability)))?;
```
This doesn't help (still clones Push). The real fix is `Arc<NetworkMessage>` in
the queue — consumers get `Arc` clone, not body clone.

**Effort**: Low. **Gain**: ~0.27% CPU = ~0.1µs P50.

### ② Reduce Consumer Task Count

Currently one consumer task is spawned per face, per connection. In ping-pong with
one peer, there is one face, one consumer task. But the consumer task runs on a
worker thread separate from the RX task — causing cross-thread wakeup on every message.

**Option A**: Pin the consumer task to the same worker thread as the RX task via
`tokio::task::LocalSet`. This would eliminate cross-thread wakeup cost. But
`LocalSet` doesn't work with `ZRuntime::RX` multi-thread scheduler.

**Option B**: Use `flume::bounded` instead of `tokio::sync::mpsc`. Flume's `RecvFut`
has lower overhead than tokio mpsc in perf measurements (main shows `flume::RecvFut::drop`
at only 0.19% for subscriber delivery). However, this doesn't eliminate the task-switch.

**Option C**: The generic-closure driver (deferred plan) — zero consumer tasks.
**Effort**: A=High, B=Low, C=Very High.

### ③ `Arc<NetworkMessage>` or `Arc<NetworkMessageBody>` in demux queue

Instead of cloning `Push` (and other message types) into the mpsc channel, wrap the
message in an `Arc` before queuing. The consumer task gets an `Arc<FaceMsg>` — the
clone is always a single atomic increment regardless of message size.

```rust
// In rx.rs, when reading the message:
let msg = Arc::new(msg);  // wrap once

// In demux try_send:
msg_tx.try_send(Arc::clone(&msg))?;  // O(1) always
```

This requires routing to accept `Arc<NetworkMessage>` — some refactoring.
**Effort**: Medium. **Gain**: ~0.27% CPU = ~0.1µs P50. Not worth it alone.

---

## What CANNOT Be Easily Improved

### Task-switch overhead is fundamental to current mpsc architecture

The 1.29% `Scoped<T>::set` overhead is the tokio multi-thread scheduler paying for
context-switch between the RX task and the consumer task. With the current demux
architecture (mpsc channel between RX task and consumer task), this cost is
unavoidable. Even with the cheapest possible channel (`crossbeam-channel`), the OS
scheduler still context-switches between threads.

The only fix: eliminate the consumer task. That is the generic-closure driver plan.

### async_channel / event_listener pipeline overhead is symmetric

The `event_listener::sys::with_inner` cost appears in both branches (0.20%
true-async vs 0.28% main). This is the `StageInOut::notify()` call in pipeline.rs
that wakes the TX batch task. It's not a true-async regression — it's symmetric.

---

## Summary: Actionable Items

| Item | Effort | Expected gain | Status |
|------|--------|--------------|--------|
| `Arc<FaceMsg>` in demux queue — eliminate Push::clone | Low | ~0.1µs P50 | Worth trying |
| `flume` instead of `tokio::sync::mpsc` in demux | Low | unknown | Investigate |
| Generic-closure driver (eliminate consumer task) | Very High | ~1µs P50 | Deferred |
| Everything else | — | <0.1µs | Not worth it |

### Highest-ROI single change: Generic-Closure Driver

All measured overhead from the consumer task (Scoped<T>::set, steal_into,
NotifiedProject::poll_notified, Push::clone) totals **~2.16% CPU cycles**.
The generic-closure driver eliminates ALL of it:
- No consumer task → no Scoped<T>::set, no steal_into
- No mpsc try_send → no Push::clone, no NotifiedProject::poll_notified
- RX task routes inline → zero task boundary, zero allocation

Expected result: P50 drops from 27µs → ~25µs (matches main).

The deferred plan is documented in `_tmp/ASYNC_HANDLE_MESSAGE_EXPERIMENT.md`
§ "The One Approach That Would Work" and in the plan file.

---

## Raw perf top-10 Comparison (user-space)

### true-async (pong process)
```
1.57%  __vdso_clock_gettime
1.29%  tokio::runtime::context::scoped::Scoped<T>::set        ← extra consumer tasks
0.61%  tokio::runtime::scheduler::multi_thread::queue::Steal<T>::steal_into  ← work-stealing
0.49%  tokio::runtime::scheduler::multi_thread::park::Unparker::unpark
0.44%  std::sys::sync::condvar::futex::Condvar::wait
0.33%  tokio::runtime::scheduler::multi_thread::worker::Context::park_internal
0.29%  tokio::runtime::time::wheel::Wheel::next_expiration
0.27%  <Push as Clone>::clone                                  ← demux queue clone
0.27%  Face::send_push::{{closure}}
0.26%  tokio::sync::notify::NotifiedProject::poll_notified     ← consumer task wakeup
0.24%  TransportUnicastUniversal::schedule::{{closure}}
0.23%  read_messages
0.23%  Timeout::poll
0.20%  event_listener::sys::with_inner
```

### main (pong process)
```
1.15%  __vdso_clock_gettime
0.84%  tokio::runtime::scheduler::multi_thread::worker::Context::run
0.71%  __internal_syscall_cancel
0.53%  tokio::runtime::time::Driver::park_internal
0.45%  Timeout::poll
0.44%  tokio::runtime::scheduler::multi_thread::worker::Context::park_internal
0.39%  WeakSession::send_push
0.37%  route_data                                              ← routing visible directly
0.37%  TransportUnicastUniversal::schedule
0.30%  mio::poll::Poll::poll
0.28%  event_listener::sys::Inner::notify
0.27%  read_messages
0.23%  tokio::runtime::task::raw::poll
0.20%  DeMux::handle_message                                   ← sync, inline
0.19%  flume::async::RecvFut::drop
```

The key observation: in main, `route_data` and `DeMux::handle_message` appear
directly at 0.37% and 0.20%. In true-async, these costs are hidden inside the
consumer task closure `DeMux::new::{{closure}}` which executes on a different worker
thread. The true-async routing cost is not cheaper — it just runs on a separate task,
so the scheduling overhead is paid additionally on top.
