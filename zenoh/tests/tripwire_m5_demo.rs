//! Ad hoc verification (not part of the crate's own test suite): does
//! wiring `lock-tripwire` into the real M5 self-join mechanism catch it on
//! unpatched `main`, and stay clean once the per-entity fix in this PR is
//! applied?
//!
//! Two real sites are instrumented in `zenoh/src`, not simulated here:
//! `Callback::call`/`call_with_message` marks the on-drop permit implicitly
//! held for the duration of the call (`hold_resource_at`), and
//! `SyncGroup::wait`'s `block_in_place(acquire_many(..))` is the guarded
//! call-out (`invoke_user_callback!`).

#![cfg(any(feature = "unstable", feature = "internal"))]

use std::{
    panic::AssertUnwindSafe,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc, Mutex,
    },
    time::Duration,
};

use zenoh::{sample::Sample, Wait};

const WAIT: Duration = Duration::from_secs(10);
const DELIVERY_GRACE: Duration = Duration::from_millis(500);

fn isolated_config() -> zenoh::Config {
    let mut config = zenoh::Config::default();
    config
        .insert_json5("scouting/multicast/enabled", "false")
        .unwrap();
    config
        .insert_json5("scouting/gossip/enabled", "false")
        .unwrap();
    config.insert_json5("listen/endpoints", "[]").unwrap();
    config
}

#[derive(Debug)]
enum Outcome {
    Completed,
    Deadlocked,
    Panicked(String),
}

fn run_scenario(body: impl FnOnce(Arc<AtomicBool>) + Send + 'static) -> (Outcome, Arc<AtomicBool>) {
    let entered = Arc::new(AtomicBool::new(false));
    let entered_body = entered.clone();
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| body(entered_body)));
        let _ = tx.send(result.map_err(|payload| {
            payload
                .downcast_ref::<&str>()
                .map(|s| (*s).to_owned())
                .or_else(|| payload.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "<non-string panic payload>".to_owned())
        }));
    });
    let outcome = match rx.recv_timeout(WAIT) {
        Ok(Ok(())) => Outcome::Completed,
        Ok(Err(msg)) => Outcome::Panicked(msg),
        Err(mpsc::RecvTimeoutError::Timeout) => Outcome::Deadlocked,
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            Outcome::Panicked("<thread aborted without sending>".to_owned())
        }
    };
    (outcome, entered)
}

type Slot<T> = Arc<Mutex<Option<T>>>;

fn take<T>(slot: &Slot<T>) -> Option<T> {
    slot.lock().unwrap().take()
}

/// The self-join: a subscriber undeclared with `wait_callbacks()` from
/// inside its own sample callback. Dexory's production shape, one process.
#[test]
fn self_join_subscriber_undeclare_from_own_callback() {
    let (outcome, entered) = run_scenario(|entered| {
        let session = zenoh::open(isolated_config()).wait().unwrap();
        let slot: Slot<zenoh::pubsub::Subscriber<()>> = Arc::new(Mutex::new(None));

        let slot_cb = slot.clone();
        let sub = session
            .declare_subscriber("test/tripwire_m5_demo/self_join")
            .callback(move |_s: Sample| {
                entered.store(true, Ordering::SeqCst);
                if let Some(sub) = take(&slot_cb) {
                    let _ = sub.undeclare().wait_callbacks().wait();
                }
            })
            .wait()
            .unwrap();
        *slot.lock().unwrap() = Some(sub);

        session
            .put("test/tripwire_m5_demo/self_join", "trigger")
            .wait()
            .unwrap();
    });

    std::thread::sleep(DELIVERY_GRACE);
    assert!(
        entered.load(Ordering::SeqCst),
        "the callback never ran, so this scenario proves nothing"
    );
    println!("self_join outcome: {outcome:?}");
    match outcome {
        Outcome::Panicked(ref msg) if msg.contains("re-entrancy hazard") => {
            println!("CAUGHT by lock-tripwire, as expected pre-fix: {msg}");
        }
        Outcome::Completed => {
            println!("COMPLETED cleanly, as expected post-fix (#24)");
        }
        other => panic!("unexpected outcome: {other:?}"),
    }
}

/// Control: ordinary delivery, no self-undeclare anywhere in the callback.
/// Must never panic on either branch — proves the instrumentation only
/// fires on the real hazard, not on every callback invocation.
#[test]
fn control_ordinary_delivery_never_panics() {
    let (outcome, entered) = run_scenario(|entered| {
        let session = zenoh::open(isolated_config()).wait().unwrap();
        let _sub = session
            .declare_subscriber("test/tripwire_m5_demo/control")
            .callback(move |_s: Sample| {
                entered.store(true, Ordering::SeqCst);
            })
            .wait()
            .unwrap();

        session
            .put("test/tripwire_m5_demo/control", "trigger")
            .wait()
            .unwrap();
        std::thread::sleep(DELIVERY_GRACE);
    });

    assert!(entered.load(Ordering::SeqCst), "the callback never ran");
    println!("control outcome: {outcome:?}");
    assert!(
        matches!(outcome, Outcome::Completed),
        "control must complete cleanly, got {outcome:?}"
    );
}
