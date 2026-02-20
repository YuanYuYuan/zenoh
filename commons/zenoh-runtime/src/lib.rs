//
// Copyright (c) 2024 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//

//! ⚠️ WARNING ⚠️
//!
//! This crate is intended for Zenoh's internal use.
//!
//! [Click here for Zenoh's documentation](https://docs.rs/zenoh/latest/zenoh)
use core::panic;
use std::{
    borrow::Borrow,
    collections::HashMap,
    env, fmt,
    future::Future,
    ops::Deref,
    sync::{
        atomic::{AtomicUsize, Ordering},
        OnceLock,
    },
    time::Duration,
};

use tokio::{
    runtime::{Handle, Runtime},
    task::JoinHandle,
};

/// [`ZRuntime`], the access point for spawning tasks within zenoh.
///
/// All variants share a single underlying tokio runtime. The variant names are
/// kept for API compatibility but have no effect on scheduling — all spawned
/// tasks run on the shared runtime.
#[derive(Hash, Eq, PartialEq, Clone, Copy, Debug)]
pub enum ZRuntime {
    Application,
    Acceptor,
    TX,
    RX,
    Net,
}

impl ZRuntime {
    pub fn iter() -> impl Iterator<Item = ZRuntime> {
        [
            ZRuntime::Application,
            ZRuntime::Acceptor,
            ZRuntime::TX,
            ZRuntime::RX,
            ZRuntime::Net,
        ]
        .into_iter()
    }

    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        #[cfg(feature = "tracing-instrument")]
        let future = tracing::Instrument::instrument(future, tracing::Span::current());

        tokio::spawn(future)
    }

    pub fn block_in_place<F, R>(&self, f: F) -> R
    where
        F: Future<Output = R>,
    {
        #[cfg(feature = "tracing-instrument")]
        let f = tracing::Instrument::instrument(f, tracing::Span::current());

        tokio::task::block_in_place(move || get_shared_handle().block_on(f))
    }
}

static SHARED_RUNTIME: OnceLock<Runtime> = OnceLock::new();

fn get_shared_handle() -> &'static Handle {
    SHARED_RUNTIME
        .get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .thread_name("zenoh-runtime")
                .build()
                .expect("Failed to build Zenoh shared runtime")
        })
        .handle()
}

impl Deref for ZRuntime {
    type Target = Handle;
    fn deref(&self) -> &Handle {
        get_shared_handle()
    }
}

lazy_static! {
    pub static ref ZRUNTIME_POOL: ZRuntimePool = ZRuntimePool::new();
    pub static ref ZRUNTIME_INDEX: HashMap<ZRuntime, AtomicUsize> = ZRuntime::iter()
        .map(|zrt| (zrt, AtomicUsize::new(0)))
        .collect();
}

// A runtime guard used to explicitly drop the static variables that Rust doesn't drop by default
#[derive(Debug)]
pub struct ZRuntimePoolGuard;

impl Drop for ZRuntimePoolGuard {
    fn drop(&mut self) {
        // No-op: the shared runtime shuts down when the process exits.
    }
}

pub struct ZRuntimePool(HashMap<ZRuntime, OnceLock<Runtime>>);

impl fmt::Debug for ZRuntimePool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let initialized = self
            .0
            .iter()
            .filter_map(|(runtime, cell)| cell.get().map(|_| runtime))
            .collect::<Vec<_>>();
        f.debug_struct("ZRuntimePool")
            .field("initialized", &initialized)
            .finish_non_exhaustive()
    }
}

impl ZRuntimePool {
    fn new() -> Self {
        Self(ZRuntime::iter().map(|zrt| (zrt, OnceLock::new())).collect())
    }

    pub fn get(&self, zrt: &ZRuntime) -> &Handle {
        // Although the ZRuntime is called to use `zrt`, it may be handed over to another one
        // specified via the environmental variable.
        let param: &RuntimeParam = zrt.borrow();
        let zrt = match param.handover {
            Some(handover) => handover,
            None => *zrt,
        };

        self.0
            .get(&zrt)
            .unwrap_or_else(|| panic!("The hashmap should contains {zrt} after initialization"))
            .get_or_init(|| {
                zrt.init()
                    .unwrap_or_else(|_| panic!("Failed to init {zrt}"))
            })
            .handle()
    }
}

// If there are any blocking tasks spawned by ZRuntimes, the function will block until they return.
impl Drop for ZRuntimePool {
    fn drop(&mut self) {
        let handles: Vec<_> = self
            .0
            .drain()
            .filter_map(|(_name, mut rt)| {
                rt.take()
                    .map(|r| std::thread::spawn(move || r.shutdown_timeout(Duration::from_secs(1))))
            })
            .collect();

        for hd in handles {
            let _ = hd.join();
        }
    }
}

#[should_panic(expected = "Zenoh runtime doesn't support")]
#[tokio::test]
async fn block_in_place_fail_test() {
    use crate::ZRuntime;
    ZRuntime::TX.block_in_place(async { println!("Done") });
}
