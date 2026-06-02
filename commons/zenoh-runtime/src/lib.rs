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
use std::{future::Future, ops::Deref, sync::OnceLock};

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

/// A guard that can be kept to signal intent to clean up at shutdown.
/// With the single shared runtime, cleanup happens automatically at process exit.
pub struct ZRuntimePoolGuard;

impl Drop for ZRuntimePoolGuard {
    fn drop(&mut self) {
        // No-op: the shared runtime shuts down when the process exits.
    }
}
