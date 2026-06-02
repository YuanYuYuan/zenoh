//
// Copyright (c) 2023 ZettaScale Technology
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
mod demux;
mod mux;

use std::any::Any;

use async_trait::async_trait;
pub use demux::*;
pub use mux::*;
use zenoh_protocol::{
    core::Reliability,
    network::{interest::Interest, Declare, Push, Request, Response, ResponseFinal},
};

use super::routing::RoutingContext;

/// Unified async Primitives trait that consolidates the former Primitives and EPrimitives traits.
/// All methods are async and take owned messages to enable parallel sends.
#[async_trait]
pub trait Primitives: Send + Sync {
    /// Send an interest message
    async fn send_interest(&self, msg: Interest) -> bool;

    /// Send a declare message
    async fn send_declare(&self, msg: Declare) -> bool;

    /// Send a push message with specified reliability
    async fn send_push(&self, msg: Push, reliability: Reliability) -> bool;

    /// Send a request message
    async fn send_request(&self, msg: Request) -> bool;

    /// Send a response message
    async fn send_response(&self, msg: Response) -> bool;

    /// Send a response final message
    async fn send_response_final(&self, msg: ResponseFinal) -> bool;

    /// Close the primitives
    async fn close(&self);

    /// Sync fast path for pushing data messages.
    /// Returns `true` if the message was pushed without needing async operations.
    /// Default returns `false` — caller falls back to `send_push().await`.
    fn try_push_sync(&self, _msg: &Push, _reliability: Reliability) -> bool {
        false
    }

    /// Downcast support for accessing concrete types
    fn as_any(&self) -> &dyn std::any::Any;
}

#[derive(Default)]
pub struct DummyPrimitives;

#[async_trait]
impl Primitives for DummyPrimitives {
    async fn send_interest(&self, _msg: Interest) -> bool {
        false
    }

    async fn send_declare(&self, _msg: Declare) -> bool {
        false
    }

    async fn send_push(&self, _msg: Push, _reliability: Reliability) -> bool {
        false
    }

    async fn send_request(&self, _msg: Request) -> bool {
        false
    }

    async fn send_response(&self, _msg: Response) -> bool {
        false
    }

    async fn send_response_final(&self, _msg: ResponseFinal) -> bool {
        false
    }

    async fn close(&self) {}

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Compatibility alias: EPrimitives was merged into Primitives in the true-async refactor.
pub(crate) use Primitives as EPrimitives;
