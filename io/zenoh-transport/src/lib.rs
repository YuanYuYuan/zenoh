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

//! ⚠️ WARNING ⚠️
//!
//! This crate is intended for Zenoh's internal use.
//!
//! [Click here for Zenoh's documentation](https://docs.rs/zenoh/latest/zenoh)
pub mod common;
pub mod manager;
pub mod multicast;
pub mod unicast;

#[cfg(feature = "shared-memory")]
pub mod shm;
#[cfg(feature = "shared-memory")]
mod shm_context;
#[cfg(feature = "uring")]
mod uring;

use std::{any::Any, sync::Arc, time::Duration};

pub use manager::*;
use async_trait::async_trait;
use serde::Serialize;
use tokio_util::sync::CancellationToken;
use zenoh_link::Link;
use zenoh_protocol::{
    core::{RegionName, WhatAmI, ZenohIdProto},
    network::NetworkMessageMut,
};
use zenoh_result::{zerror, ZResult};
use zenoh_sync::RecyclingObjectPool;

use crate::{
    multicast::TransportMulticast,
    unicast::{link::TransportLinkUnicastRx, TransportUnicast},
    unicast::universal::transport::TransportUnicastUniversal,
};

/*************************************/
/*            TRANSPORT              */
/*************************************/
/// Async handler for decoded network messages, used by the single-task RX driver.
///
/// Implemented by `DeMux` in the `zenoh` crate so that routing happens inline
/// within the driver task, eliminating the inter-task handoff.
#[async_trait]
pub trait MessageHandlerAsync: Send + Sync {
    async fn on_message(&self, msg: NetworkMessageMut<'_>) -> ZResult<()>;
}

/// Opaque handle passed to [`TransportPeerEventHandler::take_over_rx`].
///
/// The implementation receives this and passes it to [`run_unicast_rx_driver`].
/// All internal fields are crate-private; external code treats it as an opaque token.
pub struct RxHandle {
    pub(crate) link_rx: TransportLinkUnicastRx,
    pub(crate) transport: TransportUnicastUniversal,
    pub(crate) token: CancellationToken,
    pub(crate) lease: Duration,
    pub(crate) rx_buffer_size: usize,
}

/// Run the single-task RX driver.
///
/// Loops: `recv_batch()` → decode frames/fragments (SN check, defrag) →
/// `handler.on_message(msg).await` for each decoded message.
///
/// On timeout or cancellation the loop exits cleanly.
/// On decode or I/O error, `del_link` is called on the transport to tear down
/// the link before the function returns.
///
/// Spawn on `ZRuntime::Net` from `TransportPeerEventHandler::take_over_rx`.
pub async fn run_unicast_rx_driver(handle: RxHandle, handler: Arc<dyn MessageHandlerAsync>) {
    let RxHandle { mut link_rx, transport, token, lease, rx_buffer_size } = handle;

    let mtu = link_rx.config.batch.mtu as usize;
    let n = (rx_buffer_size / mtu).max(1);
    let pool = RecyclingObjectPool::new(n, || vec![0_u8; mtu].into_boxed_slice());
    let link = Link::new_unicast(
        &link_rx.link,
        link_rx.config.priorities.clone(),
        link_rx.config.reliability,
    );

    let result: ZResult<()> = async {
        loop {
            tokio::select! {
                result = tokio::time::timeout(
                    lease,
                    link_rx.recv_batch(|| pool.try_take().unwrap_or_else(|| pool.alloc()))
                ) => {
                    let batch = result
                        .map_err(|_| zerror!("{}: link lease expired after {}ms", link, lease.as_millis()))??;
                    transport.handle_batch_with_handler(batch, &link, &handler).await?;
                }
                _ = token.cancelled() => break,
            }
        }
        Ok(())
    }
    .await;

    if let Err(e) = result {
        tracing::debug!("RX driver exited with error: {}", e);
        // Tear down only this link (not the whole transport session).
        zenoh_runtime::ZRuntime::Net
            .spawn(async move { transport.del_link(link).await });
    }
}

pub trait TransportEventHandler: Send + Sync {
    fn new_unicast(
        &self,
        peer: TransportPeer,
        transport: TransportUnicast,
    ) -> ZResult<Arc<dyn TransportPeerEventHandler>>;

    fn new_multicast(
        &self,
        _transport: TransportMulticast,
    ) -> ZResult<Arc<dyn TransportMulticastEventHandler>>;
}

#[derive(Debug, Default)]
pub struct DummyTransportEventHandler;

impl TransportEventHandler for DummyTransportEventHandler {
    fn new_unicast(
        &self,
        _peer: TransportPeer,
        _transport: TransportUnicast,
    ) -> ZResult<Arc<dyn TransportPeerEventHandler>> {
        Ok(Arc::new(DummyTransportPeerEventHandler))
    }

    fn new_multicast(
        &self,
        _transport: TransportMulticast,
    ) -> ZResult<Arc<dyn TransportMulticastEventHandler>> {
        Ok(Arc::new(DummyTransportMulticastEventHandler))
    }
}

/*************************************/
/*            MULTICAST              */
/*************************************/
pub trait TransportMulticastEventHandler: Send + Sync {
    fn new_peer(&self, peer: TransportPeer) -> ZResult<Arc<dyn TransportPeerEventHandler>>;
    fn closed(&self);
    fn as_any(&self) -> &dyn Any;
}

// Define an empty TransportCallback for the listener transport
#[derive(Debug, Default)]
pub struct DummyTransportMulticastEventHandler;

impl TransportMulticastEventHandler for DummyTransportMulticastEventHandler {
    fn new_peer(&self, _peer: TransportPeer) -> ZResult<Arc<dyn TransportPeerEventHandler>> {
        Ok(Arc::new(DummyTransportPeerEventHandler))
    }
    fn closed(&self) {}
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/*************************************/
/*             CALLBACK              */
/*************************************/
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
#[serde(rename = "Transport")]
pub struct TransportPeer {
    pub zid: ZenohIdProto,
    pub whatami: WhatAmI,
    pub is_qos: bool,
    #[serde(skip)]
    pub links: Vec<Link>,
    #[cfg(feature = "shared-memory")]
    pub is_shm: bool,
    pub region_name: Option<RegionName>,
}

pub trait TransportPeerEventHandler: Send + Sync {
    fn handle_message(&self, msg: NetworkMessageMut) -> ZResult<()>;
    fn new_link(&self, src: Link);
    fn del_link(&self, link: Link);
    fn closed(&self);
    fn as_any(&self) -> &dyn Any;
    /// Called once from within the per-connection RX runtime's async context,
    /// just before the RX read loop starts.  Implementors that maintain a
    /// consumer task (e.g. `DeMux`) can use this hook to spawn it from within
    /// a specific runtime context if needed.
    ///
    /// The default implementation is a no-op (correct for sync handlers).
    fn rx_runtime_ready(&self) {}

    /// Optional pull-based RX takeover (non-uring path only).
    ///
    /// When this returns `true` the caller skips spawning the built-in RX task.
    /// The implementation is responsible for consuming `handle` by passing it to
    /// [`run_unicast_rx_driver`] (typically spawned on `ZRuntime::Net`).
    ///
    /// The default returns `false` (built-in RX task is used).
    #[cfg(not(feature = "uring"))]
    fn take_over_rx(&self, _handle: RxHandle) -> bool {
        false
    }
}

// Define an empty TransportCallback for the listener transport
#[derive(Debug, Default)]
pub struct DummyTransportPeerEventHandler;

impl TransportPeerEventHandler for DummyTransportPeerEventHandler {
    fn handle_message(&self, _message: NetworkMessageMut) -> ZResult<()> {
        Ok(())
    }

    fn new_link(&self, _link: Link) {}
    fn del_link(&self, _link: Link) {}
    fn closed(&self) {}

    fn as_any(&self) -> &dyn Any {
        self
    }
}
