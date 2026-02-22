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
use std::sync::MutexGuard;

use zenoh_buffers::ZSlice;
use zenoh_codec::transport::frame::FrameReader;
use zenoh_core::zlock;
use zenoh_link::Link;
use zenoh_protocol::{
    core::{Priority, Reliability},
    network::NetworkMessageMut,
    transport::{Close, Fragment, KeepAlive, TransportBody, TransportMessage, TransportSn},
};
use zenoh_result::{bail, zerror, ZResult};

use super::transport::TransportUnicastUniversal;
use crate::{
    common::{
        batch::{Decode, RBatch},
        priority::TransportChannelRx,
    },
    unicast::transport_unicast_inner::TransportUnicastTrait,
    TransportPeerEventHandler,
};

/*************************************/
/*            TRANSPORT RX           */
/*************************************/
impl TransportUnicastUniversal {
    async fn trigger_callback(
        &self,
        callback: &dyn TransportPeerEventHandler,
        #[allow(unused_mut)] // shared-memory feature requires mut
        mut msg: NetworkMessageMut<'_>,
        #[cfg(feature = "stats")] stats: &zenoh_stats::LinkStats,
    ) -> ZResult<()> {
        #[cfg(feature = "stats")]
        stats.inc_network_message(
            zenoh_stats::Rx,
            zenoh_protocol::network::NetworkMessageExt::as_ref(&msg),
        );
        #[cfg(feature = "shared-memory")]
        {
            if let Some(shm_context) = &self.shm_context {
                if let Err(e) =
                    crate::shm::map_zmsg_to_shmbuf(msg.as_mut(), &shm_context.shm_reader)
                {
                    tracing::debug!("Error receiving SHM buffer: {e}");
                    return Ok(());
                }
            }
        }
        callback.handle_message_async(msg).await
    }

    fn handle_close(&self, link: &Link, _reason: u8, session: bool) -> ZResult<()> {
        // Delete and clean up
        let c_transport = self.clone();
        let c_link = link.clone();
        // Spawn a task to avoid a deadlock waiting for this same task
        // to finish in the link close() joining the rx handle
        tokio::spawn(async move {
            if session {
                let _ = c_transport.delete().await;
            } else {
                let _ = c_transport.del_link(c_link).await;
            }
        });

        Ok(())
    }

    async fn handle_frame(
        &self,
        frame: FrameReader<'_, ZSlice>,
        #[cfg(feature = "stats")] stats: &zenoh_stats::LinkStats,
    ) -> ZResult<()> {
        let priority = frame.ext_qos.priority();
        let c = if self.is_qos() {
            &self.priority_rx[priority as usize]
        } else if priority == Priority::DEFAULT {
            &self.priority_rx[0]
        } else {
            bail!(
                "Transport: {}. Unknown priority: {:?}.",
                self.config.zid,
                priority
            );
        };

        // Verify SN under a scoped guard so the MutexGuard is dropped before any .await.
        let sn_ok = {
            let mut guard = match frame.reliability {
                Reliability::Reliable => zlock!(c.reliable),
                Reliability::BestEffort => zlock!(c.best_effort),
            };
            self.verify_sn("Frame", frame.sn, &mut guard)?
        };

        if !sn_ok {
            return Ok(());
        }

        let callback = self.callback.load_full();
        if let Some(callback) = callback.as_deref() {
            for mut msg in frame {
                self.trigger_callback(
                    callback.as_ref(),
                    msg.as_mut(),
                    #[cfg(feature = "stats")]
                    stats,
                )
                .await?;
            }
        } else {
            tracing::debug!(
                "Transport: {}. No callback available, dropping messages",
                self.config.zid,
            );
        }

        Ok(())
    }

    async fn handle_fragment(
        &self,
        fragment: Fragment,
        #[cfg(feature = "stats")] stats: &zenoh_stats::LinkStats,
    ) -> ZResult<()> {
        let Fragment {
            reliability,
            more,
            sn,
            ext_qos: qos,
            ext_first,
            ext_drop,
            payload,
        } = fragment;

        let c = if self.is_qos() {
            &self.priority_rx[qos.priority() as usize]
        } else if qos.priority() == Priority::DEFAULT {
            &self.priority_rx[0]
        } else {
            bail!(
                "Transport: {}. Unknown priority: {:?}.",
                self.config.zid,
                qos.priority()
            );
        };

        // Defragment under the guard, then release before awaiting the callback.
        let defragmented = {
            let mut guard = match reliability {
                Reliability::Reliable => zlock!(c.reliable),
                Reliability::BestEffort => zlock!(c.best_effort),
            };

            if !self.verify_sn("Fragment", sn, &mut guard)? {
                // Drop invalid message and continue
                return Ok(());
            }
            if self.config.patch.has_fragmentation_markers() {
                if ext_first.is_some() {
                    guard.defrag.clear();
                } else if guard.defrag.is_empty() {
                    tracing::trace!(
                        "Transport: {}. First fragment received without start marker.",
                        self.manager.config.zid,
                    );
                    return Ok(());
                }
                if ext_drop.is_some() {
                    guard.defrag.clear();
                    return Ok(());
                }
            }
            if guard.defrag.is_empty() {
                let _ = guard.defrag.sync(sn);
            }
            if let Err(e) = guard.defrag.push(sn, payload) {
                // Defrag errors don't close transport
                tracing::trace!("{}", e);
                return Ok(());
            }
            if !more {
                guard.defrag.defragment()
            } else {
                None
            }
            // guard dropped here
        };

        if let Some(mut msg) = defragmented {
            let callback = self.callback.load_full();
            if let Some(callback) = callback.as_deref() {
                return self
                    .trigger_callback(
                        callback.as_ref(),
                        msg.as_mut(),
                        #[cfg(feature = "stats")]
                        stats,
                    )
                    .await;
            } else {
                tracing::debug!(
                    "Transport: {}. No callback available, dropping messages: {:?}",
                    self.config.zid,
                    msg
                );
            }
        }

        Ok(())
    }

    fn verify_sn(
        &self,
        message_type: &str,
        sn: TransportSn,
        guard: &mut MutexGuard<'_, TransportChannelRx>,
    ) -> ZResult<bool> {
        let precedes = guard.sn.roll(sn)?;
        if !precedes {
            tracing::trace!(
                "Transport: {}. {} with invalid SN dropped: {}. Expected: {}.",
                self.config.zid,
                message_type,
                sn,
                guard.sn.next()
            );
            return Ok(false);
        }

        Ok(true)
    }

    /// Async RX path — used by the standard (non-uring) rx_task.
    /// Routes each message inline (no per-face consumer task) via handle_message_async.
    pub(super) async fn read_messages_async(
        &self,
        mut batch: RBatch,
        link: &Link,
        #[cfg(feature = "stats")] stats: &zenoh_stats::LinkStats,
    ) -> ZResult<()> {
        while !batch.is_empty() {
            if let Ok(frame) = batch.decode() {
                tracing::trace!("Received: {:?}", frame);
                #[cfg(feature = "stats")]
                {
                    stats.inc_transport_message(zenoh_stats::Rx, 1);
                }
                self.handle_frame(
                    frame,
                    #[cfg(feature = "stats")]
                    stats,
                )
                .await?;
                continue;
            }
            let msg: TransportMessage = batch
                .decode()
                .map_err(|_| zerror!("{}: decoding error", link))?;

            tracing::trace!("Received: {:?}", msg);

            #[cfg(feature = "stats")]
            {
                stats.inc_transport_message(zenoh_stats::Rx, 1);
            }

            match msg.body {
                TransportBody::Frame(_) => unreachable!(),
                TransportBody::Fragment(fragment) => {
                    self.handle_fragment(
                        fragment,
                        #[cfg(feature = "stats")]
                        stats,
                    )
                    .await?
                }
                TransportBody::Close(Close { reason, session }) => {
                    self.handle_close(link, reason, session)?
                }
                TransportBody::KeepAlive(KeepAlive { .. }) => {}
                _ => {
                    tracing::debug!(
                        "Transport: {}. Message handling not implemented: {:?}",
                        self.config.zid,
                        msg
                    );
                }
            }
        }

        Ok(())
    }

    /// Sync RX path — kept for the io_uring path where the callback is called
    /// from a completion handler and cannot directly await.
    pub(super) fn read_messages(
        &self,
        mut batch: RBatch,
        link: &Link,
        #[cfg(feature = "stats")] stats: &zenoh_stats::LinkStats,
    ) -> ZResult<()> {
        while !batch.is_empty() {
            if let Ok(frame) = batch.decode() {
                let frame: FrameReader<'_, ZSlice> = frame;
                tracing::trace!("Received: {:?}", frame);
                #[cfg(feature = "stats")]
                {
                    stats.inc_transport_message(zenoh_stats::Rx, 1);
                }
                let priority = frame.ext_qos.priority();
                let c = if self.is_qos() {
                    &self.priority_rx[priority as usize]
                } else if priority == Priority::DEFAULT {
                    &self.priority_rx[0]
                } else {
                    bail!(
                        "Transport: {}. Unknown priority: {:?}.",
                        self.config.zid,
                        priority
                    );
                };
                let mut guard = match frame.reliability {
                    Reliability::Reliable => zlock!(c.reliable),
                    Reliability::BestEffort => zlock!(c.best_effort),
                };
                if !self.verify_sn("Frame", frame.sn, &mut guard)? {
                    continue;
                }
                let callback = self.callback.load_full();
                if let Some(callback) = callback.as_deref() {
                    for mut msg in frame {
                        #[cfg(feature = "stats")]
                        stats.inc_network_message(
                            zenoh_stats::Rx,
                            zenoh_protocol::network::NetworkMessageExt::as_ref(&msg.as_ref()),
                        );
                        callback.handle_message(msg.as_mut())?;
                    }
                }
                continue;
            }
            let msg: TransportMessage = batch
                .decode()
                .map_err(|_| zerror!("{}: decoding error", link))?;

            tracing::trace!("Received: {:?}", msg);

            #[cfg(feature = "stats")]
            {
                stats.inc_transport_message(zenoh_stats::Rx, 1);
            }

            match msg.body {
                TransportBody::Frame(_) => unreachable!(),
                TransportBody::Fragment(_) => {
                    // Fragment handling via sync path is complex; skip for uring.
                    // The uring path primarily handles regular frames.
                    tracing::debug!("Transport: {}. Fragment on uring path — not yet supported", self.config.zid);
                }
                TransportBody::Close(Close { reason, session }) => {
                    self.handle_close(link, reason, session)?
                }
                TransportBody::KeepAlive(KeepAlive { .. }) => {}
                _ => {
                    tracing::debug!(
                        "Transport: {}. Message handling not implemented: {:?}",
                        self.config.zid,
                        msg
                    );
                }
            }
        }

        Ok(())
    }
}
