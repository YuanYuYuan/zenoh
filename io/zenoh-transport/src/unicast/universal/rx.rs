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
use std::sync::{Arc, MutexGuard};

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
    MessageHandlerAsync, TransportPeerEventHandler,
};

/*************************************/
/*            TRANSPORT RX           */
/*************************************/
impl TransportUnicastUniversal {
    fn trigger_callback(
        &self,
        callback: &dyn TransportPeerEventHandler,
        #[allow(unused_mut)] // shared-memory feature requires mut
        mut msg: NetworkMessageMut,
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
        callback.handle_message(msg)
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

    fn handle_frame(
        &self,
        frame: FrameReader<ZSlice>,
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

        let mut guard = match frame.reliability {
            Reliability::Reliable => zlock!(c.reliable),
            Reliability::BestEffort => zlock!(c.best_effort),
        };

        if !self.verify_sn("Frame", frame.sn, &mut guard)? {
            // Drop invalid message and continue
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
                )?;
            }
        } else {
            tracing::debug!(
                "Transport: {}. No callback available, dropping messages",
                self.config.zid,
            );
        }

        Ok(())
    }

    fn handle_fragment(
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
            // When shared-memory feature is disabled, msg does not need to be mutable
            if let Some(mut msg) = guard.defrag.defragment() {
                let callback = self.callback.load_full();
                if let Some(callback) = callback.as_deref() {
                    return self.trigger_callback(
                        callback.as_ref(),
                        msg.as_mut(),
                        #[cfg(feature = "stats")]
                        stats,
                    );
                } else {
                    tracing::debug!(
                        "Transport: {}. No callback available, dropping messages: {:?}",
                        self.config.zid,
                        msg
                    );
                }
            } else {
                tracing::trace!("Transport: {}. Defragmentation error.", self.config.zid);
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

    // ─────────────────────────────────────────────────────────
    // Async batch processing (single-driver / non-uring path)
    // ─────────────────────────────────────────────────────────

    /// Decode a frame and call `handler.on_message()` for each contained
    /// network message.  The SN mutex is released before any `.await`.
    async fn handle_frame_with_handler(
        &self,
        frame: FrameReader<'_, ZSlice>,
        _link: &Link,
        handler: &Arc<dyn MessageHandlerAsync>,
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

        // SN verification — hold mutex only for this check, drop before await.
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

        for mut msg in frame {
            #[cfg(feature = "shared-memory")]
            if let Some(shm_context) = &self.shm_context {
                if let Err(e) =
                    crate::shm::map_zmsg_to_shmbuf(msg.as_mut(), &shm_context.shm_reader)
                {
                    tracing::debug!("Error receiving SHM buffer: {e}");
                    continue;
                }
            }
            handler.on_message(msg.as_mut()).await?;
        }
        Ok(())
    }

    /// Reassemble a fragment and, once complete, call `handler.on_message()`.
    /// The defrag mutex is released before any `.await`.
    async fn handle_fragment_with_handler(
        &self,
        fragment: Fragment,
        _link: &Link,
        handler: &Arc<dyn MessageHandlerAsync>,
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

        // Acquire the SN/defrag mutex only for synchronous work; release before await.
        let maybe_msg = {
            let mut guard = match reliability {
                Reliability::Reliable => zlock!(c.reliable),
                Reliability::BestEffort => zlock!(c.best_effort),
            };

            if !self.verify_sn("Fragment", sn, &mut guard)? {
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
                tracing::trace!("{}", e);
                return Ok(());
            }
            if !more {
                guard.defrag.defragment()
            } else {
                None
            }
            // guard dropped here, before any await
        };

        if let Some(mut msg) = maybe_msg {
            #[cfg(feature = "shared-memory")]
            if let Some(shm_context) = &self.shm_context {
                if let Err(e) =
                    crate::shm::map_zmsg_to_shmbuf(msg.as_mut(), &shm_context.shm_reader)
                {
                    tracing::debug!("Error receiving SHM buffer: {e}");
                    return Ok(());
                }
            }
            handler.on_message(msg.as_mut()).await?;
        } else if !more {
            tracing::trace!("Transport: {}. Defragmentation error.", self.config.zid);
        }

        Ok(())
    }

    /// Decode an entire received batch and call `handler.on_message()` for each
    /// network message.  Equivalent to [`read_messages`] but fully async: SN
    /// mutexes are released before every `.await`, so no blocking occurs.
    ///
    /// Used by the single-task RX driver (non-uring path).
    pub(crate) async fn handle_batch_with_handler(
        &self,
        mut batch: RBatch,
        link: &Link,
        handler: &Arc<dyn MessageHandlerAsync>,
    ) -> ZResult<()> {
        while !batch.is_empty() {
            if let Ok(frame) = batch.decode() {
                tracing::trace!("Received: {:?}", frame);
                self.handle_frame_with_handler(frame, link, handler).await?;
                continue;
            }
            let msg: TransportMessage = batch
                .decode()
                .map_err(|_| zerror!("{}: decoding error", link))?;

            tracing::trace!("Received: {:?}", msg);

            match msg.body {
                TransportBody::Frame(_) => unreachable!(),
                TransportBody::Fragment(fragment) => {
                    self.handle_fragment_with_handler(fragment, link, handler)
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

    pub(super) fn read_messages(
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
                )?;
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
                TransportBody::Fragment(fragment) => self.handle_fragment(
                    fragment,
                    #[cfg(feature = "stats")]
                    stats,
                )?,
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

        // Process the received message

        Ok(())
    }
}
