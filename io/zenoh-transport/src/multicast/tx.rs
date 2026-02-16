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
use zenoh_core::zread;
use zenoh_protocol::network::{NetworkMessageExt, NetworkMessageMut, NetworkMessageRef};
use zenoh_result::ZResult;

use super::transport::TransportMulticastInner;
#[cfg(feature = "shared-memory")]
use crate::shm::map_zmsg_to_partner;

//noinspection ALL
impl TransportMulticastInner {
    async fn schedule_on_link(&self, msg: NetworkMessageRef<'_>) -> ZResult<bool> {
        // Clone pipeline in a separate scope to ensure guard is dropped
        let pipeline_opt = {
            let guard = zread!(self.link);
            guard.as_ref().and_then(|l| l.pipeline.as_ref()).cloned()
        }; // guard is dropped here

        if let Some(pl) = pipeline_opt {
            Ok(pl.push_network_message(msg).await?)
        } else {
            tracing::trace!(
                "Message dropped because the transport has no links: {}",
                msg
            );
            Ok(false)
        }
    }

    #[allow(unused_mut)] // When feature "shared-memory" is not enabled
    #[allow(clippy::let_and_return)] // When feature "stats" is not enabled
    #[inline(always)]
    pub(super) async fn schedule(&self, mut msg: NetworkMessageMut<'_>) -> ZResult<bool> {
        #[cfg(feature = "shared-memory")]
        if let Some(shm_context) = &self.shm_context {
            map_zmsg_to_partner(&mut msg, &shm_context.shm_config, &shm_context.shm_provider);
        }

        let res = self.schedule_on_link(msg.as_ref()).await?;

        #[cfg(feature = "stats")]
        if res {
            self.link_stats.inc_network_message(zenoh_stats::Tx, msg);
        } else {
            self.link_stats.tx_observe_congestion(msg);
        }

        Ok(res)
    }
}
