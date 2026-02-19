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
//! This module is intended for Zenoh's internal use.
//!
//! [Click here for Zenoh's documentation](https://docs.rs/zenoh/latest/zenoh)

use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Mutex,
    },
    time::Instant,
};

use lazy_static::lazy_static;
use nonempty_collections::NEVec;
use zenoh_config::{DownsamplingItemConf, DownsamplingMessage, DownsamplingRuleConf};
use zenoh_keyexpr::keyexpr_tree::{
    impls::KeyedSetProvider, support::UnknownWildness, IKeyExprTree, IKeyExprTreeMut, KeBoxTree,
};
use zenoh_protocol::{
    network::{NetworkBodyMut, Push},
    zenoh::PushBody,
};
use zenoh_result::ZResult;

use crate::net::routing::interceptor::*;

pub(crate) fn downsampling_interceptor_factories(
    config: &Vec<DownsamplingItemConf>,
) -> ZResult<Vec<InterceptorFactory>> {
    let mut res: Vec<InterceptorFactory> = vec![];

    let mut id_set = HashSet::new();
    for ds in config {
        // check unicity of rule id
        if let Some(id) = &ds.id {
            if !id_set.insert(id.clone()) {
                bail!("Invalid Downsampling config: id '{id}' is repeated");
            }
        }

        res.push(Box::new(DownsamplingInterceptorFactory::new(ds.clone())));
    }

    Ok(res)
}

pub struct DownsamplingInterceptorFactory {
    interfaces: Option<NEVec<String>>,
    link_protocols: Option<NEVec<InterceptorLink>>,
    rules: NEVec<DownsamplingRuleConf>,
    flows: InterfaceEnabled,
    messages: DownsamplingFilters,
}

impl DownsamplingInterceptorFactory {
    pub fn new(conf: DownsamplingItemConf) -> Self {
        Self {
            interfaces: conf.interfaces,
            rules: conf.rules,
            link_protocols: conf.link_protocols,
            flows: conf.flows.map(|f| (&f).into()).unwrap_or(InterfaceEnabled {
                ingress: true,
                egress: true,
            }),
            messages: DownsamplingFilters::new(conf.messages.iter().copied()),
        }
    }
}

impl InterceptorFactoryTrait for DownsamplingInterceptorFactory {
    fn new_transport_unicast(
        &self,
        transport: &TransportUnicast,
    ) -> (Option<IngressInterceptor>, Option<EgressInterceptor>) {
        if let Some(interfaces) = &self.interfaces {
            if let Ok(links) = transport.get_links() {
                for link in links {
                    if !link.interfaces.iter().any(|x| interfaces.contains(x)) {
                        return (None, None);
                    }
                }
            }
        }
        if let Some(config_protocols) = &self.link_protocols {
            match transport.get_auth_ids() {
                Ok(auth_ids) => {
                    if !auth_ids
                        .link_auth_ids()
                        .iter()
                        .map(|auth_id| InterceptorLinkWrapper::from(auth_id).0)
                        .any(|v| config_protocols.contains(&v))
                    {
                        return (None, None);
                    }
                }
                Err(e) => {
                    tracing::error!("Error loading transport AuthIds: {e}");
                    return (None, None);
                }
            }
        }

        #[cfg(feature = "stats")]
        let Ok(stats) = transport
            .get_stats()
            .map(|stats| stats.drop_stats(zenoh_stats::ReasonLabel::Downsampling))
        else {
            // `get_stats` returning an error means the transport is closed
            return (None, None);
        };
        let interceptor = |flow| {
            let direction = match flow {
                InterceptorFlow::Ingress => "ingress",
                InterceptorFlow::Egress => "egress",
            };
            tracing::debug!("New {direction} downsampler on transport unicast {transport:?}");
            Box::new(DownsamplingInterceptor::new(
                self.messages,
                &self.rules,
                flow,
                #[cfg(feature = "stats")]
                stats.clone(),
            )) as Interceptor
        };
        (
            self.flows
                .ingress
                .then(|| interceptor(InterceptorFlow::Ingress)),
            self.flows
                .egress
                .then(|| interceptor(InterceptorFlow::Egress)),
        )
    }

    fn new_transport_multicast(
        &self,
        _transport: &TransportMulticast,
    ) -> Option<EgressInterceptor> {
        None
    }

    fn new_peer_multicast(&self, _transport: &TransportMulticast) -> Option<IngressInterceptor> {
        None
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct DownsamplingFilters {
    delete: bool,
    put: bool,
    query: bool,
    reply: bool,
}

impl DownsamplingFilters {
    fn new<T: IntoIterator<Item = DownsamplingMessage>>(iter: T) -> Self {
        let mut filters = Self::default();
        for m in iter {
            match m {
                DownsamplingMessage::Delete => filters.delete = true,
                #[allow(deprecated)]
                DownsamplingMessage::Push => {
                    filters.put = true;
                    filters.delete = true;
                }
                DownsamplingMessage::Put => filters.put = true,
                DownsamplingMessage::Query => filters.query = true,
                DownsamplingMessage::Reply => filters.reply = true,
            }
        }
        filters
    }

    fn is_msg_filtered(&self, msg: &mut NetworkMessageMut) -> bool {
        match msg.body {
            NetworkBodyMut::Push(Push { payload, .. }) => match payload {
                PushBody::Put(_) => self.put,
                PushBody::Del(_) => self.delete,
            },
            NetworkBodyMut::Request(_) => self.query,
            NetworkBodyMut::Response(_) => self.reply,
            NetworkBodyMut::ResponseFinal(_) => false,
            NetworkBodyMut::Interest(_) => false,
            NetworkBodyMut::Declare(_) => false,
            NetworkBodyMut::OAM(_) => false,
        }
    }
}

lazy_static! {
    // Reference point for lock-free timestamp arithmetic (nanos since this Instant).
    static ref DS_EPOCH: Instant = Instant::now();
}

pub(crate) struct DownsamplingInterceptor {
    filters: DownsamplingFilters,
    ke_id: KeBoxTree<usize, UnknownWildness, KeyedSetProvider>,
    // Per-rule threshold in nanoseconds (u64::MAX = block all, indexed by rule ID).
    thresholds: Box<[u64]>,
    // Per-rule last-passed timestamp in nanos since DS_EPOCH (indexed by rule ID).
    // Accessed with Relaxed ordering — slight over-passing under contention is acceptable
    // for a rate-limiter.
    last_timestamps: Box<[AtomicU64]>,
    flow: InterceptorFlow,
    #[cfg(feature = "stats")]
    stats: zenoh_stats::DropStats,
}

impl DownsamplingInterceptor {
    fn get_id(&self, key_expr: &keyexpr) -> Option<usize> {
        let node = self.ke_id.intersecting_keys(key_expr).next()?;
        self.ke_id.weight_at(&node).copied()
    }
}

// The flag is used to print a message only once
static INFO_FLAG: AtomicBool = AtomicBool::new(false);

impl InterceptorTrait for DownsamplingInterceptor {
    fn compute_keyexpr_cache(&self, key_expr: &keyexpr) -> Option<Box<dyn Any + Send + Sync>> {
        Some(Box::new(self.get_id(key_expr)))
    }

    fn intercept(&self, msg: &mut NetworkMessageMut, ctx: &mut dyn InterceptorContext) -> bool {
        if !self.filters.is_msg_filtered(msg) {
            return true;
        };
        let cache = ctx
            .get_cache(msg)
            .and_then(|c| c.downcast_ref::<Option<usize>>().copied());
        let Some(id) = cache.unwrap_or_else(|| self.get_id(&ctx.full_keyexpr(msg)?)) else {
            return true;
        };

        let Some(threshold) = self.thresholds.get(id).copied() else {
            tracing::debug!("unexpected cache ID {}", id);
            return true;
        };
        let now_nanos = DS_EPOCH.elapsed().as_nanos() as u64;
        let last_nanos = self.last_timestamps[id].load(Ordering::Relaxed);
        if now_nanos.saturating_sub(last_nanos) >= threshold {
            self.last_timestamps[id].store(now_nanos, Ordering::Relaxed);
            true
        } else {
            if !INFO_FLAG.swap(true, Ordering::Relaxed) {
                tracing::info!("Some message(s) have been dropped by the downsampling interceptor. Enable trace level tracing for more details.");
            }
            let direction = match self.flow {
                InterceptorFlow::Egress => "to",
                InterceptorFlow::Ingress => "from",
            };
            tracing::trace!(
                "Message dropped by the downsampling interceptor: {msg}({key_expr}) {direction}:{face}",
                key_expr = ctx.full_expr(msg).unwrap_or_default(),
                face = ctx.face().map(|f| f.to_string()).unwrap_or_default(),
            );
            #[cfg(feature = "stats")]
            self.stats
                .observe_network_message_dropped_payload(stats_direction(self.flow), msg);
            false
        }
    }
}

const NANOS_PER_SEC: f64 = 1_000_000_000.0;

impl DownsamplingInterceptor {
    pub fn new(
        filters: DownsamplingFilters,
        rules: &NEVec<DownsamplingRuleConf>,
        flow: InterceptorFlow,
        #[cfg(feature = "stats")] stats: zenoh_stats::DropStats,
    ) -> Self {
        // Ensure DS_EPOCH is initialized now (at interceptor creation time).
        let _ = *DS_EPOCH;

        let mut ke_id = KeBoxTree::default();
        let mut thresholds = Vec::with_capacity(rules.len().get());
        let mut last_timestamps = Vec::with_capacity(rules.len().get());

        for (id, rule) in rules.into_iter().enumerate() {
            let threshold_nanos = if rule.freq != 0.0 {
                (1. / rule.freq * NANOS_PER_SEC) as u64
            } else {
                // freq == 0 means block all messages; u64::MAX is never reached by elapsed nanos
                // over any realistic program lifetime (would require ~584 years of runtime).
                u64::MAX
            };
            ke_id.insert(&rule.key_expr, id);
            // Initialize to 0 so the first message passes once DS_EPOCH.elapsed() >= threshold.
            // For typical thresholds (>= 1ms) this holds within normal session setup time.
            thresholds.push(threshold_nanos);
            last_timestamps.push(AtomicU64::new(0));
            tracing::debug!(
                "New downsampler rule enabled: key_expr={:?}, threshold_nanos={:?}, messages={:?}",
                rule.key_expr,
                threshold_nanos,
                filters,
            );
        }
        Self {
            filters,
            ke_id,
            thresholds: thresholds.into_boxed_slice(),
            last_timestamps: last_timestamps.into_boxed_slice(),
            flow,
            #[cfg(feature = "stats")]
            stats,
        }
    }
}
