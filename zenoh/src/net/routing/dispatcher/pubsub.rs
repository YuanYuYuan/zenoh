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

use std::sync::Arc;

use zenoh_protocol::{
    core::{Region, Reliability, WireExpr},
    network::{declare::SubscriberId, push::ext, Push},
};

use super::{
    face::FaceState,
    resource::Resource,
    tables::{NodeId, Route, RoutingExpr, Tables, TablesLock},
};
use crate::net::routing::{
    dispatcher::{
        face::Face,
        local_resources::{LocalResourceInfoTrait, LocalResources},
        tables::InterRegionFilter,
    },
    gateway::{get_or_set_route, node_id_as_source, Direction, RouteBuilder},
    hat::{DispatcherContext, SendDeclare},
};

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) struct SubscriberInfo;

#[allow(clippy::too_many_arguments)]
pub(crate) async fn declare_subscription<'a>(
    hat_code: &(dyn HatTrait + Send + Sync),
    tables: &TablesLock,
    face: &mut Arc<FaceState>,
    id: SubscriberId,
    expr: &'a WireExpr<'a>,
    sub_info: &SubscriberInfo,
    node_id: NodeId,
    send_declare: &'a mut SendDeclare<'a>,
) {
    let rtables = tables.tables.read().await;
    match rtables
        .get_mapping(face, &expr.scope, expr.mapping)
        .cloned()
    {
        Some(mut prefix) => {
            tracing::debug!(
                "{} Declare subscriber {} ({}{})",
                face,
                id,
                prefix.expr(),
                expr.suffix
            );
            let res = Resource::get_resource(&prefix, &expr.suffix);
            let (mut res, mut wtables) =
                if res.as_ref().map(|r| r.context.is_some()).unwrap_or(false) {
                    drop(rtables);
                    let wtables = tables.tables.write().await;
                    (res.unwrap(), wtables)
                } else {
                    let mut fullexpr = prefix.expr().to_string();
                    fullexpr.push_str(expr.suffix.as_ref());
                    let mut matches = keyexpr::new(fullexpr.as_str())
                        .map(|ke| Resource::get_matches(&rtables, ke))
                        .unwrap_or_default();
                    drop(rtables);
                    let mut wtables = tables.tables.write().await;
                    let mut res = Resource::make_resource(
                        hat_code,
                        &mut wtables,
                        &mut prefix,
                        expr.suffix.as_ref(),
                    );
                    matches.push(Arc::downgrade(&res));
                    Resource::match_resource(&wtables, &mut res, matches);
                    (res, wtables)
                };

            let mut ctx = DispatcherContext {
                tables_lock: &self.tables,
                tables: &mut tables.data,
                src_face: &mut self.state.clone(),
                send_declare,
            };

            hats[region].register_subscriber(ctx.reborrow(), id, res.clone(), node_id, sub_info);

            hats[region].disable_data_routes(&mut res);

            for dst in hats.regions().collect_vec() {
                let other_info = hats
                    .values()
                    .filter(|hat| hat.region() != dst)
                    .flat_map(|hat| hat.remote_subscribers_of(ctx.tables, &res))
                    .reduce(|_, _| SubscriberInfo);

                hats[dst].propagate_subscriber(ctx.reborrow(), res.clone(), other_info);
            }
        });
    }

pub(crate) async fn undeclare_subscription<'a>(
    hat_code: &(dyn HatTrait + Send + Sync),
    tables: &TablesLock,
    face: &mut Arc<FaceState>,
    id: SubscriberId,
    expr: &'a WireExpr<'a>,
    node_id: NodeId,
    send_declare: &'a mut SendDeclare<'a>,
) {
    let res = if expr.is_empty() {
        None
    } else {
        let rtables = tables.tables.read().await;
        match rtables.get_mapping(face, &expr.scope, expr.mapping) {
            Some(prefix) => match Resource::get_resource(prefix, expr.suffix.as_ref()) {
                Some(res) => Some(res),
                None => {
                    tracing::error!(
                        "{} Undeclare unknown subscriber {}{}!",
                        face,
                        prefix.expr(),
                        expr.suffix
                    );
                    return;
                }
            }
        }
    };
    let mut wtables = tables.tables.write().await;
    if let Some(mut res) =
        hat_code.undeclare_subscription(&mut wtables, face, id, res, node_id, send_declare)
    {
        tracing::debug!("{} Undeclare subscriber {} ({})", face, id, res.expr());
        disable_matches_data_routes(&mut wtables, &mut res);
        Resource::clean(&mut res);
        drop(wtables);
    } else {
        // NOTE: This is expected behavior if subscriber declarations are denied with ingress ACL interceptor.
        tracing::debug!("{} Undeclare unknown subscriber {}", face, id);
    }
}

pub(crate) fn disable_matches_data_routes(_tables: &mut Tables, res: &mut Arc<Resource>) {
    if res.context.is_some() {
        get_mut_unchecked(res).context_mut().disable_data_routes();
        for match_ in &res.context().matches {
            let mut match_ = match_.upgrade().unwrap();
            if !Arc::ptr_eq(&match_, res) {
                get_mut_unchecked(&mut match_)
                    .context_mut()
                    .disable_data_routes();
            }
        }
    }
}

macro_rules! treat_timestamp {
    ($hlc:expr, $payload:expr, $drop:expr) => {
        // if an HLC was configured (via Config.add_timestamp),
        // check DataInfo and add a timestamp if there isn't
        if let Some(hlc) = $hlc {
            if let zenoh_protocol::zenoh::PushBody::Put(data) = &mut $payload {
                if let Some(ref ts) = data.timestamp {
                    // Timestamp is present; update HLC with it (possibly raising error if delta exceed)
                    match hlc.update_with_timestamp(ts) {
                        Ok(()) => (),
                        Err(e) => {
                            if $drop {
                                tracing::error!(
                                    "Error treating timestamp for received Data ({}). Drop it!",
                                    e
                                );
                                return;
                            } else {
                                data.timestamp = Some(hlc.new_timestamp());
                                tracing::error!(
                                    "Error treating timestamp for received Data ({}). Replace timestamp: {:?}",
                                    e,
                                    data.timestamp);
                            }
                        }
                    }
                } else {
                    // Timestamp not present; add one
                    data.timestamp = Some(hlc.new_timestamp());
                    tracing::trace!("Adding timestamp to DataInfo: {:?}", data.timestamp);
                }
            }
        }
    }
}

#[inline]
fn get_hat_data_route(
    tables: &Tables,
    src_face: &FaceState,
    expr: &RoutingExpr,
    node_id: NodeId,
    region: &Region,
) -> Arc<Route> {
    let node_id = tables.hats[region].map_routing_context(&tables.data, src_face, node_id);
    let compute_route =
        || tables.hats[region].compute_data_route(&tables.data, &src_face.region, expr, node_id);
    match expr
        .resource()
        .as_ref()
        .and_then(|res| res.ctx.as_ref())
        .map(|ctx| &ctx.hats[region].data_routes)
    {
        Some(data_routes) => get_or_set_route(
            data_routes,
            tables.data.hats[region].routes_version,
            &src_face.region,
            node_id,
            compute_route,
        ),
        None => compute_route(),
    }
}

#[inline]
fn get_data_route(
    tables: &Tables,
    src_face: &FaceState,
    expr: &RoutingExpr,
    node_id: NodeId,
) -> Arc<Route> {
    let compute_route = || {
        let mut builder = RouteBuilder::<Direction>::new();

        for (region, _) in tables.hats.iter() {
            let route = get_hat_data_route(tables, src_face, expr, node_id, &region);

            for dir in route.iter() {
                builder.insert(dir.dst_face.id, || dir.clone());
            }
        }
        Arc::new(builder.build())
    };
    let node_id = tables.hats[src_face.region].map_routing_context(&tables.data, src_face, node_id);
    match expr
        .resource()
        .as_ref()
        .and_then(|res| res.ctx.as_ref())
        .map(|ctx| &ctx.data_routes)
    {
        Some(data_routes) => get_or_set_route(
            data_routes,
            tables.data.routes_version,
            &src_face.region,
            node_id,
            compute_route,
        ),
        None => compute_route(),
    }
}

pub async fn route_data(
    tables_ref: &Arc<TablesLock>,
    src_face: &FaceState,
    msg: &mut Push,
    reliability: Reliability,
    consume: bool,
) {
    // Fast path: try_read() succeeds without blocking when no writer holds the lock (99.9% of
    // messages). Falls back to async wait only under declaration churn.
    let tables = match tables_ref.tables.try_read() {
        Some(g) => g,
        None => tables_ref.tables.read().await,
    };
    match tables.get_mapping(face, &msg.wire_expr.scope, msg.wire_expr.mapping) {
        Some(prefix) => {
            tracing::trace!(
                "{} Route data for res {}{}",
                face,
                prefix.expr(),
                msg.wire_expr.suffix.as_ref()
            );
            let expr = RoutingExpr::new(prefix, msg.wire_expr.suffix.as_ref());

    tracing::trace!(
        "{} Route data for res {}{}",
        src_face,
        prefix.expr(),
        msg.wire_expr.suffix.as_ref()
    );

    let expr = RoutingExpr::new(prefix, msg.wire_expr.suffix.as_ref());

    #[cfg(feature = "stats")]
    let payload_observer = super::stats::PayloadObserver::new(msg, Some(&expr), tables);
    #[cfg(feature = "stats")]
    payload_observer.observe_payload(zenoh_stats::Rx, src_face, msg);

    if !tables.ingress_filter(src_face) {
        return;
    }

    let send_push = |dst_face: &FaceState, msg: &mut Push, reliability: Reliability| {
        if dst_face.primitives.send_push(msg, reliability) {
            #[cfg(feature = "stats")]
            let payload_observer = super::stats::PayloadObserver::new(msg, Some(&expr), &tables);
            #[cfg(feature = "stats")]
            payload_observer.observe_payload(zenoh_stats::Rx, face, msg);

            if tables_ref.hat_code.ingress_filter(&tables, face, &expr) {
                let route = get_data_route(
                    tables_ref.hat_code.as_ref(),
                    &tables,
                    face,
                    &expr,
                    msg.ext_nodeid.node_id,
                );

                if !route.is_empty() {
                    treat_timestamp!(&tables.hlc, msg.payload, tables.drop_future_timestamp);

                    if route.len() == 1 {
                        let (outface, key_expr, context) = route.iter().next().unwrap();
                        if tables_ref
                            .hat_code
                            .egress_filter(&tables, face, outface, &expr)
                        {
                            drop(tables);
                            // Construct msg_to_send directly — avoids cloning the wire_expr
                            // string a second time (the original code set msg.wire_expr then
                            // cloned msg, paying for two string allocations).
                            let msg_to_send = Push {
                                wire_expr: key_expr.into(),
                                ext_qos: msg.ext_qos,
                                ext_tstamp: msg.ext_tstamp,
                                ext_nodeid: ext::NodeIdType { node_id: *context },
                                payload: msg.payload.clone(),
                            };
                            if outface.primitives.send_push(msg_to_send, reliability).await {
                                #[cfg(feature = "stats")]
                                payload_observer.observe_payload(zenoh_stats::Tx, outface, msg);
                            }
                            // Reset the wire_expr to indicate the message has been consumed
                            msg.wire_expr = WireExpr::empty();
                        }
                    } else {
                        let route = route
                            .iter()
                            .filter(|(outface, _key_expr, _context)| {
                                tables_ref
                                    .hat_code
                                    .egress_filter(&tables, face, outface, &expr)
                            })
                            .cloned()
                            .collect::<Vec<Direction>>();

                        drop(tables);
                        for (outface, key_expr, context) in route {
                            let msg_to_send = Push {
                                wire_expr: key_expr,
                                ext_qos: msg.ext_qos,
                                ext_tstamp: None,
                                ext_nodeid: ext::NodeIdType { node_id: context },
                                payload: msg.payload.clone(),
                            };
                            if outface.primitives.send_push(msg_to_send, reliability).await {
                                #[cfg(feature = "stats")]
                                payload_observer.observe_payload(zenoh_stats::Tx, &outface, &msg);
                            }
                        }
                    }
                }
            }
        }
    };

    let route = get_data_route(&rtables, src_face, &expr, msg.ext_nodeid.node_id);

    tracing::trace!(?route);

    if !route.is_empty() {
        treat_timestamp!(
            &rtables.data.hlc,
            msg.payload,
            rtables.data.drop_future_timestamp
        );

        let inter_region_filter = {
            let src_zid = tables.hats[src_face.region]
                .remote_node_id_to_zid(src_face, msg.ext_nodeid.node_id);
            move |dir: &Direction| {
                InterRegionFilter {
                    src: &src_face.region,
                    dst: &dir.dst_face.region,
                    src_zid: src_zid.as_ref(),
                    fwd_zid: Some(&src_face.zid),
                    dst_zid: Some(&dir.dst_face.zid),
                }
                .resolve(tables)
            }
        };

        if route.len() == 1 {
            let dir = route.iter().next().unwrap();

            if inter_region_filter(dir) && rtables.egress_filter(src_face, &dir.dst_face) {
                drop(rtables);
                let mut msg_clone;
                let mut msg = &mut *msg;
                if !consume {
                    msg_clone = msg.clone();
                    msg = &mut msg_clone;
                }

                msg.wire_expr = dir.wire_expr.clone();
                msg.ext_nodeid = ext::NodeIdType {
                    node_id: dir.node_id,
                };
                send_push(&dir.dst_face, msg, reliability);
            }
        } else {
            let dirs = route
                .iter()
                .filter(|dir| {
                    inter_region_filter(dir) && rtables.egress_filter(src_face, &dir.dst_face)
                })
                .collect::<Vec<&Direction>>();

            drop(rtables);
            for dir in dirs {
                send_push(
                    &dir.dst_face,
                    &mut Push {
                        wire_expr: dir.wire_expr.clone(),
                        ext_qos: msg.ext_qos,
                        ext_tstamp: None,
                        ext_nodeid: ext::NodeIdType {
                            node_id: dir.node_id,
                        },
                        payload: msg.payload.clone(),
                    },
                    reliability,
                );
            }
        }
    }
}

impl LocalResourceInfoTrait<Arc<Resource>> for SubscriberInfo {
    fn aggregate(
        _self_val: Option<Self>,
        _self_res: &Arc<Resource>,
        other_val: &Self,
        _other_res: &Arc<Resource>,
    ) -> Self {
        *other_val
    }

    fn aggregate_many<'a>(
        _self_res: &Arc<Resource>,
        mut iter: impl Iterator<Item = (&'a Arc<Resource>, Self)>,
    ) -> Option<Self> {
        iter.next().map(|(_, val)| val)
    }
}

pub(crate) type LocalSubscribers = LocalResources<SubscriberId, Arc<Resource>, SubscriberInfo>;
