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
use std::{
    ops::Not,
    sync::{Arc, Weak},
    time::Duration,
};

use async_trait::async_trait;
use itertools::Itertools;
use tokio_util::sync::CancellationToken;
use zenoh_buffers::ZBuf;
#[allow(unused_imports)]
use zenoh_core::polyfill::*;
use zenoh_protocol::{
    core::{Encoding, Region, WireExpr},
    network::{
        declare::{queryable::ext::QueryableInfoType, QueryableId},
        request::{self, ext::QueryTarget, Request, RequestId},
        response::{self, Response, ResponseFinal},
    },
    zenoh::{self, ResponseBody},
};
use zenoh_sync::get_mut_unchecked;
use zenoh_util::Timed;
use zenoh_runtime::ZRuntime;

use super::{
    face::FaceState,
    resource::{QueryTargetQablSet, Resource},
    tables::{NodeId, RoutingExpr, TablesLock},
};
use crate::net::routing::{
    dispatcher::{
        face::Face,
        local_resources::{LocalResourceInfoTrait, LocalResources},
        tables::{InterRegionFilter, Tables},
    },
    gateway::{get_or_set_route, node_id_as_source, QueryDirection, QueryTargetQabl, RouteBuilder},
    hat::{DispatcherContext, SendDeclare, UnregisterEntityResult},
};

#[derive(Clone)]
pub(crate) struct Query {
    src_face: Arc<FaceState>,
    src_qid: RequestId,
    src_qos: response::ext::QoSType,
}

#[inline]
pub(crate) fn get_matching_queryables(
    hat_code: &(dyn HatTrait + Send + Sync),
    tables: &Tables,
    key_expr: &KeyExpr<'_>,
    complete: bool,
) -> HashMap<usize, Arc<FaceState>> {
    hat_code.get_matching_queryables(tables, key_expr, complete)
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn declare_queryable<'a>(
    hat_code: &(dyn HatTrait + Send + Sync),
    tables: &TablesLock,
    face: &mut Arc<FaceState>,
    id: QueryableId,
    expr: &'a WireExpr<'a>,
    qabl_info: &QueryableInfoType,
    node_id: NodeId,
    send_declare: &'a mut SendDeclare<'a>,
) {
    let rtables = zasyncread!(tables.tables);
    match rtables
        .get_mapping(face, &expr.scope, expr.mapping)
        .cloned()
    {
        Some(mut prefix) => {
            tracing::debug!(
                "{} Declare queryable {} ({}{})",
                face,
                id,
                prefix.expr(),
                expr.suffix
            );
            let res = Resource::get_resource(&prefix, &expr.suffix);
            let (mut res, mut wtables) =
                if res.as_ref().map(|r| r.context.is_some()).unwrap_or(false) {
                    drop(rtables);
                    let wtables = zasyncwrite!(tables.tables);
                    (res.unwrap(), wtables)
                } else {
                    let mut fullexpr = prefix.expr().to_string();
                    fullexpr.push_str(expr.suffix.as_ref());
                    let mut matches = keyexpr::new(fullexpr.as_str())
                        .map(|ke| Resource::get_matches(&rtables, ke))
                        .unwrap_or_default();
                    drop(rtables);
                    let mut wtables = zasyncwrite!(tables.tables);
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

            hat_code.declare_queryable(
                &mut wtables,
                face,
                id,
                &mut res,
                qabl_info,
                node_id,
                send_declare,
            );

            disable_matches_query_routes(&mut wtables, &mut res);
            drop(wtables);
        }
        None => tracing::error!(
            "{} Declare queryable {} for unknown scope {}",
            face,
            id,
            expr.scope
        ),
        ret
    )]
    pub(crate) fn declare_queryable(
        &self,
        id: QueryableId,
        expr: &WireExpr,
        qabl_info: &QueryableInfoType,
        node_id: NodeId,
        send_declare: &mut SendDeclare,
    ) {
        self.with_mapped_expr(expr, |tables, mut res| {
            let region = self.state.region;

pub(crate) async fn undeclare_queryable<'a>(
    hat_code: &(dyn HatTrait + Send + Sync),
    tables: &TablesLock,
    face: &mut Arc<FaceState>,
    id: QueryableId,
    expr: &'a WireExpr<'a>,
    node_id: NodeId,
    send_declare: &'a mut SendDeclare<'a>,
) {
    let res = if expr.is_empty() {
        None
    } else {
        let rtables = zasyncread!(tables.tables);
        match rtables.get_mapping(face, &expr.scope, expr.mapping) {
            Some(prefix) => match Resource::get_resource(prefix, expr.suffix.as_ref()) {
                Some(res) => Some(res),
                None => {
                    tracing::error!(
                        "{} Undeclare unknown queryable {} ({}{})",
                        face,
                        id,
                        prefix.expr(),
                        expr.suffix
                    );
                    return;
                }

                for dst in rtables.hats.regions() {
                    let qabls =
                        get_query_route(&rtables, src_face, &expr, msg.ext_nodeid.node_id, &dst);

                    let filter = {
                        let src_zid = rtables.hats[src_face.region]
                            .remote_node_id_to_zid(src_face, msg.ext_nodeid.node_id);
                        let tables = &rtables;

                        move |q: &QueryTargetQabl| {
                            InterRegionFilter {
                                src: &src_face.region,
                                dst: &q.region,
                                src_zid: src_zid.as_ref(),
                                fwd_zid: Some(&self.state.zid),
                                dst_zid: Some(&q.dir.dst_face.zid),
                            }
                            .resolve(tables)
                                && tables.egress_filter(src_face, &q.dir.dst_face)
                        }
                    };

                    self.compute_final_route(msg.ext_target, &mut builder, &query, &qabls, filter);
                }

                // NOTE: it's important to drop the `Arc<Query>` object immediately otherwise
                // a ResponseFinal from a local queryable won't finalize the query,
                // this is because `Arc::strong_count(&query)` would always be > 1.
                drop(query);

                let timeout = msg
                    .ext_timeout
                    .unwrap_or(rtables.data.queries_default_timeout);

                drop(queries_lock);
                drop(rtables);

                let dirs = builder.build();

                tracing::trace!(?dirs);

                if dirs.is_empty() {
                    tracing::debug!(
                        "{}:{} Send final reply (no matching queryables or not master)",
                        self.state,
                        msg.id
                    );
                    self.state
                        .primitives
                        .clone()
                        .send_response_final(&mut ResponseFinal {
                            rid: msg.id,
                            ext_qos: msg.ext_qos,
                            ext_tstamp: None,
                        });
                } else {
                    for QueryDirection { dir, rid } in dirs.into_iter() {
                        QueryCleanup::spawn_query_clean_up_task(
                            &dir.dst_face,
                            &self.tables,
                            rid,
                            msg.ext_qos,
                            timeout,
                        );

                        tracing::trace!(
                            "{}:{} Propagate query to {}:{}",
                            self.state,
                            msg.id,
                            dir.dst_face,
                            rid
                        );

                        let msg = &mut Request {
                            id: rid,
                            wire_expr: dir.wire_expr,
                            ext_qos: msg.ext_qos,
                            ext_tstamp: msg.ext_tstamp,
                            ext_nodeid: request::ext::NodeIdType {
                                node_id: dir.node_id,
                            },
                            ext_target: msg.ext_target,
                            ext_budget: msg.ext_budget,
                            ext_timeout: msg.ext_timeout,
                            payload: msg.payload.clone(),
                        };

                        if dir.dst_face.primitives.send_request(msg) {
                            #[cfg(feature = "stats")]
                            payload_observer.observe_payload(zenoh_stats::Tx, &dir.dst_face, msg);
                        }
                    }
                }
            }
            None => {
                tracing::error!(
                    "{}:{} Route query with unknown scope {}! Send final reply.",
                    self.state,
                    msg.id,
                    msg.wire_expr.scope,
                );
                drop(rtables);
                self.state
                    .primitives
                    .clone()
                    .send_response_final(&mut ResponseFinal {
                        rid: msg.id,
                        ext_qos: msg.ext_qos,
                        ext_tstamp: None,
                    });
            }
        }
    }

    #[allow(clippy::incompatible_msrv)]
    fn compute_final_route(
        &self,
        target: QueryTarget,
        route: &mut RouteBuilder<QueryDirection>,
        query: &Arc<Query>,
        qabls: &Arc<QueryTargetQablSet>,
        filter: impl Fn(&QueryTargetQabl) -> bool,
    ) {
        match target {
            QueryTarget::All => {
                for qabl in qabls.iter().filter(|q| filter(q)) {
                    route.insert(qabl.dir.dst_face.id, || {
                        let mut dir = qabl.dir.clone();
                        let rid = insert_pending_query(&mut dir.dst_face, query.clone());
                        tracing::debug!(dst = %dir.dst_face, dst.target = "all");
                        QueryDirection { dir, rid }
                    });
                }
            }
            QueryTarget::AllComplete => {
                for qabl in qabls
                    .iter()
                    .filter(|q| q.info.is_none_or(|info| info.complete) && filter(q))
                {
                    route.insert(qabl.dir.dst_face.id, || {
                        let mut dir = qabl.dir.clone();
                        let rid = insert_pending_query(&mut dir.dst_face, query.clone());
                        tracing::debug!(dst = %dir.dst_face, dst.target = "all-complete");
                        QueryDirection { dir, rid }
                    });
                }
            }
            QueryTarget::BestMatching => {
                if let Some(qabl) = qabls
                    .iter()
                    .find(|q| q.info.is_some_and(|info| info.complete) && filter(q))
                {
                    route.insert(qabl.dir.dst_face.id, || {
                        let mut dir = qabl.dir.clone();
                        let rid = insert_pending_query(&mut dir.dst_face, query.clone());
                        tracing::debug!(dst = %dir.dst_face, dst.target = "best-matching");
                        QueryDirection { dir, rid }
                    });
                } else {
                    self.compute_final_route(QueryTarget::All, route, query, qabls, filter)
                }
            }
        }
    };
    let mut wtables = zasyncwrite!(tables.tables);
    if let Some(mut res) =
        hat_code.undeclare_queryable(&mut wtables, face, id, res, node_id, send_declare)
    {
        tracing::debug!("{} Undeclare queryable {} ({})", face, id, res.expr());
        disable_matches_query_routes(&mut wtables, &mut res);
        Resource::clean(&mut res);
        drop(wtables);
    } else {
        // NOTE: This is expected behavior if queryable declarations are denied with ingress ACL interceptor.
        tracing::debug!("{} Undeclare unknown queryable {}", face, id);
    }
}

#[inline]
fn insert_pending_query(outface: &mut Arc<FaceState>, query: Arc<Query>) -> RequestId {
    let outface_mut = get_mut_unchecked(outface);
    // This `wrapping_add` is kind of "safe" because it would require an incredible amount
    // of parallel running queries to conflict a currently used id.
    // However, query ids are encoded with varint algorithm, so an incremental id isn't a
    // good match, and there is still room for optimization.
    outface_mut.next_qid = outface_mut.next_qid.wrapping_add(1);
    let qid = outface_mut.next_qid;
    outface_mut.pending_queries.insert(
        qid,
        (query, outface_mut.task_controller.get_cancellation_token()),
    );
    qid
}

#[derive(Clone)]
struct QueryCleanup {
    tables: Arc<TablesLock>,
    face: Weak<FaceState>,
    qid: RequestId,
    qos: response::ext::QoSType,
    timeout: Duration,
}

impl QueryCleanup {
    pub fn spawn_query_clean_up_task(
        face: &Arc<FaceState>,
        tables_ref: &Arc<TablesLock>,
        qid: u32,
        qos: response::ext::QoSType,
        timeout: Duration,
    ) {
        let mut cleanup = QueryCleanup {
            tables: tables_ref.clone(),
            face: Arc::downgrade(face),
            qid,
            qos,
            timeout,
        };
        let queries_lock = zread!(tables_ref.queries_lock);
        if let Some((_, cancellation_token)) = face.pending_queries.get(&qid) {
            let c_cancellation_token = cancellation_token.clone();
            drop(queries_lock);
            face.task_controller
                .spawn_with_rt(zenoh_runtime::ZRuntime::Net, async move {
                    tokio::select! {
                        _ = async_io::Timer::after(timeout) => { cleanup.run().await }
                        _ = c_cancellation_token.cancelled() => {}
                    }
                });
        }
    }
}

#[async_trait]
impl Timed for QueryCleanup {
    async fn run(&mut self) {
        if let Some(mut face) = self.face.upgrade() {
            let ext_respid = Some(response::ext::ResponderIdType {
                zid: face.zid,
                eid: 0,
            });
            route_send_response(
                &self.tables,
                &mut face,
                &mut Response {
                    rid: self.qid,
                    wire_expr: WireExpr::empty(),
                    payload: ResponseBody::Err(zenoh::Err {
                        encoding: Encoding::default(),
                        ext_sinfo: None,
                        #[cfg(feature = "shared-memory")]
                        ext_shm: None,
                        ext_unknown: vec![],
                        payload: ZBuf::from("Timeout".as_bytes().to_vec()),
                    }),
                    ext_qos: self.qos,
                    ext_tstamp: None,
                    ext_respid,
                },
            );
            let queries_lock = zasyncwrite!(self.tables.queries_lock);
            if let Some(query) = get_mut_unchecked(&mut face)
                .pending_queries
                .remove(&self.qid)
            {
                drop(queries_lock);
                tracing::warn!(
                    "{}:{} Didn't receive final reply for query {}:{}: Timeout({:#?})!",
                    face,
                    self.qid,
                    query.0.src_face,
                    query.0.src_qid,
                    self.timeout,
                );
                finalize_pending_query(query);
            }
        }
    }
}

#[inline]
fn get_query_route(
    tables: &Tables,
    src_face: &FaceState,
    expr: &RoutingExpr,
    routing_context: NodeId,
    region: &Region,
) -> Arc<QueryTargetQablSet> {
    let node_id = tables.hats[region].map_routing_context(&tables.data, src_face, routing_context);
    let compute_route =
        || tables.hats[region].compute_query_route(&tables.data, &src_face.region, expr, node_id);
    if let Some(query_routes) = expr
        .resource()
        .as_ref()
        .and_then(|res| res.ctx.as_ref())
        .map(|ctx| &ctx.hats[region].query_routes)
    {
        return get_or_set_route(
            query_routes,
            tables.data.hats[region].routes_version,
            &src_face.region,
            node_id,
            compute_route,
        );
    }
    compute_route()
}

#[allow(clippy::too_many_arguments)]
pub async fn route_query(tables_ref: &Arc<TablesLock>, face: &Arc<FaceState>, msg: &mut Request) {
    let rtables = zasyncread!(tables_ref.tables);
    match rtables.get_mapping(face, &msg.wire_expr.scope, msg.wire_expr.mapping) {
        Some(prefix) => {
            tracing::debug!(
                "{}:{} Route query for res {}{}",
                face,
                msg.id,
                prefix.expr(),
                msg.wire_expr.suffix.as_ref(),
            );
            let prefix = prefix.clone();
            let expr = RoutingExpr::new(&prefix, msg.wire_expr.suffix.as_ref());

            #[cfg(feature = "stats")]
            let payload_observer = super::stats::PayloadObserver::new(msg, Some(&expr), &rtables);
            #[cfg(feature = "stats")]
            payload_observer.observe_payload(zenoh_stats::Rx, face, msg);

            if tables_ref.hat_code.ingress_filter(&rtables, face, &expr) {
                let route = get_query_route(
                    tables_ref.hat_code.as_ref(),
                    &rtables,
                    face,
                    &expr,
                    msg.ext_nodeid.node_id,
                );

                let query = Arc::new(Query {
                    src_face: face.clone(),
                    src_qid: msg.id,
                });

                let queries_lock = zasyncwrite!(tables_ref.queries_lock);
                let route = compute_final_route(
                    tables_ref.hat_code.as_ref(),
                    &rtables,
                    &route,
                    face,
                    &expr,
                    &msg.ext_target,
                    query,
                )
                .build();
                let timeout = msg.ext_timeout.unwrap_or(rtables.queries_default_timeout);
                drop(queries_lock);
                drop(rtables);

                if route.is_empty() {
                    tracing::debug!(
                        "{}:{} Send final reply (no matching queryables or not master)",
                        face,
                        msg.id
                    );
                    face.primitives
                        .clone()
                        .send_response_final(ResponseFinal {
                            rid: msg.id,
                            ext_qos: response::ext::QoSType::RESPONSE_FINAL,
                            ext_tstamp: None,
                        }).await;
                } else {
                    for ((outface, key_expr, context), outqid) in route {
                        QueryCleanup::spawn_query_clean_up_task(
                            &outface, tables_ref, outqid, timeout,
                        );

                        tracing::trace!(
                            "{}:{} Propagate query to {}:{}",
                            face,
                            msg.id,
                            outface,
                            outqid
                        );
                        let msg_to_send = Request {
                            id: outqid,
                            wire_expr: key_expr,
                            ext_qos: msg.ext_qos,
                            ext_tstamp: msg.ext_tstamp,
                            ext_nodeid: ext::NodeIdType { node_id: context },
                            ext_target: msg.ext_target,
                            ext_budget: msg.ext_budget,
                            ext_timeout: msg.ext_timeout,
                            payload: msg.payload.clone(),
                        };
                        if outface.primitives.send_request(msg_to_send).await {
                            #[cfg(feature = "stats")]
                            payload_observer.observe_payload(zenoh_stats::Tx, &outface, msg);
                        }
                    }
                }
            } else {
                tracing::debug!("{}:{} Send final reply (not master)", face, msg.id);
                drop(rtables);
                face.primitives
                    .clone()
                    .send_response_final(ResponseFinal {
                        rid: msg.id,
                        ext_qos: response::ext::QoSType::RESPONSE_FINAL,
                        ext_tstamp: None,
                    }).await;
            }
        }
        None => {
            tracing::error!(
                "{}:{} Route query with unknown scope {}! Send final reply.",
                face,
                msg.id,
                msg.wire_expr.scope,
            );
            drop(rtables);
            face.primitives
                .clone()
                .send_response_final(ResponseFinal {
                    rid: msg.id,
                    ext_qos: response::ext::QoSType::RESPONSE_FINAL,
                    ext_tstamp: None,
                }).await;
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn route_send_response(
    tables_ref: &Arc<TablesLock>,
    face: &mut Arc<FaceState>,
    msg: &mut Response,
) {
    let tables = zasyncread!(tables_ref.tables);
    match tables.get_mapping(face, &msg.wire_expr.scope, msg.wire_expr.mapping) {
        Some(prefix) => {
            let expr = msg
                .wire_expr
                .is_empty() // account for empty wire expression in ReplyErr messages
                .not()
                .then(|| RoutingExpr::new(prefix, msg.wire_expr.suffix.as_ref()));
            #[cfg(feature = "stats")]
            let payload_observer = super::stats::PayloadObserver::new(msg, expr.as_ref(), &tables);
            #[cfg(feature = "stats")]
            payload_observer.observe_payload(zenoh_stats::Rx, face, msg);
            let queries_lock = zasyncread!(tables_ref.queries_lock);
            match face.pending_queries.get(&msg.rid) {
                Some((query, _)) => {
                    if let Some(expr) = expr {
                        // TODO: consider to optimize keyexpr for 2.0 ?
                        // Doing it now will break wire compatibility
                        // msg.wire_expr = expr.get_best_key(src_face.id).to_owned();
                        match expr.key_expr() {
                            Some(key_expr) => {
                                msg.wire_expr =
                                    WireExpr::empty().with_suffix(key_expr.as_str()).to_owned();
                            }
                            None => {
                                tracing::error!("{}:{} Route reply: wire expr {} does not map to a valid key expression!", face, msg.rid, msg.wire_expr);
                                return;
                            }
                        }
                    }
                    tracing::trace!(
                        "{}:{} Route reply for query {}:{} ({})",
                        face,
                        msg.rid,
                        query.src_face,
                        query.src_qid,
                        msg.wire_expr.suffix.as_ref()
                    );
                    drop(tables);
                    drop(queries_lock);

                    msg.rid = query.src_qid;
                    let msg_to_send = msg.clone();
                    if query.src_face.primitives.send_response(msg_to_send).await {
                        #[cfg(feature = "stats")]
                        payload_observer.observe_payload(zenoh_stats::Tx, &query.src_face, msg);
                    }
                }
                None => tracing::warn!("{}:{} Route reply: Query not found!", face, msg.rid),
            }
        }
        None => {
            tracing::error!(
                "{} Routing reply {} for unknown scope {}",
                face,
                msg.rid,
                msg.wire_expr.scope
            )
        }
    }
}

pub(crate) async fn route_send_response_final(
    tables_ref: &Arc<TablesLock>,
    face: &mut Arc<FaceState>,
    qid: RequestId,
) {
    let queries_lock = zasyncwrite!(tables_ref.queries_lock);
    match get_mut_unchecked(face).pending_queries.remove(&qid) {
        Some(query) => {
            drop(queries_lock);
            tracing::debug!(
                "{}:{} Received final reply for query {}:{} strong_count={}",
                face,
                qid,
                query.0.src_face,
                query.0.src_qid,
                Arc::strong_count(&query.0)
            );
            finalize_pending_query(query);
        }
        None => tracing::warn!("{}:{} Route final reply: Query not found!", face, qid),
    }
}

pub(crate) async fn finalize_pending_queries(tables_ref: &TablesLock, face: &mut Arc<FaceState>) {
    let queries_lock = zasyncwrite!(tables_ref.queries_lock);
    for (_, query) in get_mut_unchecked(face).pending_queries.drain() {
        finalize_pending_query(query);
    }
    drop(queries_lock);
}

pub(crate) fn finalize_pending_query(query: (Arc<Query>, CancellationToken)) {
    let (query, cancellation_token) = query;
    cancellation_token.cancel();
    if let Some(query) = Arc::into_inner(query) {
        tracing::debug!("{}:{} Propagate final reply", query.src_face, query.src_qid);
        let primitives = query.src_face.primitives.clone();
        let rid = query.src_qid;
        ZRuntime::Net.spawn(async move {
            primitives
                .send_response_final(ResponseFinal {
                    rid,
                    ext_qos: response::ext::QoSType::RESPONSE_FINAL,
                    ext_tstamp: None,
                })
                .await;
        });
    }
}

pub(crate) fn merge_qabl_infos(
    mut this: QueryableInfoType,
    info: QueryableInfoType,
) -> QueryableInfoType {
    this.distance = match (this.complete, info.complete) {
        (true, true) | (false, false) => std::cmp::min(this.distance, info.distance),
        (true, false) => this.distance,
        (false, true) => info.distance,
    };
    this.complete = this.complete || info.complete;
    this
}

impl LocalResourceInfoTrait<Arc<Resource>> for QueryableInfoType {
    fn aggregate(
        self_val: Option<Self>,
        self_res: &Arc<Resource>,
        other_val: &Self,
        other_res: &Arc<Resource>,
    ) -> Self {
        // shortcut to avoid checking inclusion of ke, since we only care about completeness in aggregates and can ignore distance
        if let Some(val) = self_val {
            if val.complete == other_val.complete {
                return val;
            }
        }

        let other_complete = if other_val.complete {
            if let (Some(self_ke), Some(other_ke)) = (self_res.keyexpr(), other_res.keyexpr()) {
                other_ke.includes(self_ke)
            } else {
                false
            }
        } else {
            false
        };
        let mut other_val = *other_val;
        other_val.complete = other_complete;

        if let Some(val) = self_val {
            merge_qabl_infos(val, other_val)
        } else {
            other_val
        }
    }
}

pub(crate) type LocalQueryables = LocalResources<QueryableId, Arc<Resource>, QueryableInfoType>;
