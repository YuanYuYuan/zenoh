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

use std::{
    collections::{HashMap, HashSet},
    fmt::{self, Debug},
    sync::{Arc, Weak},
    time::Duration,
};

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;
use zenoh_protocol::{
    core::Region,
    network::{
        declare::{self},
        interest::{InterestId, InterestMode, InterestOptions},
        Declare, DeclareBody, DeclareFinal, Interest,
    },
};
use zenoh_sync::get_mut_unchecked;
use zenoh_util::Timed;

use super::{face::FaceState, tables::TablesLock};
use crate::net::routing::{
    dispatcher::{face::Face, tables::Tables},
    gateway::{register_expr_interest, NodeId, Resource},
    hat::{DispatcherContext, Remote, RouteCurrentDeclareResult, RouteInterestResult, SendDeclare},
    RoutingContext,
};

#[derive(Debug, Clone)]
pub(crate) struct CurrentInterest {
    pub(crate) src: Remote,
    pub(crate) src_region: Region,
    pub(crate) src_interest_id: InterestId,
    pub(crate) mode: InterestMode,
}

pub(crate) struct PendingCurrentInterest {
    pub(crate) interest: Arc<CurrentInterest>,
    pub(crate) cancellation_token: CancellationToken,
    pub(crate) rejection_token: CancellationToken,
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub(crate) struct RemoteInterest {
    pub(crate) res: Option<Arc<Resource>>,
    pub(crate) options: InterestOptions,
    pub(crate) mode: InterestMode,
}

impl fmt::Debug for RemoteInterest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteInterest")
            .field("res", &self.res.as_ref().map(|res| res.expr()))
            .field("opts", &self.options)
            .field("mode", &self.mode)
            .finish()
    }
}

impl RemoteInterest {
    pub(crate) fn matches(&self, res: &Arc<Resource>) -> bool {
        self.res.as_ref().map(|r| r.matches(res)).unwrap_or(true)
    }
}

pub(crate) async fn declare_final<'a>(
    hat_code: &(dyn HatTrait + Send + Sync),
    wtables: &mut Tables,
    face: &mut Arc<FaceState>,
    id: InterestId,
    send_declare: &'a mut SendDeclare<'a>,
) {
    if let Some(interest) = get_mut_unchecked(face)
        .pending_current_interests
        .remove(&id)
    {
        finalize_pending_interest(interest, send_declare);
    }

    hat_code.declare_final(wtables, face, id);
}

pub(crate) async fn finalize_pending_interests<'a>(
    _tables_ref: &TablesLock,
    face: &mut Arc<FaceState>,
    send_declare: &'a mut SendDeclare<'a>,
) {
    for (_, interest) in get_mut_unchecked(face).pending_current_interests.drain() {
        finalize_pending_interest(interest, send_declare);
    }
}

pub(crate) fn finalize_pending_interest(
    pending_interest: PendingCurrentInterest,
    send_declare: &mut SendDeclare,
) {
    let interest = pending_interest.interest;
    pending_interest.cancellation_token.cancel();
    if let Some(interest) = Arc::into_inner(interest) {
        // FIXME(regions): this is only safe as long as router interests remain unimplemented
        let src_face = interest
            .src
            .downcast_ref_to_face()
            .expect("interest source remote should be a face");

        tracing::debug!(
            "{}:{} Propagate DeclareFinal",
            src_face,
            interest.src_interest_id
        );

        send_declare(
            &src_face.primitives,
            RoutingContext::new(Declare {
                interest_id: Some(interest.src_interest_id),
                ext_qos: declare::ext::QoSType::DECLARE,
                ext_tstamp: None,
                ext_nodeid: declare::ext::NodeIdType::DEFAULT,
                body: DeclareBody::DeclareFinal(DeclareFinal),
            }),
        );
    }
}

#[derive(Clone)]
pub(crate) struct CurrentInterestCleanup {
    tables: Arc<TablesLock>,
    face: Weak<FaceState>,
    id: InterestId,
    interests_timeout: Duration,
}

impl CurrentInterestCleanup {
    pub(crate) fn spawn_interest_clean_up_task(
        face: &Arc<FaceState>,
        tables_ref: &Arc<TablesLock>,
        id: u32,
        interests_timeout: Duration,
    ) {
        let mut cleanup = CurrentInterestCleanup {
            tables: tables_ref.clone(),
            face: Arc::downgrade(face),
            id,
            interests_timeout,
        };
        if let Some(pending_interest) = face.pending_current_interests.get(&id) {
            let cancellation_token = pending_interest.cancellation_token.clone();
            let rejection_token = pending_interest.rejection_token.clone();
            face.task_controller
                .spawn_with_rt(zenoh_runtime::ZRuntime::Net, async move {
                    tokio::select! {
                        _ = async_io::Timer::after(cleanup.interests_timeout) => { cleanup.run().await }
                        _ = cancellation_token.cancelled() => {}
                        _ = rejection_token.cancelled() => { cleanup.execute(false).await }
                    }
                });
        }
    }

    async fn execute(&mut self, print_warning: bool) {
        if let Some(mut face) = self.face.upgrade() {
            let ctrl_lock = self.tables.ctrl_lock.lock().await;
            if let Some(interest) = get_mut_unchecked(&mut face)
                .pending_current_interests
                .remove(&self.id)
            {
                drop(ctrl_lock);
                if print_warning {
                    tracing::warn!(
                        "{}:{} Didn't receive DeclareFinal for interest {:?}:{}: Timeout({:#?})!",
                        face,
                        self.id,
                        interest.interest.src.downcast_ref_to_face(),
                        interest.interest.src_interest_id,
                        self.interests_timeout,
                    );
                }
                let mut declares = vec![];
                finalize_pending_interest(interest, &mut |p, m| {
                    declares.push((p.clone(), m))
                });
                for (p, m) in declares {
                    let _ = p.send_declare(m.msg).await;
                }
            }
        }
    }
}

#[async_trait]
impl Timed for CurrentInterestCleanup {
    async fn run(&mut self) {
        self.execute(true).await;
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn declare_interest<'a>(
    hat_code: &(dyn HatTrait + Send + Sync),
    tables_ref: &Arc<TablesLock>,
    face: &mut Arc<FaceState>,
    id: InterestId,
    expr: Option<&'a WireExpr<'a>>,
    mode: InterestMode,
    options: InterestOptions,
    send_declare: &'a mut SendDeclare<'a>,
) {
    if options.keyexprs() && mode != InterestMode::Current {
        register_expr_interest(tables_ref, face, id, expr).await;
    }

    if let Some(expr) = expr {
        let rtables = tables_ref.tables.read().await;
        match rtables
            .get_mapping(face, &expr.scope, expr.mapping)
            .cloned()
        {
            Some(mut prefix) => {
                tracing::debug!(
                    "{} Declare interest {} ({}{})",
                    face,
                    id,
                    prefix.expr(),
                    expr.suffix
                );
                let res = Resource::get_resource(&prefix, &expr.suffix);
                let (mut res, mut wtables) =
                    if res.as_ref().map(|r| r.context.is_some()).unwrap_or(false) {
                        drop(rtables);
                        let wtables = tables_ref.tables.write().await;
                        (res.unwrap(), wtables)
                    } else {
                        let mut fullexpr = prefix.expr().to_string();
                        fullexpr.push_str(expr.suffix.as_ref());
                        let mut matches = keyexpr::new(fullexpr.as_str())
                            .map(|ke| Resource::get_matches(&rtables, ke))
                            .unwrap_or_default();
                        drop(rtables);
                        let mut wtables = tables_ref.tables.write().await;
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

                hat_code.declare_interest(
                    &mut wtables,
                    tables_ref,
                    face,
                    id,
                    Some(&mut res),
                    mode,
                    options,
                    send_declare,
                );
            }
            None => tracing::error!(
                "{} Declare interest {} for unknown scope {}!",
                face,
                id,
                expr.scope
            ),
        }
    } else {
        let mut wtables = tables_ref.tables.write().await;
        hat_code.declare_interest(
            &mut wtables,
            tables_ref,
            face,
            id,
            mode,
            options,
            wire_expr,
            ..
        } = msg;

        if options.keyexprs() && mode != &InterestMode::Current {
            register_expr_interest(
                &self.tables,
                &mut self.state.clone(),
                *id,
                wire_expr.as_ref(),
            );
        }

        self.with_mapped_optional_expr(wire_expr.as_ref(), |tables, res| {
            let hats = &mut tables.hats;

            let mut ctx = DispatcherContext {
                tables_lock: &self.tables,
                tables: &mut tables.data,
                src_face: &mut self.state.clone(),
                send_declare,
            };

            let Some(src) = hats[region].new_remote(ctx.src_face, msg.ext_nodeid.node_id) else {
                return;
            };

            let route_interest_res =
                hats[Region::North].route_interest(ctx.reborrow(), msg, res.clone(), &src);

            if msg.mode.is_current() {
                if msg.options.subscribers() {
                    let other_sub_matches = hats
                        .values()
                        .filter(|hat| hat.region() != region)
                        .flat_map(|hat| {
                            hat.remote_subscribers_matching(ctx.tables, res.as_deref())
                                .into_iter()
                        })
                        .collect::<HashMap<_, _>>();

                    hats[region].send_current_subscribers(
                        ctx.reborrow(),
                        msg,
                        res.clone(),
                        other_sub_matches,
                    );
                }

                if msg.options.queryables() {
                    let other_qabl_matches = hats
                        .values()
                        .filter(|hat| hat.region() != region)
                        .flat_map(|hat| {
                            hat.remote_queryables_matching(ctx.tables, res.as_deref())
                                .into_iter()
                        })
                        .collect::<HashMap<_, _>>();
                    hats[region].send_current_queryables(
                        ctx.reborrow(),
                        msg,
                        res.clone(),
                        other_qabl_matches,
                    );
                }

                if msg.options.tokens() {
                    let other_token_matches = hats
                        .values()
                        .filter(|hat| hat.region() != region)
                        .flat_map(|hat| {
                            hat.remote_tokens_matching(ctx.tables, res.as_deref())
                                .into_iter()
                        })
                        .collect::<HashSet<_>>();
                    hats[region].send_current_tokens(
                        ctx.reborrow(),
                        msg,
                        res.clone(),
                        other_token_matches,
                    );
                }
            }

            if msg.mode.is_future() {
                hats[region].register_interest(ctx.reborrow(), msg, res);
            }

            if let RouteInterestResult::ResolvedCurrentInterest = route_interest_res {
                hats[region].send_declare_final(ctx.reborrow(), msg.id, &src);
            }
        });
    }

    #[tracing::instrument(
        level = "debug",
        name = "interest",
        skip(self, msg),
        fields(
            id = msg.id,
            mode = ?InterestMode::Final,
            opts = %msg.options,
            expr = msg.wire_expr.as_ref().map(|we| we.to_string())
        ),
        ret
    )]
    pub(crate) fn interest_final(&self, msg: &Interest) {
        let mut wtables = zwrite!(self.tables.tables);
        let tables = &mut *wtables;

        let mut ctx = DispatcherContext {
            tables_lock: &self.tables,
            tables: &mut tables.data,
            src_face: &mut self.state.clone(),
            send_declare: &mut |_, _| unreachable!(),
        };

        // Unregister keyexpr interest
        get_mut_unchecked(ctx.src_face)
            .remote_key_interests
            .remove(&msg.id);

        let hats = &mut tables.hats;
        let region = ctx.src_face.region;

        let Some(remote_interest) = hats[region].unregister_interest(ctx.reborrow(), msg) else {
            return;
        };

        hats[Region::North].route_interest_final(ctx, msg, &remote_interest);
    }

    #[tracing::instrument(level = "debug", skip(self, wtables, _node_id, send_declare), ret)]
    pub(crate) fn declare_final(
        &self,
        wtables: &mut Tables,
        interest_id: InterestId,
        _node_id: NodeId,
        send_declare: &mut SendDeclare,
    ) {
        let tables = &mut *wtables;

        let mut ctx = DispatcherContext {
            tables_lock: &self.tables,
            tables: &mut tables.data,
            src_face: &mut self.state.clone(),
            send_declare,
        };

        let hats = &mut tables.hats;
        let region = ctx.src_face.region;

        if region.bound().is_south() {
            tracing::error!("Received DeclareFinal from south-bound face");
            return;
        }

        // TODO(regions): this is too conservative, the north hat should be able to decide what
        // keyexpr(s)—if not all—are affected and whether this finalization concerns subscribers
        // or queryables or borth.
        hats[region].disable_all_routes(ctx.tables);

        match hats[region].route_declare_final(ctx.reborrow(), interest_id) {
            RouteCurrentDeclareResult::Noop | RouteCurrentDeclareResult::NoBreadcrumb => {} // ¯\_(ツ)_/¯
            RouteCurrentDeclareResult::Breadcrumb { interest } => {
                debug_assert!(interest.mode.is_current());

                hats[interest.src_region].send_declare_final(
                    ctx,
                    interest.src_interest_id,
                    &interest.src,
                );
            }
        }
    }
}

pub(crate) async fn undeclare_interest(
    hat_code: &(dyn HatTrait + Send + Sync),
    tables: &TablesLock,
    face: &mut Arc<FaceState>,
    id: InterestId,
) {
    tracing::debug!("{} Undeclare interest {}", face, id,);
    unregister_expr_interest(tables, face, id).await;
    let mut wtables = tables.tables.write().await;
    hat_code.undeclare_interest(&mut wtables, face, id);
}
