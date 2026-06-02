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
use std::{any::Any, cell::OnceCell, sync::Arc};

use arc_swap::ArcSwapOption;
use async_trait::async_trait;
use zenoh_link::Link;
use zenoh_protocol::{
    core::ZenohIdProto,
    network::{
        ext, response, Declare, DeclareBody, DeclareFinal, NetworkBodyMut, NetworkMessageExt as _,
        NetworkMessageMut, ResponseFinal,
    },
};
use zenoh_result::ZResult;

use zenoh_transport::{
    unicast::TransportUnicast, MessageHandlerAsync, TransportPeerEventHandler,
};

use super::Primitives;
use crate::net::routing::{
    dispatcher::face::Face,
    gateway::{InterceptorCacheValueType, Resource},
    hat::DispatcherContext,
    interceptor::{has_interceptor, InterceptorContext, InterceptorTrait, InterceptorsChain},
    RoutingContext,
};

pub struct DeMux {
    pub(crate) face: Face,
    pub(crate) transport: Option<TransportUnicast>,
    pub(crate) interceptor: Arc<ArcSwapOption<InterceptorsChain>>,
    zid: ZenohIdProto,
}

impl DeMux {
    pub(crate) fn new(
        face: Face,
        transport: Option<TransportUnicast>,
        interceptor: Arc<ArcSwapOption<InterceptorsChain>>,
        zid: ZenohIdProto,
    ) -> Self {
        Self {
            face,
            transport,
            interceptor,
            zid,
        }
    }
}

struct DeMuxContext<'a> {
    demux: &'a DeMux,
    cache: OnceCell<InterceptorCacheValueType>,
    expr: OnceCell<String>,
}

impl DeMuxContext<'_> {
    fn prefix(&self, msg: &NetworkMessageMut) -> Option<Arc<Resource>> {
        let wire_expr = msg.wire_expr()?;
        let wire_expr = wire_expr.to_owned();
        let tables = self.demux.face.tables.tables.try_read()?;
        tables
            .data
            .get_mapping(&self.demux.face.state, &wire_expr.scope, wire_expr.mapping)
            .cloned()
    }
}

impl InterceptorContext for DeMuxContext<'_> {
    fn face(&self) -> Option<Face> {
        Some(self.demux.face.clone())
    }

    fn full_expr(&self, msg: &NetworkMessageMut) -> Option<&str> {
        if self.expr.get().is_none() {
            if let Some(wire_expr) = msg.wire_expr() {
                if let Some(prefix) = self.prefix(msg) {
                    self.expr
                        .set(prefix.expr().to_string() + wire_expr.suffix.as_ref())
                        .ok();
                }
            }
        }
        self.expr.get().map(|x| x.as_str())
    }
    fn get_cache(&self, msg: &NetworkMessageMut) -> Option<&Box<dyn Any + Send + Sync>> {
        if self.cache.get().is_none() && msg.wire_expr().is_some_and(|we| !we.has_suffix()) {
            if let Some(prefix) = self.prefix(msg) {
                // TODO interceptor can change between the initial load and the cache load
                if let Some(cache) = self
                    .demux
                    .interceptor
                    .load()
                    .as_ref()
                    .and_then(|i| prefix.get_ingress_cache(&self.demux.face, i))
                {
                    self.cache.set(cache).ok();
                }
            }
        }
        self.cache.get().and_then(|c| c.get_ref().as_ref())
    }
}

impl TransportPeerEventHandler for DeMux {
    fn handle_message(&self, mut msg: NetworkMessageMut) -> ZResult<()> {
        let _span = tracing::enabled!(tracing::Level::DEBUG).then(|| {
            tracing::debug_span!(
                "demux",
                zid = %self.zid,
                src = %self.face
            )
            .entered()
        });

        if has_interceptor(&self.interceptor) {
            if let Some(interceptor) = self.interceptor.load().as_ref() {
                let mut ctx = DeMuxContext {
                    demux: self,
                    cache: OnceCell::new(),
                    expr: OnceCell::new(),
                };

                match &msg.body {
                    NetworkBodyMut::Request(request) => {
                        let request_id = request.id;
                        if !interceptor.intercept(&mut msg, &mut ctx as &mut dyn InterceptorContext) {
                            // request was blocked by an interceptor, we need to send response final to avoid timeout error
                            let primitives = self.face.state.primitives.clone();
                            tokio::spawn(async move {
                                primitives.send_response_final(ResponseFinal {
                                    rid: request_id,
                                    ext_qos: response::ext::QoSType::DEFAULT,
                                    ext_tstamp: None,
                                }).await;
                            });
                            return Ok(());
                        }
                    }
                    NetworkBodyMut::Interest(interest) => {
                        let interest_id = interest.id;
                        if !interceptor.intercept(&mut msg, &mut ctx as &mut dyn InterceptorContext) {
                            // request was blocked by an interceptor, we need to send declare final to avoid timeout error
                            let primitives = self.face.state.primitives.clone();
                            tokio::spawn(async move {
                                let ctx = RoutingContext::new(
                                    Declare {
                                        interest_id: Some(interest_id),
                                        ext_qos: ext::QoSType::DECLARE,
                                        ext_tstamp: None,
                                        ext_nodeid: ext::NodeIdType::DEFAULT,
                                        body: DeclareBody::DeclareFinal(DeclareFinal),
                                    },
                                );
                                primitives.send_declare(ctx.msg).await;
                            });
                            return Ok(());
                        }
                    }
                    _ => {
                        if !interceptor.intercept(&mut msg, &mut ctx as &mut dyn InterceptorContext) {
                            return Ok(());
                        }
                    }
                };
            }
        }

        let face = self.face.clone();
        match msg.body {
            NetworkBodyMut::Push(m) => {
                let reliability = msg.reliability;
                let msg = m.clone();
                tokio::spawn(async move { face.send_push(msg, reliability).await });
            }
            // Control-plane messages (Declare, Interest) must be processed in order:
            // spawning each as a separate task creates races where DeclareSubscriber
            // executes before DeclareKeyExpr for the same scope. Use block_in_place
            // so handle_message blocks until the declaration is registered, preserving
            // wire ordering without blocking a tokio worker thread.
            NetworkBodyMut::Declare(m) => {
                let msg = m.clone();
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(face.send_declare(msg))
                });
            }
            NetworkBodyMut::Interest(m) => {
                let msg = m.clone();
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(face.send_interest(msg))
                });
            }
            NetworkBodyMut::Request(m) => {
                let msg = m.clone();
                tokio::spawn(async move { face.send_request(msg).await });
            }
            // Response and ResponseFinal must be processed in wire order:
            // ResponseFinal removes the pending query, so if it ran before Response
            // the routing would warn "Query not found". Use block_in_place to
            // serialize them, same as Declare/Interest.
            NetworkBodyMut::Response(m) => {
                let msg = m.clone();
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(face.send_response(msg))
                });
            }
            NetworkBodyMut::ResponseFinal(m) => {
                let msg = m.clone();
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(face.send_response_final(msg))
                });
            }
            NetworkBodyMut::OAM(m) => {
                if self.transport.is_some() {
                    // Spawn async work since TransportPeerEventHandler trait methods are sync
                    let face = self.face.clone();
                    let mut oam = m.clone();
                    tokio::spawn(async move {
                        use crate::net::routing::hat::{DispatcherContext as DC, HatTrait};
                        type DeclareVec = Vec<(
                            Arc<dyn super::Primitives + Send + Sync>,
                            RoutingContext<zenoh_protocol::network::Declare>,
                        )>;
                        let mut declares: DeclareVec = vec![];
                        let ctrl_lock = face.tables.ctrl_lock.lock().await;
                        let mut wtables = face.tables.tables.write().await;
                        let tables = &mut *wtables;
                        let region = face.state.region;
                        let (owner_hat, other_hats) =
                            match tables.hats.partition_mut(&region) {
                                Some(pair) => pair,
                                None => {
                                    tracing::error!("OAM: no hat for region {:?}", region);
                                    return;
                                }
                            };
                        let mut face_state_arc = face.state.clone();
                        let ctx = DC {
                            tables_lock: &face.tables,
                            tables: &mut tables.data,
                            src_face: &mut face_state_arc,
                            send_declare: &mut |p, m| declares.push((p.clone(), m)),
                        };
                        if let Err(e) = owner_hat.handle_oam(
                            ctx,
                            &mut oam,
                            other_hats.map(|hat| hat.as_mut() as &mut dyn HatTrait),
                        ) {
                            tracing::error!("Error handling OAM: {}", e);
                        }
                        drop(wtables);
                        drop(ctrl_lock);
                        for (p, m) in declares {
                            let _ = p.send_declare(m.msg).await;
                        }
                    });
                }
            }
        }

        Ok(())
    }

    fn new_link(&self, _link: Link) {}

    fn del_link(&self, _link: Link) {}

    fn closed(&self) {
        // Spawn async close in background since this trait method is sync
        let face = self.face.clone();
        tokio::spawn(async move {
            face.close().await;
        });
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Async message handler — used by the single-task RX driver (non-uring path).
///
/// This simply delegates to `handle_message`, which already uses `block_in_place`
/// for synchronous-ordering messages and spawns tasks for fire-and-forget ones.
#[async_trait]
impl MessageHandlerAsync for DeMux {
    async fn on_message(&self, msg: NetworkMessageMut<'_>) -> ZResult<()> {
        self.handle_message(msg)
    }
}
