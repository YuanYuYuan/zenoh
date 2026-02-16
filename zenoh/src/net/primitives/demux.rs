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
use zenoh_link::Link;
use zenoh_protocol::{
    core::ZenohIdProto,
    network::{
        ext, Declare, DeclareBody, DeclareFinal, NetworkBodyMut, NetworkMessageExt as _,
        NetworkMessageMut, ResponseFinal,
    },
};
use zenoh_result::ZResult;
use zenoh_runtime::ZRuntime;
use zenoh_transport::{unicast::TransportUnicast, TransportPeerEventHandler};

use super::Primitives;
use crate::net::routing::{
    dispatcher::face::Face,
    gateway::{InterceptorCacheValueType, Resource},
    hat::{DispatcherContext, HatTrait},
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
        if let Some(wire_expr) = msg.wire_expr() {
            let wire_expr = wire_expr.to_owned();
            // Note: Using blocking lock here since InterceptorContext trait methods are sync
            use futures::executor::block_on;
            let tables = block_on(self.demux.face.tables.tables.read());
            if let Some(prefix) = tables
                .get_mapping(&self.demux.face.state, &wire_expr.scope, wire_expr.mapping)
                .cloned()
            {
                return Some(prefix);
            }
        }
        None
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
                                    ext_qos: response::ext::QoSType::RESPONSE_FINAL,
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
                ZRuntime::RX.spawn(async move { face.send_push(msg, reliability).await });
            }
            NetworkBodyMut::Declare(m) => {
                let msg = m.clone();
                ZRuntime::RX.spawn(async move { face.send_declare(msg).await });
            }
            NetworkBodyMut::Interest(m) => {
                let msg = m.clone();
                ZRuntime::RX.spawn(async move { face.send_interest(msg).await });
            }
            NetworkBodyMut::Request(m) => {
                let msg = m.clone();
                ZRuntime::RX.spawn(async move { face.send_request(msg).await });
            }
            NetworkBodyMut::Response(m) => {
                let msg = m.clone();
                ZRuntime::RX.spawn(async move { face.send_response(msg).await });
            }
            NetworkBodyMut::ResponseFinal(m) => {
                let msg = m.clone();
                ZRuntime::RX.spawn(async move { face.send_response_final(msg).await });
            }
            NetworkBodyMut::OAM(m) => {
                if let Some(transport) = self.transport.as_ref() {
                    // Spawn async work since TransportPeerEventHandler trait methods are sync
                    let face = self.face.clone();
                    let transport = transport.clone();
                    let mut oam = m.clone();
                    ZRuntime::RX.spawn(async move {
                        let mut declares = vec![];
                        let ctrl_lock = zasynclock!(face.tables.ctrl_lock);
                        let mut tables = zasyncwrite!(face.tables.tables);
                        if let Err(e) = face.tables.hat_code.handle_oam(
                            &mut tables,
                            &face.tables,
                            &mut oam,
                            &transport,
                            &mut |p, m| declares.push((p.clone(), m)),
                        ) {
                            tracing::error!("Error handling OAM: {}", e);
                        }
                        drop(tables);
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
        ZRuntime::RX.spawn(async move {
            face.close().await;
        });
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
