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
    any::Any,
    cell::OnceCell,
    sync::{Arc, OnceLock},
};

use arc_swap::ArcSwap;
use async_trait::async_trait;
use zenoh_protocol::{
    core::Reliability,
    network::{
        interest::Interest, Declare, NetworkBodyMut, NetworkMessageExt as _, NetworkMessageMut,
        Push, Request, Response, ResponseFinal,
    },
};
use zenoh_transport::{multicast::TransportMulticast, unicast::TransportUnicast};

use super::Primitives;
use crate::net::routing::{
    dispatcher::face::{Face, WeakFace},
    gateway::{InterceptorCacheValueType, Resource},
    interceptor::{has_interceptor, InterceptorContext, InterceptorTrait, InterceptorsChain},
    RoutingContext,
};

pub struct Mux {
    pub handler: TransportUnicast,
    pub(crate) interceptor: ArcSwapOption<InterceptorsChain>,
    pub(crate) face: OnceLock<WeakFace>,
}

impl Mux {
    pub(crate) fn new(handler: TransportUnicast, interceptor: InterceptorsChain) -> Mux {
        Mux {
            handler,
            face: OnceLock::new(),
            interceptor: ArcSwapOption::new(interceptor.into()),
        }
    }

    #[inline(always)]
    fn can_schedule(&self, msg: &mut NetworkMessageMut) -> bool {
        if !has_interceptor(&self.interceptor) {
            return true;
        }
        match self.interceptor.load().as_ref() {
            Some(interceptor) => interceptor.intercept(
                msg,
                &mut MuxContext {
                    mux: self,
                    cache: OnceCell::new(),
                    expr: OnceCell::new(),
                },
            ),
            None => true,
        }
    }

    #[inline(always)]
    fn schedule(&self, mut msg: NetworkMessageMut) -> bool {
        self.can_schedule(&mut msg) && self.handler.schedule(msg).unwrap_or(false)
    }
}

struct MuxContext<'a> {
    mux: &'a Mux,
    cache: OnceCell<InterceptorCacheValueType>,
    expr: OnceCell<String>,
}

impl MuxContext<'_> {
    // Non-blocking prefix lookup using try_read(). Returns None if the lock is
    // contended (extremely rare on the read path) rather than blocking the caller.
    fn try_prefix<'a>(&self, msg: &'a NetworkMessageMut<'a>) -> Option<Arc<Resource>> {
        let wire_expr = msg.wire_expr()?;
        let wire_expr = wire_expr.to_owned();
        let face = self.mux.face.get().and_then(|f| f.upgrade())?;
        let tables = face.tables.tables.try_read()?;
        tables
            .get_sent_mapping(&face.state, &wire_expr.scope, wire_expr.mapping)
            .cloned()
    }
}

impl InterceptorContext for MuxContext<'_> {
    fn face(&self) -> Option<Face> {
        self.mux.face.get().and_then(|f| f.upgrade())
    }

    fn full_expr(&self, msg: &NetworkMessageMut) -> Option<&str> {
        if self.expr.get().is_none() {
            if let Some(wire_expr) = msg.wire_expr() {
                if let Some(prefix) = self.try_prefix(msg) {
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
            if let Some(prefix) = self.try_prefix(msg) {
                if let Some(face) = self.mux.face.get().and_then(|f| f.upgrade()) {
                    // TODO interceptor can change between the initial load and the cache load
                    if let Some(cache) = self
                        .mux
                        .interceptor
                        .load()
                        .as_ref()
                        .and_then(|i| prefix.get_egress_cache(&face, i))
                    {
                        self.cache.set(cache).ok();
                    }
                }
            }
        }
        self.cache.get().and_then(|c| c.get_ref().as_ref())
    }
}

// New unified async Primitives implementation for Mux
#[async_trait]
impl Primitives for Mux {
    async fn send_interest(&self, mut msg: Interest) -> bool {
        let interest_id = msg.id;

        let mut net_msg = NetworkMessageMut {
            body: NetworkBodyMut::Interest(&mut msg),
            reliability: Reliability::Reliable,
        };
        let mut ctx = RoutingContext {
            msg: (),
            full_expr: OnceCell::new(),
        };

        if self
            .interceptor
            .load()
            .intercept(&mut net_msg, &mut ctx as &mut dyn InterceptorContext)
        {
            self.handler.schedule(net_msg).await.unwrap_or(false)
        } else {
            // send declare final to avoid timeout on blocked interest
            if let Some(face) = self.face.get().and_then(|f| f.upgrade()) {
                face.reject_interest(interest_id);
            }
            false
        }
    }

    async fn send_declare(&self, mut msg: Declare) -> bool {
        let mut net_msg = NetworkMessageMut {
            body: NetworkBodyMut::Declare(&mut msg),
            reliability: Reliability::Reliable,
        };
        let mut ctx = RoutingContext {
            msg: (),
            full_expr: OnceCell::new(),
        };

        if self
            .interceptor
            .load()
            .intercept(&mut net_msg, &mut ctx as &mut dyn InterceptorContext)
        {
            self.handler.schedule(net_msg).await.unwrap_or(false)
        } else {
            false
        }
    }

    async fn send_push(&self, mut msg: Push, reliability: Reliability) -> bool {
        let mut net_msg = NetworkMessageMut {
            body: NetworkBodyMut::Push(&mut msg),
            reliability,
        };
        let mut ctx = MuxContext {
            mux: self,
            cache: OnceCell::new(),
            expr: OnceCell::new(),
        };
        let interceptor = self.interceptor.load();
        if interceptor.interceptors.is_empty()
            || interceptor.intercept(&mut net_msg, &mut ctx as &mut dyn InterceptorContext)
        {
            self.handler.schedule(net_msg).await.unwrap_or(false)
        } else {
            false
        }
    }

    async fn send_request(&self, mut msg: Request) -> bool {
        let request_id = msg.id;
        let mut net_msg = NetworkMessageMut {
            body: NetworkBodyMut::Request(&mut msg),
            reliability: Reliability::Reliable,
        };
        let mut ctx = MuxContext {
            mux: self,
            cache: OnceCell::new(),
            expr: OnceCell::new(),
        };
        let interceptor = self.interceptor.load();
        if interceptor.interceptors.is_empty() {
            self.handler.schedule(net_msg).await.unwrap_or(false)
        } else if let Some(face) = self.face.get().and_then(|f| f.upgrade()) {
            if interceptor.intercept(&mut net_msg, &mut ctx as &mut dyn InterceptorContext) {
                self.handler.schedule(net_msg).await.unwrap_or(false)
            } else {
                // request was blocked by an interceptor, send response final to avoid timeout
                face.send_response_final(ResponseFinal {
                    rid: request_id,
                    ext_qos: response::ext::QoSType::RESPONSE_FINAL,
                    ext_tstamp: None,
                })
                .await;
                false
            }
        } else {
            false
        }
    }

    async fn send_response(&self, mut msg: Response) -> bool {
        let mut net_msg = NetworkMessageMut {
            body: NetworkBodyMut::Response(&mut msg),
            reliability: Reliability::Reliable,
        };
        let mut ctx = MuxContext {
            mux: self,
            cache: OnceCell::new(),
            expr: OnceCell::new(),
        };
        let interceptor = self.interceptor.load();
        if interceptor.interceptors.is_empty()
            || interceptor.intercept(&mut net_msg, &mut ctx as &mut dyn InterceptorContext)
        {
            self.handler.schedule(net_msg).await.unwrap_or(false)
        } else {
            false
        }
    }

    async fn send_response_final(&self, mut msg: ResponseFinal) -> bool {
        let mut net_msg = NetworkMessageMut {
            body: NetworkBodyMut::ResponseFinal(&mut msg),
            reliability: Reliability::Reliable,
        };
        let mut ctx = MuxContext {
            mux: self,
            cache: OnceCell::new(),
            expr: OnceCell::new(),
        };
        let interceptor = self.interceptor.load();
        if interceptor.interceptors.is_empty()
            || interceptor.intercept(&mut net_msg, &mut ctx as &mut dyn InterceptorContext)
        {
            self.handler.schedule(net_msg).await.unwrap_or(false)
        } else {
            false
        }
    }

    async fn close(&self) {
        // Close implementation - currently no-op like send_close was
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

pub struct McastMux {
    pub handler: TransportMulticast,
    pub(crate) face: OnceLock<Face>,
    pub(crate) interceptor: ArcSwapOption<InterceptorsChain>,
}

impl McastMux {
    pub(crate) fn new(handler: TransportMulticast, interceptor: InterceptorsChain) -> McastMux {
        McastMux {
            handler,
            face: OnceLock::new(),
            interceptor: ArcSwapOption::new(interceptor.into()),
        }
    }

    #[inline(always)]
    fn can_schedule(&self, msg: &mut NetworkMessageMut) -> bool {
        match self.interceptor.load().as_ref() {
            Some(interceptor) => interceptor.intercept(
                msg,
                &mut McastMuxContext {
                    mux: self,
                    cache: OnceCell::new(),
                    expr: OnceCell::new(),
                },
            ),
            None => true,
        }
    }

    #[inline(always)]
    fn schedule(&self, mut msg: NetworkMessageMut) -> bool {
        self.can_schedule(&mut msg) && self.handler.schedule(msg).unwrap_or(false)
    }
}

struct McastMuxContext<'a> {
    mux: &'a McastMux,
    cache: OnceCell<InterceptorCacheValueType>,
    expr: OnceCell<String>,
}

impl McastMuxContext<'_> {
    fn try_prefix<'a>(&self, msg: &'a NetworkMessageMut<'a>) -> Option<Arc<Resource>> {
        let wire_expr = msg.wire_expr()?;
        let wire_expr = wire_expr.to_owned();
        let face = self.mux.face.get()?;
        let tables = face.tables.tables.try_read()?;
        tables
            .get_sent_mapping(&face.state, &wire_expr.scope, wire_expr.mapping)
            .cloned()
    }
}

impl InterceptorContext for McastMuxContext<'_> {
    fn face(&self) -> Option<Face> {
        self.mux.face.get().cloned()
    }

    fn full_expr(&self, msg: &NetworkMessageMut) -> Option<&str> {
        if self.expr.get().is_none() {
            if let Some(wire_expr) = msg.wire_expr() {
                if let Some(prefix) = self.try_prefix(msg) {
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
            if let Some(prefix) = self.try_prefix(msg) {
                if let Some(face) = self.mux.face.get() {
                    // TODO interceptor can change between the initial load and the cache load
                    if let Some(cache) = self
                        .mux
                        .interceptor
                        .load()
                        .as_ref()
                        .and_then(|i| prefix.get_egress_cache(face, i))
                    {
                        self.cache.set(cache).ok();
                    }
                }
            }
        }
        self.cache.get().and_then(|c| c.get_ref().as_ref())
    }
}

// New unified async Primitives implementation for McastMux
#[async_trait]
impl Primitives for McastMux {
    async fn send_interest(&self, mut msg: Interest) -> bool {
        let interest_id = msg.id;

        let mut net_msg = NetworkMessageMut {
            body: NetworkBodyMut::Interest(&mut msg),
            reliability: Reliability::Reliable,
        };
        let mut ctx = RoutingContext {
            msg: (),
            full_expr: OnceCell::new(),
        };

        if self
            .interceptor
            .load()
            .intercept(&mut net_msg, &mut ctx as &mut dyn InterceptorContext)
        {
            self.handler.schedule(net_msg).await.unwrap_or(false)
        } else {
            // send declare final to avoid timeout on blocked interest
            if let Some(face) = self.face.get() {
                face.reject_interest(interest_id);
            }
            false
        }
    }

    async fn send_declare(&self, mut msg: Declare) -> bool {
        let mut net_msg = NetworkMessageMut {
            body: NetworkBodyMut::Declare(&mut msg),
            reliability: Reliability::Reliable,
        };
        let mut ctx = RoutingContext {
            msg: (),
            full_expr: OnceCell::new(),
        };

        if self
            .interceptor
            .load()
            .intercept(&mut net_msg, &mut ctx as &mut dyn InterceptorContext)
        {
            self.handler.schedule(net_msg).await.unwrap_or(false)
        } else {
            false
        }
    }

    async fn send_push(&self, mut msg: Push, reliability: Reliability) -> bool {
        let mut net_msg = NetworkMessageMut {
            body: NetworkBodyMut::Push(&mut msg),
            reliability,
        };
        let mut ctx = McastMuxContext {
            mux: self,
            cache: OnceCell::new(),
            expr: OnceCell::new(),
        };
        let interceptor = self.interceptor.load();
        if interceptor.interceptors.is_empty()
            || interceptor.intercept(&mut net_msg, &mut ctx as &mut dyn InterceptorContext)
        {
            self.handler.schedule(net_msg).await.unwrap_or(false)
        } else {
            false
        }
    }

    async fn send_request(&self, mut msg: Request) -> bool {
        let request_id = msg.id;
        let mut net_msg = NetworkMessageMut {
            body: NetworkBodyMut::Request(&mut msg),
            reliability: Reliability::Reliable,
        };
        let mut ctx = McastMuxContext {
            mux: self,
            cache: OnceCell::new(),
            expr: OnceCell::new(),
        };
        let interceptor = self.interceptor.load();
        if interceptor.interceptors.is_empty() {
            self.handler.schedule(net_msg).await.unwrap_or(false)
        } else if let Some(face) = self.face.get() {
            if interceptor.intercept(&mut net_msg, &mut ctx as &mut dyn InterceptorContext) {
                self.handler.schedule(net_msg).await.unwrap_or(false)
            } else {
                // request was blocked by an interceptor, send response final to avoid timeout
                face.send_response_final(ResponseFinal {
                    rid: request_id,
                    ext_qos: response::ext::QoSType::RESPONSE_FINAL,
                    ext_tstamp: None,
                })
                .await;
                false
            }
        } else {
            false
        }
    }

    async fn send_response(&self, mut msg: Response) -> bool {
        let mut net_msg = NetworkMessageMut {
            body: NetworkBodyMut::Response(&mut msg),
            reliability: Reliability::Reliable,
        };
        let mut ctx = McastMuxContext {
            mux: self,
            cache: OnceCell::new(),
            expr: OnceCell::new(),
        };
        let interceptor = self.interceptor.load();
        if interceptor.interceptors.is_empty()
            || interceptor.intercept(&mut net_msg, &mut ctx as &mut dyn InterceptorContext)
        {
            self.handler.schedule(net_msg).await.unwrap_or(false)
        } else {
            false
        }
    }

    async fn send_response_final(&self, mut msg: ResponseFinal) -> bool {
        let mut net_msg = NetworkMessageMut {
            body: NetworkBodyMut::ResponseFinal(&mut msg),
            reliability: Reliability::Reliable,
        };
        let mut ctx = McastMuxContext {
            mux: self,
            cache: OnceCell::new(),
            expr: OnceCell::new(),
        };
        let interceptor = self.interceptor.load();
        if interceptor.interceptors.is_empty()
            || interceptor.intercept(&mut net_msg, &mut ctx as &mut dyn InterceptorContext)
        {
            self.handler.schedule(net_msg).await.unwrap_or(false)
        } else {
            false
        }
    }

    async fn close(&self) {
        // Close implementation - currently no-op
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
