use bytes::Buf;
use futures::{Async, Future, Poll};
use http;
use hyper::body::Payload;
use std::marker::PhantomData;

use proxy::Error;
use svc;

pub enum Fallback<P> {
    Rejected(http::Request<P>),
    Inner(Error),
}

#[derive(Debug, Clone)]
pub struct Layer<A, B, P> {
    primary_layer: A,
    fallback_layer: B,
    _marker: PhantomData<fn(P)>,
}

#[derive(Debug)]
pub struct MakeSvc<A, B, P> {
    primary_make: A,
    fallback_make: B,
    _marker: PhantomData<fn(P)>,
}

#[derive(Debug)]
pub struct MakeFuture<A, B, P> {
    primary: A,
    fallback: B,
    _marker: PhantomData<fn(P)>,
}

pub struct Service<A, B, P> {
    primary_service: A,
    fallback_service: B,
    _marker: PhantomData<fn(P)>,
}

#[derive(Debug)]
pub enum ResponseFuture<A, B, P>
where
    A: Future<Error = Fallback<P>>,
    B: svc::Service<http::Request<P>>,
    B::Error: Into<Error>,
    P: Payload,
{
    Primary {
        future: A,
        fallback: B,
    },
    FallbackPending {
        fallback: B,
        request: Option<http::Request<P>>,
    },
    Fallback(B::Future),
}

#[derive(Clone, Debug)]
pub enum Body<A, B> {
    A(A),
    B(B),
}

pub fn layer<A, B, P>(primary_layer: A, fallback_layer: B) -> Layer<A, B, P> {
    Layer {
        primary_layer,
        fallback_layer,
        _marker: PhantomData,
    }
}

// === impl Layer ===

impl<A, B, P, M> svc::Layer<M> for Layer<A, B, P>
where
    M: Clone,
    A: svc::Layer<M>,
    B: svc::Layer<M>,
{
    type Service = MakeSvc<A::Service, B::Service, P>;

    fn layer(&self, inner: M) -> Self::Service {
        MakeSvc {
            primary_make: self.primary_layer.layer(inner.clone()),
            fallback_make: self.fallback_layer.layer(inner),
            _marker: PhantomData,
        }
    }
}

// === impl MakeSvc ===

impl<A, B, P, T> svc::Service<T> for MakeSvc<A, B, P>
where
    A: svc::Service<T>,
    A::Response: svc::Service<http::Request<P>>,
    A::Error: Into<Error>,
    B: svc::Service<T>,
    B::Response: svc::Service<http::Request<P>>,
    B::Error: Into<Error>,
    T: Clone,
{
    type Response = Service<A::Response, B::Response, P>;
    type Future = MakeFuture<A::Future, B::Future, P>;
    type Error = Error;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        let p = self.primary_make.poll_ready().map_err(Into::into)?;
        let f = self.fallback_make.poll_ready().map_err(Into::into)?;
        if p.is_ready() && f.is_ready() {
            Ok(Async::Ready(()))
        } else {
            Ok(Async::NotReady)
        }
    }

    fn call(&mut self, target: T) -> Self::Future {
        let primary = self.primary_make.call(target.clone());
        let fallback = self.fallback_make.call(target);

        MakeFuture {
            primary,
            fallback,
            _marker: PhantomData,
        }
    }
}

impl<A, B, P> Clone for MakeSvc<A, B, P>
where
    A: Clone,
    B: Clone,
{
    fn clone(&self) -> Self {
        Self {
            primary_make: self.primary_make.clone(),
            fallback_make: self.fallback_make.clone(),
            _marker: PhantomData,
        }
    }
}

// === impl MakeSvc ===

impl<A, B, P> Future for MakeFuture<A, B, P>
where
    A: Future,
    A::Item: svc::Service<http::Request<P>>,
    B: Future,
    B::Item: svc::Service<http::Request<P>>,
{
    type Item = Service<A::Item, B::Item, P>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let primary_service = try_ready!(self.primary.poll().map_err(Into::into));
        let fallback_service = try_ready!(self.fallback.poll().map_err(Into::into));

        let svc = Service {
            primary_service,
            fallback_service,
            _marker: PhantomData,
        };
        Ok(svc.into())
    }
}

// === impl Service ===

impl<A, B, P, Q, R> svc::Service<http::Request<P>> for Service<A, B, P>
where
    P: Payload,
    P::Error: Into<Error>,
    A: svc::Service<http::Request<P>, Response = http::Response<Q>, Error = Fallback<P>>,
    Q: Payload,
    Q::Error: Into<Error>,
    B: svc::Service<http::Request<P>, Response = http::Response<R>> + Clone,
    B::Error: Into<Error>,
    R: Payload,
    R::Error: Into<Error>,
{
    type Response = http::Response<Body<Q, R>>;
    type Error = Error;
    type Future = ResponseFuture<A::Future, B, P>;

    fn poll_ready(&mut self) -> Poll<(), Self::Error> {
        use svc::Service;

        match self.primary_service.poll_ready() {
            Ok(ready) => Ok(ready),
            Err(Fallback::Inner(e)) => Err(e),
            Err(Fallback::Rejected(_)) => unreachable!("poll_ready must not reject a request"),
        }
    }

    fn call(&mut self, req: http::Request<P>) -> Self::Future {
        use svc::Service;

        let future = self.primary_service.call(req);
        let fallback = self.fallback_service.clone();
        ResponseFuture::Primary { future, fallback }
    }
}

// === impl ResponseFuture ===

impl<A, B, P, Q, R> Future for ResponseFuture<A, B, P>
where
    A: Future<Item = http::Response<Q>, Error = Fallback<P>>,
    B: svc::Service<http::Request<P>, Response = http::Response<R>> + Clone,
    B::Error: Into<Error>,
    P: Payload,
    Q: Payload,
    R: Payload,
{
    type Item = http::Response<Body<Q, R>>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            *self = match self {
                ResponseFuture::Primary {
                    ref mut future,
                    ref fallback,
                } => match future.poll() {
                    Ok(Async::Ready(rsp)) => return Ok(rsp.map(Body::A).into()),
                    Err(Fallback::Inner(e)) => return Err(e),
                    Err(Fallback::Rejected(req)) => ResponseFuture::FallbackPending {
                        fallback: fallback.clone(),
                        request: Some(req),
                    },
                },

                ResponseFuture::FallbackPending {
                    ref mut fallback,
                    ref mut request,
                } => {
                    try_ready!(fallback.poll_ready().map_err(Into::into));
                    let req = request.take().expect("poll after ready");
                    ResponseFuture::Fallback(fallback.call(req))
                }

                ResponseFuture::Fallback(ref mut f) => {
                    let rsp = try_ready!(f.poll().map_err(Into::into));
                    return Ok(rsp.map(Body::B).into());
                }
            }
        }
    }
}

// === impl Body ===

impl<A, B> Payload for Body<A, B>
where
    A: Payload,
    B: Payload<Error = A::Error>,
{
    type Data = Body<A::Data, B::Data>;
    type Error = A::Error;

    fn poll_data(&mut self) -> Poll<Option<Self::Data>, Self::Error> {
        match self {
            Body::A(ref mut body) => body.poll_data().map(|r| r.map(|o| o.map(Body::A))),
            Body::B(ref mut body) => body.poll_data().map(|r| r.map(|o| o.map(Body::B))),
        }
    }

    fn poll_trailers(&mut self) -> Poll<Option<http::HeaderMap>, Self::Error> {
        match self {
            Body::A(ref mut body) => body.poll_trailers(),
            Body::B(ref mut body) => body.poll_trailers(),
        }
    }

    fn is_end_stream(&self) -> bool {
        match self {
            Body::A(ref body) => body.is_end_stream(),
            Body::B(ref body) => body.is_end_stream(),
        }
    }
}

impl<A, B: Default> Default for Body<A, B> {
    fn default() -> Self {
        Body::B(Default::default())
    }
}

impl<A, B> Body<A, B>
where
    A: Payload,
    B: Payload<Error = A::Error>,
{
    fn rsp_a(rsp: http::Response<A>) -> http::Response<Self> {
        rsp.map(Body::A)
    }

    fn rsp_b(rsp: http::Response<B>) -> http::Response<Self> {
        rsp.map(Body::B)
    }
}

impl<A, B> Buf for Body<A, B>
where
    A: Buf,
    B: Buf,
{
    fn remaining(&self) -> usize {
        match self {
            Body::A(ref buf) => buf.remaining(),
            Body::B(ref buf) => buf.remaining(),
        }
    }

    fn bytes(&self) -> &[u8] {
        match self {
            Body::A(ref buf) => buf.bytes(),
            Body::B(ref buf) => buf.bytes(),
        }
    }

    fn advance(&mut self, cnt: usize) {
        match self {
            Body::A(ref mut buf) => buf.advance(cnt),
            Body::B(ref mut buf) => buf.advance(cnt),
        }
    }
}
