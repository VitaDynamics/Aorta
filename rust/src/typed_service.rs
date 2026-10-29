use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use flatbuffers::FlatBufferBuilder;

use crate::client::Client;
use crate::header::{HeaderValues, TypedPublishMessage};
use crate::service::ServiceRequest;
use crate::zbytes::with_contiguous_bytes;
use crate::{unix_time_ns, AortaResult, SchemaMeta, TypedSubscribeMessage};

/// Marker trait for typed RPC **request** schemas: the schema must support
/// FlatBuffers building (for sending) and decoding (for server-side
/// parsing) in addition to [`SchemaMeta`]. Implemented automatically for
/// any type satisfying the supertraits.
pub trait TypedRpcRequestMessage: SchemaMeta + TypedPublishMessage + TypedSubscribeMessage {}

impl<T> TypedRpcRequestMessage for T where
    T: SchemaMeta + TypedPublishMessage + TypedSubscribeMessage
{
}

/// Marker trait for typed RPC **response** schemas, mirroring
/// [`TypedRpcRequestMessage`] on the reply side.
pub trait TypedRpcResponseMessage:
    SchemaMeta + TypedPublishMessage + TypedSubscribeMessage
{
}

impl<T> TypedRpcResponseMessage for T where
    T: SchemaMeta + TypedPublishMessage + TypedSubscribeMessage
{
}

fn build_typed_payload<M>(publisher_node: &str, sequence: u64, msg: &M::Msg<'_>) -> Vec<u8>
where
    M: TypedPublishMessage,
{
    let mut builder = FlatBufferBuilder::new();
    let header = M::build_header(
        &mut builder,
        &HeaderValues {
            publish_stamp_ns: unix_time_ns(),
            publisher_node,
            sequence,
        },
    );
    let root = M::build_typed_root(&mut builder, header, msg);
    builder.finish(root, None);
    builder.finished_data().to_vec()
}

pub(crate) fn validate_typed_client_options<Request, Response>(
    options: &mut crate::ClientOptions,
) -> AortaResult<()>
where
    Request: TypedRpcRequestMessage,
    Response: TypedRpcResponseMessage,
{
    let typed_request_schema = Request::schema_hash();
    let typed_response_schema = Response::schema_hash();
    if typed_request_schema != typed_response_schema {
        return Err(crate::AortaError::invalid_argument(
            "typed client request/response must use the same service schema hash",
        ));
    }

    let typed_request_root_type = Request::root_type().ok_or_else(|| {
        crate::AortaError::invalid_argument(
            "typed client request schema must declare root_type() for schema_by_key metadata",
        )
    })?;
    let typed_response_root_type = Response::root_type().ok_or_else(|| {
        crate::AortaError::invalid_argument(
            "typed client response schema must declare root_type() for schema_by_key metadata",
        )
    })?;

    match options.service_schema {
        Some(schema) if schema != typed_request_schema => {
            return Err(crate::AortaError::invalid_argument(
                "typed client service schema hash does not match request/response schema type",
            ));
        }
        None => {
            options.service_schema = Some(typed_request_schema);
        }
        _ => {}
    }

    match options.request_root_type.as_deref() {
        Some(value) if value != typed_request_root_type => {
            return Err(crate::AortaError::invalid_argument(
                "typed client request root type does not match request schema type",
            ));
        }
        None => {
            options.request_root_type = Some(typed_request_root_type.to_string());
        }
        _ => {}
    }

    match options.response_root_type.as_deref() {
        Some(value) if value != typed_response_root_type => {
            return Err(crate::AortaError::invalid_argument(
                "typed client response root type does not match response schema type",
            ));
        }
        None => {
            options.response_root_type = Some(typed_response_root_type.to_string());
        }
        _ => {}
    }

    Ok(())
}

/// Typed reply side of a service request, delivered to handlers of
/// [`Node::create_service_typed_view`](crate::Node::create_service_typed_view)
/// (and retained variants).
///
/// All `reply*` methods consume the responder, so a request can be replied
/// to **at most once** (compile-time enforced); dropping the responder
/// without replying leaves the client to time out. `reply` / `reply_message`
/// inject an Aorta header (unix timestamp, node name, per-service-instance
/// sequence) and encode the response schema; `reply_bytes` sends a raw
/// payload; `reply_error` answers with a textual error.
pub struct TypedServiceResponder<Response: TypedRpcResponseMessage> {
    request: ServiceRequest,
    node_name: Arc<str>,
    sequence: Arc<AtomicU64>,
    _marker: PhantomData<Response>,
}

impl<Response: TypedRpcResponseMessage> TypedServiceResponder<Response> {
    pub(crate) fn new(
        request: ServiceRequest,
        node_name: Arc<str>,
        sequence: Arc<AtomicU64>,
    ) -> Self {
        Self {
            request,
            node_name,
            sequence,
            _marker: PhantomData,
        }
    }

    /// Returns the caller namespace from the underlying service request.
    pub fn caller_namespace(&self) -> Option<&str> {
        self.request.caller_namespace()
    }

    /// Build a typed response message via `fill` and reply with it.
    pub async fn reply<F>(self, fill: F) -> AortaResult<()>
    where
        F: for<'a> FnOnce(&mut Response::Msg<'a>),
    {
        let mut msg = Response::Msg::default();
        fill(&mut msg);
        self.reply_message(&msg).await
    }

    /// Reply with a pre-built typed response message (header injection and
    /// encoding applied).
    pub async fn reply_message(self, msg: &Response::Msg<'_>) -> AortaResult<()> {
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
        let payload = build_typed_payload::<Response>(&self.node_name, sequence, msg);
        self.request.reply(payload).await
    }

    /// Reply with a raw byte payload (no header injection / encoding).
    pub async fn reply_bytes<IntoBytes>(self, response: IntoBytes) -> AortaResult<()>
    where
        IntoBytes: Into<crate::ZBytes>,
    {
        self.request.reply(response).await
    }

    /// Reply with a textual error message.
    pub async fn reply_error(self, error_msg: &str) -> AortaResult<()> {
        self.request.reply_error(error_msg).await
    }
}

/// Typed service request with an **owned** payload, delivered to handlers
/// of [`Node::create_service_typed_retained`](crate::Node::create_service_typed_retained).
///
/// Unlike the borrowed view in the `_typed_view` variant, the payload is
/// copied out of the transport buffer: the request can be decoded later
/// (even from another task) and still carries its responder for the
/// at-most-once reply.
pub struct TypedRetainedServiceRequest<
    Request: TypedRpcRequestMessage,
    Response: TypedRpcResponseMessage,
> {
    payload: Vec<u8>,
    responder: TypedServiceResponder<Response>,
    _marker: PhantomData<Request>,
}

impl<Request, Response> TypedRetainedServiceRequest<Request, Response>
where
    Request: TypedRpcRequestMessage,
    Response: TypedRpcResponseMessage,
{
    pub(crate) fn new(
        payload: Vec<u8>,
        request: ServiceRequest,
        node_name: Arc<str>,
        sequence: Arc<AtomicU64>,
    ) -> Self {
        Self {
            payload,
            responder: TypedServiceResponder::new(request, node_name, sequence),
            _marker: PhantomData,
        }
    }

    /// The raw (still-encoded) request payload bytes.
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// Returns the caller namespace from the underlying service request.
    pub fn caller_namespace(&self) -> Option<&str> {
        self.responder.caller_namespace()
    }

    /// Decode the request into its typed root view (borrows the request —
    /// keep it alive while using the view).
    pub fn decode(&self) -> AortaResult<<Request as TypedSubscribeMessage>::Root<'_>> {
        Request::decode_typed_root(&self.payload)
    }

    /// Build a typed response via `fill` and reply, consuming the request.
    pub async fn reply<F>(self, fill: F) -> AortaResult<()>
    where
        F: for<'a> FnOnce(&mut Response::Msg<'a>),
    {
        self.responder.reply(fill).await
    }

    /// Reply with a pre-built typed response message, consuming the request.
    pub async fn reply_message(self, msg: &Response::Msg<'_>) -> AortaResult<()> {
        self.responder.reply_message(msg).await
    }

    /// Reply with a textual error message, consuming the request.
    pub async fn reply_error(self, error_msg: &str) -> AortaResult<()> {
        self.responder.reply_error(error_msg).await
    }
}

/// Typed RPC client for a `{Request, Response}` schema pair, created via
/// [`Node::create_client_typed`](crate::Node::create_client_typed).
///
/// `call` builds the typed request (header + schema encoding), performs the
/// RPC with the client's timeout, and returns a [`TypedClientResponse`]
/// owning the reply payload. The client is `Clone`-free; share it behind an
/// `Arc` if needed. The raw wrapper is reachable via [`raw`](Self::raw) for
/// out-of-band operations.
pub struct TypedClient<Request: TypedRpcRequestMessage, Response: TypedRpcResponseMessage> {
    inner: Client,
    node_name: Arc<str>,
    sequence: AtomicU64,
    _marker: PhantomData<(Request, Response)>,
}

impl<Request, Response> TypedClient<Request, Response>
where
    Request: TypedRpcRequestMessage,
    Response: TypedRpcResponseMessage,
{
    pub(crate) fn new(inner: Client, node_name: Arc<str>) -> Self {
        Self {
            inner,
            node_name,
            sequence: AtomicU64::new(0),
            _marker: PhantomData,
        }
    }

    /// Build the typed request message via `fill`, send it, and await the
    /// typed response. Applies the client's timeout; returns
    /// [`AortaError::Timeout`](crate::AortaError) on expiry.
    pub async fn call<F>(&self, fill: F) -> AortaResult<TypedClientResponse<Response>>
    where
        F: for<'a> FnOnce(&mut Request::Msg<'a>),
    {
        let mut msg = Request::Msg::default();
        fill(&mut msg);
        self.call_message(&msg).await
    }

    /// Send a pre-built typed request message and await the response.
    pub async fn call_message(
        &self,
        msg: &Request::Msg<'_>,
    ) -> AortaResult<TypedClientResponse<Response>> {
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
        let request = build_typed_payload::<Request>(&self.node_name, sequence, msg);
        let response = self.inner.call(request).await?;
        let payload = with_contiguous_bytes(&response, |bytes| bytes.to_vec());
        Ok(TypedClientResponse {
            payload,
            _marker: PhantomData,
        })
    }

    /// Access the underlying raw [`Client`] (for raw calls, stats, and
    /// readiness checks). Options are fixed at construction — they cannot
    /// be changed through this handle.
    pub fn raw(&self) -> &Client {
        &self.inner
    }
}

/// Owned typed reply of a [`TypedClient::call`]: the payload is copied out
/// of the transport buffer, so the response (and anything decoded from it)
/// is independent of the `ZBytes` lifetime.
pub struct TypedClientResponse<Response: TypedRpcResponseMessage> {
    payload: Vec<u8>,
    _marker: PhantomData<Response>,
}

impl<Response: TypedRpcResponseMessage> TypedClientResponse<Response> {
    /// The raw (still-encoded) response payload bytes.
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// Decode the response into its typed root view (borrows the response).
    pub fn decode(&self) -> AortaResult<<Response as TypedSubscribeMessage>::Root<'_>> {
        Response::decode_typed_root(&self.payload)
    }

    /// Consume the response and return the owned payload bytes.
    pub fn into_payload(self) -> Vec<u8> {
        self.payload
    }
}
