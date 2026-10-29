//! Native-Zenoh interop conformance tests (Foreign Participant Profile).
//!
//! Pins the frozen native-Zenoh service interop wire contract: a NON-Aorta
//! zenoh peer — modeled here by a bare `zenoh::open`
//! session with hand-built literal keys, no attachment, no FlatBuffers —
//! must be able to (a) call an Aorta-declared raw service and (b) serve an
//! Aorta raw client, with payload bytes passing through verbatim and errors
//! carried by native `reply_err`.
//!
//! A failure here means the change broke external native-zenoh clients
//! (e.g. the phone app): amend the interop profile and coordinate the
//! external release — do not just "fix" the test.

use crate::client::Client;
use crate::error::{AortaError, AortaResult};
use crate::service::Service;
use crate::session::Session;
use crate::test_support::EnvVarGuard;
use crate::transport::reply_is_zenoh_query_timeout;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use zenoh::config::{EndPoint, WhatAmI};
use zenoh::query::{ConsolidationMode, QueryTarget};

const ROUTER_CONFIG: &str = include_str!("zenoh_router_config.json5");

/// The literal wire key a foreign peer hand-builds (contract point 1). A
/// native client has no `KeyExprBuilder`; the stability of this exact string
/// shape — `aorta/{group}/srv/{name}`, no category segment — is the contract.
fn foreign_service_key(name: &str) -> String {
    format!("aorta/default/srv/{name}")
}

fn unique_name(base: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{}_p{}_{}", base, std::process::id(), id)
}

/// Request bytes shaped like a protobuf message (varint field #1 = 150),
/// deliberately NOT a valid FlatBuffer.
const PROTOBUF_SHAPED_REQUEST: &[u8] = &[0x08, 0x96, 0x01];

async fn open_router_session() -> AortaResult<(Session, EndPoint)> {
    let mut config = zenoh::Config::from_json5(ROUTER_CONFIG)
        .map_err(|err| AortaError::internal(format!("router config error: {:?}", err)))?;
    config
        .set_mode(Some(WhatAmI::Router))
        .map_err(|err| AortaError::internal(format!("router mode error: {:?}", err)))?;
    let session = Session::open(config).await?;

    // Router locator publication is asynchronous. Poll until a tcp locator appears.
    let started = tokio::time::Instant::now();
    loop {
        let locators = session.inner().info().locators().await;
        if let Some(locator) = locators
            .iter()
            .find(|loc| loc.to_string().starts_with("tcp/"))
        {
            return Ok((session, EndPoint::from(locator.clone())));
        }
        if started.elapsed() >= Duration::from_secs(5) {
            return Err(AortaError::timeout(
                "router locator not found within 5s".to_string(),
            ));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn peer_config(endpoint: &EndPoint) -> AortaResult<zenoh::Config> {
    let mut config = zenoh::Config::from_json5(
        r#"
{
  mode: "client",
  scouting: {
    multicast: { enabled: false }
  },
  transport: {
    shared_memory: { enabled: false }
  }
}
"#,
    )
    .map_err(|err| AortaError::internal(format!("peer config error: {:?}", err)))?;
    config
        .connect
        .endpoints
        .set(vec![endpoint.clone().into()])
        .map_err(|err| AortaError::internal(format!("peer connect error: {:?}", err)))?;
    Ok(config)
}

/// Aorta-side peer session (goes through the Aorta Session wrapper).
async fn open_aorta_session(endpoint: &EndPoint) -> AortaResult<Session> {
    Session::open(peer_config(endpoint)?).await
}

/// Foreign-side peer session: bare zenoh, exactly what a phone app has.
async fn open_native_session(endpoint: &EndPoint) -> AortaResult<zenoh::Session> {
    zenoh::open(peer_config(endpoint)?)
        .await
        .map_err(|err| AortaError::internal(format!("native session open error: {:?}", err)))
}

/// Issue one native `get` per the profile (target=All, consolidation=None,
/// explicit timeout, raw payload, NO attachment) and return the first reply,
/// retrying until the route to the provider is established.
async fn native_get_first_reply(
    native: &zenoh::Session,
    key: &str,
    payload: &[u8],
    target: QueryTarget,
    overall_timeout: Duration,
) -> AortaResult<Option<zenoh::query::Reply>> {
    let deadline = tokio::time::Instant::now() + overall_timeout;
    loop {
        let replies = native
            .get(key)
            .target(target)
            .consolidation(ConsolidationMode::None)
            .payload(payload)
            .timeout(Duration::from_millis(500))
            .await
            .map_err(|err| AortaError::internal(format!("native get error: {:?}", err)))?;
        if let Ok(reply) = replies.recv_async().await {
            // zenoh synthesizes a "Timeout" reply error through the reply
            // channel when the per-attempt timeout fires (see the dual-guard
            // docs on transport::reply_error_is_zenoh_query_timeout); that is
            // a route-race artifact, not a provider answer — keep retrying
            // until the overall deadline.
            if !reply_is_zenoh_query_timeout(&reply) {
                return Ok(Some(reply));
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return Ok(None);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Direction A + contract points 1/2/3: a foreign native client calls an
/// Aorta raw service; protobuf-shaped request bytes reach the handler
/// verbatim and the raw reply bytes come back verbatim.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_get_calls_aorta_raw_service_bytes_verbatim() -> AortaResult<()> {
    // Group is read from AORTA_GROUP; remove it so the Aorta side agrees with
    // the foreign peer's hand-built `aorta/default/...` literal.
    let _group_guard = EnvVarGuard::remove("AORTA_GROUP");

    let (router_session, router_endpoint) = open_router_session().await?;
    let service_session = open_aorta_session(&router_endpoint).await?;
    let native_session = open_native_session(&router_endpoint).await?;
    let service_name = unique_name("native_interop_echo");

    let service = Service::builder(service_session.clone(), &service_name)
        .handler(move |req| {
            let payload = req.payload().to_bytes().into_owned();
            tokio::spawn(async move {
                // Verbatim echo plus a marker so the assertion proves the
                // request bytes arrived unmodified AND untouched bytes flow back.
                let mut response = payload;
                response.extend_from_slice(b"|aorta-ok");
                let _ = req.reply(response).await;
            });
        })
        .build();
    let _running = service.start().await?;

    let key = foreign_service_key(&service_name);
    let reply = native_get_first_reply(
        &native_session,
        &key,
        PROTOBUF_SHAPED_REQUEST,
        QueryTarget::All,
        Duration::from_secs(6),
    )
    .await?
    .ok_or_else(|| AortaError::timeout("no reply from Aorta service within 6s".to_string()))?;

    let sample = reply
        .result()
        .map_err(|err| AortaError::internal(format!("expected OK reply, got error: {:?}", err)))?;
    let mut expected = PROTOBUF_SHAPED_REQUEST.to_vec();
    expected.extend_from_slice(b"|aorta-ok");
    assert_eq!(sample.payload().to_bytes().as_ref(), expected.as_slice());

    drop(router_session);
    Ok(())
}

/// Contract point 1 (the `complete = false` pin): the same reachable Aorta
/// service is INVISIBLE to a query targeting only complete queryables. This
/// is why the profile mandates target=All for foreign clients. If this test
/// fails because Aorta started declaring `complete(true)`, that is a profile
/// change: external clients relying on documented target=All still work, but
/// the profile text and this pin must be revised together.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_get_all_complete_gets_no_reply_from_aorta_service() -> AortaResult<()> {
    let _group_guard = EnvVarGuard::remove("AORTA_GROUP");

    let (router_session, router_endpoint) = open_router_session().await?;
    let service_session = open_aorta_session(&router_endpoint).await?;
    let native_session = open_native_session(&router_endpoint).await?;
    let service_name = unique_name("native_interop_complete_pin");

    let service = Service::builder(service_session.clone(), &service_name)
        .handler(move |req| {
            let payload = req.payload().to_bytes().into_owned();
            tokio::spawn(async move {
                let _ = req.reply(payload).await;
            });
        })
        .build();
    let _running = service.start().await?;

    let key = foreign_service_key(&service_name);
    // First prove the service is reachable at all (route established) with
    // the profile-mandated target...
    let reply = native_get_first_reply(
        &native_session,
        &key,
        PROTOBUF_SHAPED_REQUEST,
        QueryTarget::All,
        Duration::from_secs(6),
    )
    .await?;
    assert!(reply.is_some(), "service must be reachable with target=All");

    // ...then show AllComplete finds nothing, because the queryable is
    // declared complete=false.
    let replies = native_session
        .get(&key)
        .target(QueryTarget::AllComplete)
        .consolidation(ConsolidationMode::None)
        .payload(PROTOBUF_SHAPED_REQUEST)
        .timeout(Duration::from_millis(500))
        .await
        .map_err(|err| AortaError::internal(format!("native get error: {:?}", err)))?;
    match replies.recv_async().await {
        // Channel closed with no reply — the queryable was not targeted.
        Err(_) => {}
        // The only acceptable reply is zenoh's own synthesized timeout; a
        // real sample or provider error would mean a complete queryable
        // answered.
        Ok(reply) => assert!(
            reply_is_zenoh_query_timeout(&reply),
            "complete=false queryable must not answer an AllComplete query"
        ),
    }

    drop(router_session);
    Ok(())
}

/// Contract point 4, direction A: `ServiceRequest::reply_error` surfaces to a
/// foreign client as a native zenoh error reply with the UTF-8 message as
/// payload.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_get_receives_aorta_reply_error_as_native_err() -> AortaResult<()> {
    let _group_guard = EnvVarGuard::remove("AORTA_GROUP");

    let (router_session, router_endpoint) = open_router_session().await?;
    let service_session = open_aorta_session(&router_endpoint).await?;
    let native_session = open_native_session(&router_endpoint).await?;
    let service_name = unique_name("native_interop_err");

    let service = Service::builder(service_session.clone(), &service_name)
        .handler(move |req| {
            tokio::spawn(async move {
                let _ = req.reply_error("interop boom").await;
            });
        })
        .build();
    let _running = service.start().await?;

    let key = foreign_service_key(&service_name);
    let reply = native_get_first_reply(
        &native_session,
        &key,
        PROTOBUF_SHAPED_REQUEST,
        QueryTarget::All,
        Duration::from_secs(6),
    )
    .await?
    .ok_or_else(|| AortaError::timeout("no reply from Aorta service within 6s".to_string()))?;

    let err = match reply.result() {
        Ok(sample) => {
            return Err(AortaError::internal(format!(
                "expected error reply, got OK payload of {} bytes",
                sample.payload().len()
            )));
        }
        Err(err) => err,
    };
    assert_eq!(err.payload().to_bytes().as_ref(), b"interop boom");

    drop(router_session);
    Ok(())
}

/// Direction B + contract points 2/5/6: an Aorta raw client calls a queryable
/// declared by a bare zenoh peer (default `complete=false`, no attachment on
/// the reply, non-FlatBuffers payload); the bytes arrive verbatim and
/// `wait_for_matching` observes the foreign provider.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aorta_client_calls_native_queryable_bytes_verbatim() -> AortaResult<()> {
    let _group_guard = EnvVarGuard::remove("AORTA_GROUP");

    let (router_session, router_endpoint) = open_router_session().await?;
    let client_session = open_aorta_session(&router_endpoint).await?;
    let native_session = open_native_session(&router_endpoint).await?;
    let service_name = unique_name("native_interop_provider");

    let key = foreign_service_key(&service_name);
    let queryable = native_session
        .declare_queryable(key.as_str())
        .await
        .map_err(|err| AortaError::internal(format!("native queryable error: {:?}", err)))?;
    tokio::spawn(async move {
        while let Ok(query) = queryable.recv_async().await {
            let mut response = query
                .payload()
                .map(|p| p.to_bytes().into_owned())
                .unwrap_or_default();
            response.extend_from_slice(b"|from-native");
            let key_expr = query.key_expr().clone();
            let _ = query.reply(key_expr, response).await;
        }
    });

    let client = Client::builder(client_session, &service_name)
        .timeout_ms(5_000)
        .build();
    // Matching must see the FOREIGN provider (it is transport-level, not an
    // Aorta handshake) — this is the recommended route-race guard for the
    // phone-provider direction.
    assert!(
        client.wait_for_matching(Duration::from_secs(6)).await?,
        "Aorta client must observe the native queryable via matching status"
    );

    let response = client.call(PROTOBUF_SHAPED_REQUEST).await?;
    let mut expected = PROTOBUF_SHAPED_REQUEST.to_vec();
    expected.extend_from_slice(b"|from-native");
    assert_eq!(response.to_bytes().as_ref(), expected.as_slice());

    drop(router_session);
    Ok(())
}

/// Contract point 4, direction B: a native `reply_err` from the foreign
/// provider surfaces as a call error on the Aorta client (never as an OK
/// payload).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aorta_client_surfaces_native_reply_err_as_error() -> AortaResult<()> {
    let _group_guard = EnvVarGuard::remove("AORTA_GROUP");

    let (router_session, router_endpoint) = open_router_session().await?;
    let client_session = open_aorta_session(&router_endpoint).await?;
    let native_session = open_native_session(&router_endpoint).await?;
    let service_name = unique_name("native_interop_provider_err");

    let key = foreign_service_key(&service_name);
    let queryable = native_session
        .declare_queryable(key.as_str())
        .await
        .map_err(|err| AortaError::internal(format!("native queryable error: {:?}", err)))?;
    tokio::spawn(async move {
        while let Ok(query) = queryable.recv_async().await {
            let _ = query.reply_err("native boom").await;
        }
    });

    let client = Client::builder(client_session, &service_name)
        .timeout_ms(5_000)
        .build();
    assert!(
        client.wait_for_matching(Duration::from_secs(6)).await?,
        "Aorta client must observe the native queryable via matching status"
    );

    let err = match client.call(PROTOBUF_SHAPED_REQUEST).await {
        Ok(response) => {
            return Err(AortaError::internal(format!(
                "expected call error, got OK payload of {} bytes",
                response.len()
            )));
        }
        Err(err) => err,
    };
    let detail = format!("{:?}", err);
    assert!(
        detail.contains("native boom"),
        "error must carry the native reply_err message, got: {detail}"
    );

    drop(router_session);
    Ok(())
}
