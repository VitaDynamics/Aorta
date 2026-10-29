//! Single-key snapshot queryable used by both telemetry publishers and
//! action-server status/info snapshots. The queryable serves the latest
//! contents of an `Arc<RwLock<Option<ZBytes>>>` on a single key; when the
//! payload is absent or empty it surfaces `reply_err("snapshot unavailable")`
//! so callers can map the absent state to `Ok(None)`.

use std::sync::Arc;

use tokio::sync::{oneshot, RwLock as TokioRwLock};

use crate::error::AortaResult;
use crate::log_prelude::*;
use crate::session::Session;
use crate::ZBytes;

/// Reply-error payload used for "the snapshot slot is absent or empty".
/// Consumers classify it with [`crate::transport::reply_error_is_absent_snapshot`]
/// and map it to absence rather than a provider fault — see the note there.
pub(crate) const ABSENT_SNAPSHOT_MARKER: &str = "snapshot unavailable";

/// An alternate payload served when a query carries a specific parameter:
/// `(parameter name, expected value, payload)`.
///
/// Used by the action server so `act/info/<name>?schema=bfbs` answers with the
/// action BFBS on the same key that answers a bare query with ActionInfo.
pub(crate) type ParameterOverride = (&'static str, &'static str, ZBytes);

pub async fn spawn_single_key_snapshot_queryable(
    session: Session,
    key: String,
    payload: Arc<TokioRwLock<Option<ZBytes>>>,
    stop_rx: oneshot::Receiver<()>,
) -> AortaResult<tokio::task::JoinHandle<()>> {
    spawn_snapshot_queryable_with_override(session, key, payload, None, stop_rx).await
}

/// [`spawn_single_key_snapshot_queryable`] with one optional parameter-selected
/// alternate payload. Kept as a single implementation so the stop / undeclare /
/// absent-reply contract cannot drift between the two callers.
pub(crate) async fn spawn_snapshot_queryable_with_override(
    session: Session,
    key: String,
    payload: Arc<TokioRwLock<Option<ZBytes>>>,
    parameter_override: Option<ParameterOverride>,
    mut stop_rx: oneshot::Receiver<()>,
) -> AortaResult<tokio::task::JoinHandle<()>> {
    let queryable = session.inner().declare_queryable(key.clone()).await?;

    Ok(tokio::spawn(async move {
        let queryable = queryable;
        loop {
            let query = tokio::select! {
                _ = &mut stop_rx => break,
                query = queryable.recv_async() => match query {
                    Ok(query) => query,
                    Err(_) => break,
                },
            };

            let reply_result = match payload.read().await.clone() {
                Some(bytes) if !bytes.is_empty() => {
                    // The override only applies while a snapshot exists, so an
                    // absent snapshot still reports absence on every selector.
                    let bytes = match parameter_override.as_ref() {
                        Some((name, value, override_bytes))
                            if query.parameters().get(*name) == Some(*value) =>
                        {
                            override_bytes.clone()
                        }
                        _ => bytes,
                    };
                    query.reply(&key, bytes).await
                }
                _ => query.reply_err(ABSENT_SNAPSHOT_MARKER).await,
            };
            if let Err(err) = reply_result {
                aorta_core_warn!("Aorta WARN: snapshot query reply failed: {}", err);
            }
        }

        if let Err(err) = queryable.undeclare().await {
            aorta_core_warn!("Aorta WARN: snapshot queryable undeclare failed: {}", err);
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::Duration;

    use crate::test_support::ENV_LOCK;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn queryable_replies_error_when_snapshot_unavailable() -> crate::error::AortaResult<()> {
        let _guard = ENV_LOCK.lock().expect("lock");
        let session = Session::open_default().await?;
        let key = format!(
            "aorta/default/sys/snapshot/test_queryable/{}",
            crate::time::unix_time_ns()
        );
        let payload = Arc::new(TokioRwLock::new(None));
        let (stop_tx, stop_rx) = oneshot::channel();
        let task =
            spawn_single_key_snapshot_queryable(session.clone(), key.clone(), payload, stop_rx)
                .await?;

        let replies = session.inner().get(key).await?;
        let reply = tokio::time::timeout(Duration::from_secs(3), replies.recv_async())
            .await
            .expect("snapshot query timeout")
            .expect("snapshot reply");
        assert!(reply.into_result().is_err());

        let _ = stop_tx.send(());
        task.await.expect("snapshot queryable task");
        session.close().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn parameter_override_serves_an_alternate_payload_on_the_same_key(
    ) -> crate::error::AortaResult<()> {
        let _guard = ENV_LOCK.lock().expect("lock");
        let session = Session::open_default().await?;
        let key = format!(
            "aorta/default/sys/snapshot/test_queryable_override/{}",
            crate::time::unix_time_ns()
        );
        let payload = Arc::new(TokioRwLock::new(Some(ZBytes::from(b"snapshot".to_vec()))));
        let (stop_tx, stop_rx) = oneshot::channel();
        let task = spawn_snapshot_queryable_with_override(
            session.clone(),
            key.clone(),
            payload.clone(),
            Some(("schema", "bfbs", ZBytes::from(b"alternate".to_vec()))),
            stop_rx,
        )
        .await?;

        for (selector, expected) in [
            (key.clone(), &b"snapshot"[..]),
            (format!("{key}?schema=bfbs"), &b"alternate"[..]),
            (format!("{key}?schema=other"), &b"snapshot"[..]),
        ] {
            let replies = session.inner().get(selector.clone()).await?;
            let reply = tokio::time::timeout(Duration::from_secs(3), replies.recv_async())
                .await
                .expect("override query timeout")
                .expect("override reply");
            let sample = reply.into_result().expect("override reply payload");
            assert_eq!(
                sample.payload().to_bytes().as_ref(),
                expected,
                "unexpected payload for {selector}"
            );
        }

        // An absent snapshot reports absence on the override selector too.
        *payload.write().await = None;
        let replies = session.inner().get(format!("{key}?schema=bfbs")).await?;
        let reply = tokio::time::timeout(Duration::from_secs(3), replies.recv_async())
            .await
            .expect("absent override query timeout")
            .expect("absent override reply");
        assert!(reply.into_result().is_err());

        let _ = stop_tx.send(());
        task.await.expect("snapshot queryable task");
        session.close().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn queryable_replies_error_after_snapshot_cleared() -> crate::error::AortaResult<()> {
        // Documents the cleanup-window contract relied on by ActionClient::query_status:
        // once the snapshot payload is cleared, the queryable emits reply_err for any
        // subsequent query until it is undeclared. The client must treat that reply_err
        // as "no current snapshot" (Ok(None)), not as a hard error.
        let _guard = ENV_LOCK.lock().expect("lock");
        let session = Session::open_default().await?;
        let key = format!(
            "aorta/default/sys/snapshot/test_queryable_cleared/{}",
            crate::time::unix_time_ns()
        );
        let payload = Arc::new(TokioRwLock::new(Some(ZBytes::from(b"snapshot".to_vec()))));
        let (stop_tx, stop_rx) = oneshot::channel();
        let task = spawn_single_key_snapshot_queryable(
            session.clone(),
            key.clone(),
            payload.clone(),
            stop_rx,
        )
        .await?;

        let replies = session.inner().get(&key).await?;
        let reply = tokio::time::timeout(Duration::from_secs(3), replies.recv_async())
            .await
            .expect("first snapshot query timeout")
            .expect("first snapshot reply");
        let sample = reply
            .into_result()
            .expect("first reply must carry snapshot");
        assert_eq!(sample.payload().to_bytes().as_ref(), b"snapshot");

        *payload.write().await = None;

        let replies = session.inner().get(&key).await?;
        let reply = tokio::time::timeout(Duration::from_secs(3), replies.recv_async())
            .await
            .expect("second snapshot query timeout")
            .expect("second snapshot reply");
        assert!(
            reply.into_result().is_err(),
            "after clearing the payload the queryable must surface reply_err"
        );

        let _ = stop_tx.send(());
        task.await.expect("snapshot queryable task");
        session.close().await?;
        Ok(())
    }
}
