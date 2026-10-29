use std::sync::Arc;
use std::time::Duration;

use zenoh::key_expr::KeyExpr;
use zenoh::liveliness::LivelinessToken;
use zenoh::Session;

use crate::error::AortaResult;
use crate::key_expr::KeyExprBuilder;

#[derive(Debug)]
pub struct LivelinessManager {
    _token: LivelinessToken,
}

impl LivelinessManager {
    pub async fn declare(
        session: &Session,
        key_builder: &KeyExprBuilder,
        node_name: &str,
        node_id: &str,
    ) -> AortaResult<Arc<Self>> {
        let key = key_builder.liveliness_key(node_name, node_id)?;
        Self::declare_key(session, &key).await
    }

    /// Declares a liveliness token on an already-built key. Used both for the
    /// per-node alive token and for per-service owner-claim tokens.
    pub async fn declare_key(session: &Session, key: &KeyExpr<'static>) -> AortaResult<Arc<Self>> {
        let token = session.liveliness().declare_token(key).await.map_err(|e| {
            crate::error::AortaError::internal(format!("failed to declare liveliness token: {}", e))
        })?;

        Ok(Arc::new(Self { _token: token }))
    }
}

/// One-shot query of the liveliness tokens currently alive under `pattern`,
/// returning their key expressions. Used by the network-level single-provider
/// check to discover other providers that already claimed a service.
pub async fn liveliness_get(
    session: &Session,
    pattern: &KeyExpr<'static>,
    timeout: Duration,
) -> AortaResult<Vec<String>> {
    let replies = session
        .liveliness()
        .get(pattern)
        .timeout(timeout)
        .await
        .map_err(|e| {
            crate::error::AortaError::internal(format!(
                "failed to query liveliness '{}': {}",
                pattern.as_str(),
                e
            ))
        })?;

    let deadline = tokio::time::Instant::now() + timeout;
    let mut keys = Vec::new();
    while let Ok(Ok(reply)) = tokio::time::timeout_at(deadline, replies.recv_async()).await {
        if let Ok(sample) = reply.into_result() {
            keys.push(sample.key_expr().as_str().to_string());
        }
    }
    Ok(keys)
}
