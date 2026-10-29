use crate::client::{Client, DEFAULT_CALL_TIMEOUT_MS};
use crate::error::AortaResult;
use crate::schema::SchemaHash;
use crate::session::Session;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_matching_status_false_without_provider() -> AortaResult<()> {
    // Readiness gate via the cached Querier. With no service provider declared,
    // matching is false and a bounded wait times out to Ok(false) (resolved via
    // `.await`, never `.wait()`).
    let session = Session::open_default().await?;
    let client = Client::builder(session, "rpc_match_no_provider").build();
    assert!(!client.matching_status().await?);
    assert!(!client.wait_for_matching(Duration::from_millis(50)).await?);
    Ok(())
}

#[test]
fn client_default_timeout_constant() {
    assert_eq!(DEFAULT_CALL_TIMEOUT_MS, 5000);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_builder_creates_client() -> AortaResult<()> {
    let session = Session::open_default().await?;
    let client = Client::builder(session, "test_service").build();

    assert_eq!(client.service_name(), "test_service");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_builder_with_custom_timeout() -> AortaResult<()> {
    let session = Session::open_default().await?;
    let custom_timeout = Duration::from_millis(10000);

    let client = Client::builder(session, "timeout_service")
        .timeout(custom_timeout)
        .build();

    assert_eq!(client.service_name(), "timeout_service");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_builder_with_timeout_ms() -> AortaResult<()> {
    let session = Session::open_default().await?;
    let timeout_ms = 3000;

    let client = Client::builder(session, "ms_timeout_service")
        .timeout_ms(timeout_ms)
        .build();

    assert_eq!(client.service_name(), "ms_timeout_service");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_builder_with_service_schema() -> AortaResult<()> {
    let session = Session::open_default().await?;
    let service_hash = SchemaHash::from_bfbs_bytes(&[1, 2, 3]);

    let client = Client::builder(session, "service_schema_service")
        .service_schema(service_hash)
        .build();

    assert_eq!(client.service_name(), "service_schema_service");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_builder_with_service_schema_chaining() -> AortaResult<()> {
    let session = Session::open_default().await?;
    let service_hash = SchemaHash::from_bfbs_bytes(&[4, 5, 6]);

    let client = Client::builder(session, "service_schema_chain_service")
        .service_schema(service_hash)
        .build();

    assert_eq!(client.service_name(), "service_schema_chain_service");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_builder_with_service_schema_and_timeout() -> AortaResult<()> {
    let session = Session::open_default().await?;
    let service_hash = SchemaHash::from_bfbs_bytes(&[1, 2, 3]);

    let client = Client::builder(session, "service_schema_timeout_service")
        .service_schema(service_hash)
        .timeout_ms(2000)
        .build();

    assert_eq!(client.service_name(), "service_schema_timeout_service");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_builder_with_service_root_types() -> AortaResult<()> {
    let session = Session::open_default().await?;

    let client = Client::builder(session, "service_root_types_service")
        .request_root_type("demo.rpc.Request")
        .response_root_type("demo.rpc.Response")
        .build();

    assert_eq!(client.service_name(), "service_root_types_service");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_builder_method_chaining() -> AortaResult<()> {
    let session = Session::open_default().await?;
    let service_hash = SchemaHash::from_bfbs_bytes(&[10, 20, 30]);

    let client = Client::builder(session, "chained_service")
        .timeout(Duration::from_millis(8000))
        .timeout_ms(7000)
        .service_schema(service_hash)
        .build();

    assert_eq!(client.service_name(), "chained_service");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_exposes_service_name() -> AortaResult<()> {
    let session = Session::open_default().await?;
    let service_name = "my_service";

    let client = Client::builder(session, service_name).build();

    assert_eq!(client.service_name(), service_name);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_builder_with_various_timeouts() -> AortaResult<()> {
    let session = Session::open_default().await?;

    let test_cases = vec![100, 1000, 5000, 10000, 30000];

    for timeout_ms in test_cases {
        let client = Client::builder(session.clone(), &format!("service_{}", timeout_ms))
            .timeout_ms(timeout_ms)
            .build();

        assert_eq!(client.service_name(), &format!("service_{}", timeout_ms));
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_builder_multiple_clients_same_session() -> AortaResult<()> {
    let session = Session::open_default().await?;

    let client1 = Client::builder(session.clone(), "service1").build();
    let client2 = Client::builder(session.clone(), "service2").build();
    let client3 = Client::builder(session, "service3").build();

    assert_eq!(client1.service_name(), "service1");
    assert_eq!(client2.service_name(), "service2");
    assert_eq!(client3.service_name(), "service3");

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_builder_default_timeout_value() -> AortaResult<()> {
    let session = Session::open_default().await?;

    let client = Client::builder(session, "default_timeout_service").build();

    assert_eq!(client.service_name(), "default_timeout_service");
    Ok(())
}

// ============================================================================
// Edge Case Tests - Timeout Handling
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_builder_with_zero_timeout() -> AortaResult<()> {
    let session = Session::open_default().await?;
    let client = Client::builder(session, "zero_timeout_service")
        .timeout_ms(0)
        .build();
    assert_eq!(client.service_name(), "zero_timeout_service");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_builder_with_tiny_timeout() -> AortaResult<()> {
    let session = Session::open_default().await?;
    let client = Client::builder(session, "tiny_timeout_service")
        .timeout_ms(1)
        .build();
    assert_eq!(client.service_name(), "tiny_timeout_service");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_builder_with_huge_timeout() -> AortaResult<()> {
    let session = Session::open_default().await?;
    let client = Client::builder(session, "huge_timeout_service")
        .timeout_ms(3600000)
        .build();
    assert_eq!(client.service_name(), "huge_timeout_service");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_builder_timeout_override() -> AortaResult<()> {
    let session = Session::open_default().await?;
    let client = Client::builder(session, "override_timeout_service")
        .timeout_ms(1000)
        .timeout(Duration::from_millis(2000))
        .build();
    assert_eq!(client.service_name(), "override_timeout_service");
    Ok(())
}

// ============================================================================
// Edge Case Tests - Service Name Handling
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_with_empty_service_name() -> AortaResult<()> {
    let session = Session::open_default().await?;
    let client = Client::builder(session, "").build();
    assert_eq!(client.service_name(), "");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_with_very_long_service_name() -> AortaResult<()> {
    let session = Session::open_default().await?;
    let long_name = "b".repeat(1024);
    let client = Client::builder(session, &long_name).build();
    assert_eq!(client.service_name(), long_name);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_with_special_service_name() -> AortaResult<()> {
    let session = Session::open_default().await?;
    let client = Client::builder(session, "my/service:name-test_123").build();
    assert_eq!(client.service_name(), "my/service:name-test_123");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_with_unicode_service_name() -> AortaResult<()> {
    let session = Session::open_default().await?;
    let client = Client::builder(session, "客户端服务").build();
    assert_eq!(client.service_name(), "客户端服务");
    Ok(())
}
