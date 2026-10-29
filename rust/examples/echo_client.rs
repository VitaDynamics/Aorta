use aorta_sdk::log_prelude::*;

use std::time::Duration;

use aorta_sdk::{ClientOptions, Node, SchemaMeta};
use cli_args::ExampleArgs;
use echo_msg::prelude::*;
use schema_attach::parse_schema_attach_policy;

mod cli_args;
mod schema_attach;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut node_name = "echo_client".to_string();
    let mut group = "default".to_string();
    let mut service_name = "/echo".to_string();
    let mut schema_attach = "never".to_string();
    let mut enforce_single_provider = false;
    let mut multi_reply_window_ms: Option<u64> = None;
    let mut interval_ms = 1000u64;

    let mut args = ExampleArgs::new();
    while let Some(arg) = args.next_flag() {
        match arg.as_str() {
            "--name" => {
                if let Some(value) = args.next_value() {
                    node_name = value;
                }
            }
            "--group" => {
                if let Some(value) = args.next_value() {
                    group = value;
                }
            }
            "--service" => {
                if let Some(value) = args.next_value() {
                    service_name = value;
                }
            }
            "--schema-attach" => {
                if let Some(value) = args.next_value() {
                    schema_attach = value;
                }
            }
            "--enforce-single-provider" => {
                enforce_single_provider = true;
            }
            "--multi-reply-window-ms" => {
                if let Some(value) = args.next_value() {
                    if let Ok(parsed) = value.parse::<u64>() {
                        multi_reply_window_ms = Some(parsed);
                    }
                }
            }
            "--interval-ms" => {
                if let Some(value) = args.next_value() {
                    if let Ok(parsed) = value.parse::<u64>() {
                        interval_ms = parsed.max(1);
                    }
                }
            }
            _ => {}
        }
    }

    let schema_attach_policy = parse_schema_attach_policy(&schema_attach).ok_or_else(|| {
        format!(
            "invalid --schema-attach value: {} (expected never|always|inherit)",
            schema_attach
        )
    })?;

    let mut client_options = ClientOptions::new()
        .with_timeout_ms(2000)
        .with_schema_attach_policy(schema_attach_policy);
    if enforce_single_provider {
        client_options = client_options.with_enforce_single_provider(true);
    }
    if let Some(window_ms) = multi_reply_window_ms {
        client_options = client_options.with_multi_reply_check_window_ms(window_ms);
    }

    let node = Node::new(node_name, group).await?;
    aorta_info!(
        "Echo client node={} group={} service={} schema_attach={} enforce_single_provider={} multi_reply_window_ms={:?} typed_rpc=true",
        node.name(),
        node.group(),
        service_name,
        schema_attach,
        enforce_single_provider,
        multi_reply_window_ms
    );
    aorta_info!(
        "request schema hash={} response schema hash={}",
        EchoRequestSchema::schema_hash().to_hex(),
        EchoResponseSchema::schema_hash().to_hex()
    );

    let client = node.create_client_typed_with_options::<EchoRequestSchema, EchoResponseSchema>(
        &service_name,
        client_options,
    )?;

    aorta_info!(
        "Echo client ready, sending one request every {} ms.",
        interval_ms
    );
    aorta_info!("Press Ctrl+C to exit");

    let mut counter: u64 = 0;
    let mut ticker = tokio::time::interval(Duration::from_millis(interval_ms));
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                break;
            }
            _ = ticker.tick() => {
                let payload = format!("echo request #{} from {}", counter, node.name());
                match client.call(|msg| {
                    msg.message = Some(payload.clone());
                }).await {
                    Ok(response) => {
                        let decoded = response.decode()?;
                        let response_text = decoded.echoed_message().unwrap_or_default();
                        aorta_info!(
                            "[echo ok] recv={}B text=\"{}\"",
                            response.payload().len(),
                            response_text
                        );
                    }
                    Err(err) => {
                        aorta_error!("Echo call failed: {}", err);
                    }
                }
                counter = counter.wrapping_add(1);
            }
        }
    }

    node.close().await?;
    Ok(())
}
