use aorta_sdk::log_prelude::*;

use aorta_sdk::{Node, SchemaMeta, ServiceOptions};
use cli_args::ExampleArgs;
use echo_msg::prelude::*;
use schema_attach::parse_schema_attach_policy;

mod cli_args;
mod schema_attach;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut node_name = "echo_server".to_string();
    let mut group = "default".to_string();
    let mut service_name = "/echo".to_string();
    let mut schema_attach = "never".to_string();

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
            _ => {}
        }
    }

    let schema_attach_policy = parse_schema_attach_policy(&schema_attach).ok_or_else(|| {
        format!(
            "invalid --schema-attach value: {} (expected never|always|inherit)",
            schema_attach
        )
    })?;

    let node = Node::new(node_name, group).await?;
    let server_name = node.name().to_string();
    let service_options = ServiceOptions::new().with_schema_attach_policy(schema_attach_policy);
    aorta_info!(
        "Echo server node={} group={} service={} schema_attach={} typed_rpc=true",
        node.name(),
        node.group(),
        service_name,
        schema_attach
    );
    aorta_info!(
        "request schema hash={} response schema hash={}",
        EchoRequestSchema::schema_hash().to_hex(),
        EchoResponseSchema::schema_hash().to_hex()
    );

    let mut service = node
        .create_service_typed_view_with_options::<EchoRequestSchema, EchoResponseSchema, _>(
            &service_name,
            move |request, reply| {
                let echoed_message = format!(
                    "echo from {}: {}",
                    server_name,
                    request.message().unwrap_or_default()
                );
                tokio::spawn(async move {
                    match reply
                        .reply(|msg| {
                            msg.echoed_message = Some(echoed_message.clone());
                        })
                        .await
                    {
                        Ok(_) => aorta_info!("Replied typed echo request"),
                        Err(err) => aorta_error!("Failed to reply service request: {}", err),
                    }
                });
            },
            service_options,
        )
        .await?;

    aorta_info!("Echo server ready, replying with typed EchoResponse payload.");
    aorta_info!("Press Ctrl+C to exit");

    tokio::signal::ctrl_c().await?;
    service.stop();
    node.close().await?;
    Ok(())
}
