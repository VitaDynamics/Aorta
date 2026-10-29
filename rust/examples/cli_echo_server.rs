use aorta_sdk::prelude::*;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use cli_args::ExampleArgs;
use echo_msg::prelude::*;
use schema_attach::parse_schema_attach_policy;

mod cli_args;
mod schema_attach;

fn build_echo_response(node_name: &str, sequence: u64, echoed_message: &str) -> Vec<u8> {
    let mut builder = FlatBufferBuilder::new();
    let publisher_node = builder.create_string(node_name);
    let message = builder.create_string(echoed_message);
    let header = aorta::sys::AortaHeader::create(
        &mut builder,
        &aorta::sys::AortaHeaderArgs {
            publish_stamp_ns: unix_time_ns(),
            publisher_node: Some(publisher_node),
            sequence,
        },
    );
    let response = demo::EchoResponse::create(
        &mut builder,
        &demo::EchoResponseArgs {
            aorta_header: Some(header),
            echoed_message: Some(message),
        },
    );
    builder.finish(response, None);
    builder.finished_data().to_vec()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut node_name = "cli_echo_server".to_string();
    let mut group = "default".to_string();
    let mut service_name = "/echo_schema".to_string();
    let mut schema_attach = "always".to_string();

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
    let sequence = Arc::new(AtomicU64::new(0));
    let service_options = ServiceOptions::new().with_schema_attach_policy(schema_attach_policy);

    aorta_info!(
        "cli_echo_server node={} group={} service={} schema_attach={}",
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
        .create_service_typed_with_options::<EchoRequestSchema, EchoResponseSchema, _>(
            &service_name,
            move |request| {
                let req_size = request.payload().len();
                let payload = request.payload().clone();
                with_contiguous_bytes(&payload, |request_payload| {
                    let req = match demo::root_as_echo_request(request_payload) {
                        Ok(req) => req,
                        Err(err) => {
                            tokio::spawn(async move {
                                let _ = request
                                    .reply_error(&format!("invalid EchoRequest payload: {}", err))
                                    .await;
                            });
                            return;
                        }
                    };
                    let message = req.message().unwrap_or_default();
                    let sequence = sequence.fetch_add(1, Ordering::Relaxed);
                    let echoed_message = format!("echo from {}: {}", server_name, message);
                    let response = build_echo_response(&server_name, sequence, &echoed_message);
                    tokio::spawn(async move {
                        match request.reply(response).await {
                            Ok(_) => aorta_info!("Replied request ({} bytes)", req_size),
                            Err(err) => aorta_error!("Failed to reply request: {}", err),
                        }
                    });
                });
            },
            service_options,
        )
        .await?;

    aorta_info!("Echo service is ready.");
    aorta_info!("Press Ctrl+C to exit.");

    tokio::signal::ctrl_c().await?;
    service.stop();
    node.close().await?;
    Ok(())
}
