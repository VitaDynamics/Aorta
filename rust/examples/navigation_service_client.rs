use aorta_sdk::log_prelude::*;

use aorta_sdk::{unix_time_ns, with_contiguous_bytes, ClientOptions, Node, SchemaHash};
use navigation_msg::navigation_generated::aorta::services::navigation as nav_fb;
use navigation_msg::{aorta, NAVIGATION_BFBS};

fn build_get_plan_request(
    node_name: &str,
    sequence: u64,
    start_x: f32,
    start_y: f32,
    start_z: f32,
    goal_x: f32,
    goal_y: f32,
    goal_z: f32,
    algorithm: &str,
    timeout_ms: u32,
    goal_tolerance: f32,
) -> Vec<u8> {
    let mut builder = flatbuffers::FlatBufferBuilder::new();
    let publisher_node = builder.create_string(node_name);
    let algorithm = builder.create_string(algorithm);
    let header = aorta::sys::AortaHeader::create(
        &mut builder,
        &aorta::sys::AortaHeaderArgs {
            publish_stamp_ns: unix_time_ns(),
            publisher_node: Some(publisher_node),
            sequence,
        },
    );
    let request = nav_fb::GetPlanRequest::create(
        &mut builder,
        &nav_fb::GetPlanRequestArgs {
            aorta_header: Some(header),
            start_x,
            start_y,
            start_z,
            goal_x,
            goal_y,
            goal_z,
            algorithm: Some(algorithm),
            timeout_ms,
            goal_tolerance,
        },
    );
    builder.finish(request, None);
    builder.finished_data().to_vec()
}

fn parse_arg_f32(value: &str, field: &str) -> Result<f32, Box<dyn std::error::Error>> {
    value.parse::<f32>().map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("invalid {} '{}': {}", field, value, e),
        )
        .into()
    })
}

fn parse_arg_u32(value: &str, field: &str) -> Result<u32, Box<dyn std::error::Error>> {
    value.parse::<u32>().map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("invalid {} '{}': {}", field, value, e),
        )
        .into()
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut node_name = "navigation_service_client".to_string();
    let mut group = "default".to_string();
    let mut service_name = "/get_plan".to_string();

    let mut start_x = 0.0f32;
    let mut start_y = 0.0f32;
    let mut start_z = 0.0f32;
    let mut goal_x = 5.0f32;
    let mut goal_y = 2.0f32;
    let mut goal_z = 0.0f32;
    let mut algorithm = "a_star".to_string();
    let mut request_timeout_ms = 5000u32;
    let mut goal_tolerance = 0.1f32;
    let mut rpc_timeout_ms = 5000u64;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--name" => {
                if let Some(v) = args.next() {
                    node_name = v;
                }
            }
            "--group" => {
                if let Some(v) = args.next() {
                    group = v;
                }
            }
            "--service" => {
                if let Some(v) = args.next() {
                    service_name = v;
                }
            }
            "--start-x" => {
                if let Some(v) = args.next() {
                    start_x = parse_arg_f32(&v, "--start-x")?;
                }
            }
            "--start-y" => {
                if let Some(v) = args.next() {
                    start_y = parse_arg_f32(&v, "--start-y")?;
                }
            }
            "--start-z" => {
                if let Some(v) = args.next() {
                    start_z = parse_arg_f32(&v, "--start-z")?;
                }
            }
            "--goal-x" => {
                if let Some(v) = args.next() {
                    goal_x = parse_arg_f32(&v, "--goal-x")?;
                }
            }
            "--goal-y" => {
                if let Some(v) = args.next() {
                    goal_y = parse_arg_f32(&v, "--goal-y")?;
                }
            }
            "--goal-z" => {
                if let Some(v) = args.next() {
                    goal_z = parse_arg_f32(&v, "--goal-z")?;
                }
            }
            "--algorithm" => {
                if let Some(v) = args.next() {
                    algorithm = v;
                }
            }
            "--request-timeout-ms" => {
                if let Some(v) = args.next() {
                    request_timeout_ms = parse_arg_u32(&v, "--request-timeout-ms")?;
                }
            }
            "--goal-tolerance" => {
                if let Some(v) = args.next() {
                    goal_tolerance = parse_arg_f32(&v, "--goal-tolerance")?;
                }
            }
            "--rpc-timeout-ms" => {
                if let Some(v) = args.next() {
                    rpc_timeout_ms = v.parse::<u64>().map_err(|e| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            format!("invalid --rpc-timeout-ms '{}': {}", v, e),
                        )
                    })?;
                }
            }
            _ => {}
        }
    }

    let node = Node::new(node_name, group).await?;
    let service_schema = SchemaHash::from_bfbs_bytes(NAVIGATION_BFBS);
    let client = node.create_client_with_options(
        &service_name,
        ClientOptions::new()
            .with_timeout_ms(rpc_timeout_ms.max(1))
            .with_service_schema(service_schema)
            .with_service_root_types(
                "aorta.services.navigation.GetPlanRequest",
                "aorta.services.navigation.GetPlanResponse",
            )
            .with_validate_schema(true),
    );

    aorta_info!(
        "Navigation client node={} group={} service={}",
        node.name(),
        node.group(),
        service_name
    );

    let request = build_get_plan_request(
        node.name(),
        0,
        start_x,
        start_y,
        start_z,
        goal_x,
        goal_y,
        goal_z,
        &algorithm,
        request_timeout_ms,
        goal_tolerance,
    );

    match client.call(request).await {
        Ok(response) => {
            with_contiguous_bytes(&response, |response_bytes| {
                let msg = flatbuffers::root::<nav_fb::GetPlanResponse<'_>>(response_bytes)
                    .map_err(|e| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("invalid GetPlanResponse payload: {}", e),
                        )
                    })?;
                let status = msg.status().variant_name().unwrap_or("UNKNOWN");
                aorta_info!(
                    "GetPlan reply status={} waypoint_count={} path_length={:.3} computation_time_ms={}",
                    status,
                    msg.waypoint_count(),
                    msg.path_length(),
                    msg.computation_time_ms()
                );
                let error_message: &str = msg.error_message().unwrap_or("");
                if !error_message.is_empty() {
                    aorta_warn!("GetPlan reply error_message={}", error_message);
                }
                Ok::<(), Box<dyn std::error::Error>>(())
            })?;
        }
        Err(err) => {
            node.close().await?;
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("GetPlan call failed: {}", err),
            )
            .into());
        }
    }

    node.close().await?;
    Ok(())
}
