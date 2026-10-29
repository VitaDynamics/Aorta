use aorta_sdk::log_prelude::*;

use aorta_sdk::{unix_time_ns, with_contiguous_bytes, Node, SchemaMeta, ServiceOptions};
use navigation_msg::aorta;
use navigation_msg::navigation_generated::aorta::services::navigation as nav_fb;
use navigation_msg::schema::{
    CancelPlanRequestSchema, CancelPlanResponseSchema, GetCurrentPoseRequestSchema,
    GetCurrentPoseResponseSchema, GetPlanRequestSchema, GetPlanResponseSchema,
};
// #242 absolute-path rewrite: foreign types (aorta.services.common.*) resolve in
// their OWN dep crate module (status_generated), not a skeleton under
// navigation_generated (which the redesign no longer emits).
use navigation_msg::status_generated::aorta::services::common as common_fb;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

fn build_header<'a>(
    builder: &mut flatbuffers::FlatBufferBuilder<'a>,
    node_name: &str,
    sequence: u64,
) -> flatbuffers::WIPOffset<aorta::sys::AortaHeader<'a>> {
    let node = builder.create_string(node_name);
    aorta::sys::AortaHeader::create(
        builder,
        &aorta::sys::AortaHeaderArgs {
            publish_stamp_ns: unix_time_ns(),
            publisher_node: Some(node),
            sequence,
        },
    )
}

fn build_get_plan_response(
    node_name: &str,
    sequence: u64,
    request: &nav_fb::GetPlanRequest<'_>,
) -> Vec<u8> {
    let dx = request.goal_x() - request.start_x();
    let dy = request.goal_y() - request.start_y();
    let dz = request.goal_z() - request.start_z();
    let path_length = (dx * dx + dy * dy + dz * dz).sqrt();

    let waypoints = [
        request.start_x(),
        request.start_y(),
        request.start_z(),
        (request.start_x() + request.goal_x()) * 0.5,
        (request.start_y() + request.goal_y()) * 0.5,
        (request.start_z() + request.goal_z()) * 0.5,
        request.goal_x(),
        request.goal_y(),
        request.goal_z(),
    ];

    let mut builder = flatbuffers::FlatBufferBuilder::new();
    let header = build_header(&mut builder, node_name, sequence);
    let waypoints = builder.create_vector(&waypoints);
    let response = nav_fb::GetPlanResponse::create(
        &mut builder,
        &nav_fb::GetPlanResponseArgs {
            aorta_header: Some(header),
            status: common_fb::ServiceStatus::SUCCESS,
            waypoints: Some(waypoints),
            waypoint_count: 3,
            path_length,
            computation_time_ms: 2,
            error_message: None,
        },
    );
    builder.finish(response, None);
    builder.finished_data().to_vec()
}

fn build_cancel_plan_response(
    node_name: &str,
    sequence: u64,
    request: &nav_fb::CancelPlanRequest<'_>,
) -> Vec<u8> {
    let plan_id = request.plan_id().unwrap_or_default();
    let cancelled = request.force() || !plan_id.is_empty();
    let (status, error_message) = if cancelled {
        (common_fb::ServiceStatus::SUCCESS, None)
    } else {
        (
            common_fb::ServiceStatus::INVALID_REQUEST,
            Some("plan_id is empty and force=false"),
        )
    };

    let mut builder = flatbuffers::FlatBufferBuilder::new();
    let header = build_header(&mut builder, node_name, sequence);
    let error_message = error_message.map(|value| builder.create_string(value));
    let response = nav_fb::CancelPlanResponse::create(
        &mut builder,
        &nav_fb::CancelPlanResponseArgs {
            aorta_header: Some(header),
            status,
            cancelled,
            error_message,
        },
    );
    builder.finish(response, None);
    builder.finished_data().to_vec()
}

fn build_current_pose_response(node_name: &str, sequence: u64, frame_id: &str) -> Vec<u8> {
    let mut builder = flatbuffers::FlatBufferBuilder::new();
    let header = build_header(&mut builder, node_name, sequence);
    let frame = builder.create_string(frame_id);
    let response = nav_fb::GetCurrentPoseResponse::create(
        &mut builder,
        &nav_fb::GetCurrentPoseResponseArgs {
            aorta_header: Some(header),
            status: common_fb::ServiceStatus::SUCCESS,
            position_x: 1.2,
            position_y: -0.3,
            position_z: 0.0,
            orientation_x: 0.0,
            orientation_y: 0.0,
            orientation_z: 0.0,
            orientation_w: 1.0,
            timestamp_ns: unix_time_ns(),
            frame_id: Some(frame),
        },
    );
    builder.finish(response, None);
    builder.finished_data().to_vec()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut node_name = "navigation_service_server".to_string();
    let mut group = "default".to_string();
    let mut get_plan_name = "/get_plan".to_string();
    let mut cancel_plan_name = "/cancel_plan".to_string();
    let mut current_pose_name = "/get_current_pose".to_string();

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--name" => {
                if let Some(value) = args.next() {
                    node_name = value;
                }
            }
            "--group" => {
                if let Some(value) = args.next() {
                    group = value;
                }
            }
            "--get-plan-service" => {
                if let Some(value) = args.next() {
                    get_plan_name = value;
                }
            }
            "--cancel-plan-service" => {
                if let Some(value) = args.next() {
                    cancel_plan_name = value;
                }
            }
            "--current-pose-service" => {
                if let Some(value) = args.next() {
                    current_pose_name = value;
                }
            }
            _ => {}
        }
    }

    let node = Node::new(node_name.clone(), group.clone()).await?;
    let service_options = ServiceOptions::new();

    aorta_info!(
        "Navigation service node={} group={}",
        node.name(),
        node.group()
    );
    aorta_info!(
        "navigation schema hash={}",
        GetPlanRequestSchema::schema_hash().to_hex()
    );

    let get_plan_seq = Arc::new(AtomicU64::new(0));
    let get_plan_node = node.name().to_string();
    let mut get_plan_service = node
        .create_service_typed_with_options::<GetPlanRequestSchema, GetPlanResponseSchema, _>(
            &get_plan_name,
            move |request| {
                let payload_view = request.payload().clone();
                with_contiguous_bytes(&payload_view, |payload| {
                    let req = match nav_fb::root_as_get_plan_request(payload) {
                        Ok(req) => req,
                        Err(err) => {
                            tokio::spawn(async move {
                                let _ = request
                                    .reply_error(&format!(
                                        "invalid GetPlanRequest payload: {}",
                                        err
                                    ))
                                    .await;
                            });
                            return;
                        }
                    };

                    let seq = get_plan_seq.fetch_add(1, Ordering::Relaxed);
                    let response = build_get_plan_response(&get_plan_node, seq, &req);
                    tokio::spawn(async move {
                        let _ = request.reply(response).await;
                    });
                });
            },
            service_options.clone(),
        )
        .await?;

    let cancel_plan_seq = Arc::new(AtomicU64::new(0));
    let cancel_plan_node = node.name().to_string();
    let mut cancel_plan_service = node
        .create_service_typed_with_options::<CancelPlanRequestSchema, CancelPlanResponseSchema, _>(
            &cancel_plan_name,
            move |request| {
                let payload_view = request.payload().clone();
                with_contiguous_bytes(&payload_view, |payload| {
                    let req = match flatbuffers::root::<nav_fb::CancelPlanRequest<'_>>(payload) {
                        Ok(req) => req,
                        Err(err) => {
                            tokio::spawn(async move {
                                let _ = request
                                    .reply_error(&format!(
                                        "invalid CancelPlanRequest payload: {}",
                                        err
                                    ))
                                    .await;
                            });
                            return;
                        }
                    };

                    let seq = cancel_plan_seq.fetch_add(1, Ordering::Relaxed);
                    let response = build_cancel_plan_response(&cancel_plan_node, seq, &req);
                    tokio::spawn(async move {
                        let _ = request.reply(response).await;
                    });
                });
            },
            service_options.clone(),
        )
        .await?;

    let pose_seq = Arc::new(AtomicU64::new(0));
    let pose_node = node.name().to_string();
    let mut current_pose_service = node
        .create_service_typed_with_options::<
            GetCurrentPoseRequestSchema,
            GetCurrentPoseResponseSchema,
            _,
        >(
            &current_pose_name,
            move |request| {
                let payload_view = request.payload().clone();
                with_contiguous_bytes(&payload_view, |payload| {
                    let req = match flatbuffers::root::<nav_fb::GetCurrentPoseRequest<'_>>(payload)
                    {
                        Ok(req) => req,
                        Err(err) => {
                            tokio::spawn(async move {
                                let _ = request
                                    .reply_error(&format!(
                                        "invalid GetCurrentPoseRequest payload: {}",
                                        err
                                    ))
                                    .await;
                            });
                            return;
                        }
                    };

                    let frame = req.frame_id().unwrap_or("map").to_string();
                    let seq = pose_seq.fetch_add(1, Ordering::Relaxed);
                    let response = build_current_pose_response(&pose_node, seq, &frame);
                    tokio::spawn(async move {
                        let _ = request.reply(response).await;
                    });
                });
            },
            service_options,
        )
        .await?;

    aorta_info!("Navigation services ready:");
    aorta_info!("  - {}", get_plan_name);
    aorta_info!("  - {}", cancel_plan_name);
    aorta_info!("  - {}", current_pose_name);
    aorta_info!("Press Ctrl+C to stop.");

    tokio::signal::ctrl_c().await?;

    get_plan_service.stop();
    cancel_plan_service.stop();
    current_pose_service.stop();
    node.close().await?;
    Ok(())
}
