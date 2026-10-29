use std::time::{Duration, Instant};

use aorta_sdk::prelude::*;
use cli_args::ExampleArgs;
use flow_resume_probe_msg::prelude::*;

mod cli_args;

fn goal_profile(source: &str) -> (u32, Duration) {
    match source {
        "quick" => (4, Duration::from_millis(100)),
        "cancel" => (20, Duration::from_millis(200)),
        "slow" => (12, Duration::from_millis(200)),
        _ => (6, Duration::from_millis(150)),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut node_name = "cli_execute_task_server".to_string();
    let mut group = "default".to_string();

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
            _ => {}
        }
    }

    let node = Node::new(node_name, group).await?;
    let server = ActionServer::<FlowResumeProbeAction>::serve(
        &node,
        FlowResumeProbeAction::ACTION_NAME,
        ActionServerOptions::default(),
        |goal, ctx: ActionExecutionContext<FlowResumeProbeAction>| async move {
            let goal_view = goal.decode()?;
            let source = goal_view.source().unwrap_or_default().to_string();
            let (total_nodes, step_duration) = goal_profile(&source);
            let started = Instant::now();

            aorta_info!(
                "execute_task accepted goal_id={} source={}",
                goal.goal_id(),
                source
            );

            for completed_nodes in 1..=total_nodes {
                if ctx.is_cancel_requested() {
                    aorta_info!("execute_task cancel requested goal_id={}", ctx.goal_id());
                    return ctx
                        .cancel(ActionReplyCode::Canceled, "canceled by client")
                        .await;
                }

                tokio::time::sleep(step_duration).await;

                if ctx.is_cancel_requested() {
                    aorta_info!("execute_task cancel requested goal_id={}", ctx.goal_id());
                    return ctx
                        .cancel(ActionReplyCode::Canceled, "canceled by client")
                        .await;
                }

                let progress_pct = ((completed_nodes * 100) / total_nodes) as u8;
                let feedback = FlowResumeProbeFeedbackDataMsg {
                    current_node: Some(format!("step-{}", completed_nodes)),
                    completed_nodes: Some(completed_nodes),
                    total_nodes: Some(total_nodes),
                    progress_pct: Some(progress_pct),
                };
                ctx.publish_feedback_with_message(&feedback, Some("working"))
                    .await?;
            }

            let duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
            let result = FlowResumeProbeResultMsg {
                duration_ms: Some(duration_ms),
            };
            ctx.succeed(&result).await
        },
    )
    .await?;

    aorta_info!(
        "cli_execute_task_server node={} group={} action={}",
        node.name(),
        node.group(),
        FlowResumeProbeAction::ACTION_NAME
    );
    aorta_info!(
        "action schema hash={}",
        FlowResumeProbeGoalSchema::schema_hash().to_hex()
    );
    aorta_info!("Action server is ready.");
    aorta_info!("Press Ctrl+C to exit.");

    tokio::signal::ctrl_c().await?;
    server.stop_and_join(Duration::from_secs(2)).await;
    node.close().await?;
    Ok(())
}
