use std::time::Duration;

use aorta_sdk::prelude::*;
use cli_args::ExampleArgs;
use flow_resume_probe_msg::prelude::*;

mod cli_args;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut node_name = "cli_execute_task_client".to_string();
    let mut group = "default".to_string();
    let mut source = "quick".to_string();
    let mut preset = String::new();
    let mut goal_id: Option<String> = None;
    let mut wait_timeout_ms = 10_000u64;
    let mut cancel_after_ms: Option<u64> = None;

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
            "--source" => {
                if let Some(value) = args.next_value() {
                    source = value;
                }
            }
            "--preset" => {
                if let Some(value) = args.next_value() {
                    preset = value;
                }
            }
            "--goal-id" => {
                if let Some(value) = args.next_value() {
                    goal_id = Some(value);
                }
            }
            "--wait-timeout-ms" => {
                if let Some(value) = args.next_value() {
                    if let Ok(parsed) = value.parse::<u64>() {
                        wait_timeout_ms = parsed.max(1);
                    }
                }
            }
            "--cancel-after-ms" => {
                if let Some(value) = args.next_value() {
                    if let Ok(parsed) = value.parse::<u64>() {
                        cancel_after_ms = Some(parsed);
                    }
                }
            }
            _ => {}
        }
    }

    if cancel_after_ms.is_none() && source == "cancel" {
        cancel_after_ms = Some(600);
    }
    if preset.is_empty() {
        preset = format!("demo/{}", source);
    }

    let node = Node::new(node_name, group).await?;
    let client = ActionClient::<FlowResumeProbeAction>::new(
        &node,
        FlowResumeProbeAction::ACTION_NAME,
        ActionClientOptions::default(),
    );
    let info = client.info().await?.ok_or_else(|| {
        AortaError::not_found(format!(
            "No active action server found for '{}'",
            FlowResumeProbeAction::ACTION_NAME
        ))
    })?;

    aorta_info!(
        "cli_execute_task_client node={} group={} action={}",
        node.name(),
        node.group(),
        client.action_name()
    );
    aorta_info!(
        "server_node={} server_epoch={} schema_hash={}",
        info.server_node_name,
        info.server_epoch,
        info.schema_hash.to_hex()
    );

    let goal = FlowResumeProbeGoalMsg {
        source: Some(source.clone()),
        input: Some(ProbeInputMsg::ProbePreset(Box::new(ProbePresetMsg {
            path: Some(preset.clone()),
            args: Some(Vec::new()),
        }))),
        command: None,
        wait: None,
    };

    let handle = if let Some(goal_id) = goal_id.as_deref() {
        client.send_goal_with_id(goal_id, &goal).await?
    } else {
        client.send_goal(&goal).await?
    };

    aorta_info!(
        "goal accepted goal_id={} goal_secret={} server_epoch={}",
        handle.goal_id(),
        handle.goal_secret(),
        handle.server_epoch()
    );

    let mut feedback_stream = handle.feedback_stream().await?;
    let feedback_task = tokio::spawn(async move {
        while let Some(sample_result) = feedback_stream.recv().await {
            match sample_result {
                Ok(sample) => match sample.decode() {
                    Ok(feedback) => {
                        let data = feedback.data();
                        aorta_info!(
                            "feedback goal_id={} seq={} status={:?} current_node={} completed={}/{} progress_pct={} message={}",
                            feedback.goal_id().unwrap_or_default(),
                            feedback.seq(),
                            feedback.status(),
                            data.and_then(|value| value.current_node()).unwrap_or_default(),
                            data.map(|value| value.completed_nodes()).unwrap_or(0),
                            data.map(|value| value.total_nodes()).unwrap_or(0),
                            data.map(|value| value.progress_pct()).unwrap_or(0),
                            feedback.message().unwrap_or_default(),
                        );
                    }
                    Err(err) => {
                        aorta_warn!("decode action feedback failed: {}", err);
                        break;
                    }
                },
                Err(err) => {
                    aorta_warn!("receive action feedback failed: {}", err);
                    break;
                }
            }
        }
    });

    if let Some(cancel_after_ms) = cancel_after_ms {
        aorta_info!("will request cancel after {} ms", cancel_after_ms);
        tokio::time::sleep(Duration::from_millis(cancel_after_ms)).await;
        let cancel = handle.cancel().await?;
        aorta_info!(
            "cancel response accepted={} goal_id={} status={:?} code={:?} message={}",
            cancel.accepted,
            cancel.goal_id,
            cancel.status,
            cancel.code,
            cancel.message,
        );
    }

    let result = handle
        .wait_result(Some(Duration::from_millis(wait_timeout_ms)))
        .await?;
    feedback_task.abort();

    aorta_info!(
        "result goal_id={} status={:?} code={:?} feedback_seq={} message={} server_epoch={}",
        result.goal_id,
        result.status,
        result.code,
        result.last_feedback_seq,
        result.message,
        result.server_epoch
    );
    aorta_info!(
        "feedback_snapshot current_node={} completed={}/{} progress_pct={}",
        result
            .feedback_snapshot
            .current_node
            .as_deref()
            .unwrap_or_default(),
        result.feedback_snapshot.completed_nodes.unwrap_or(0),
        result.feedback_snapshot.total_nodes.unwrap_or(0),
        result.feedback_snapshot.progress_pct.unwrap_or(0)
    );
    if let Some(result_payload) = result.result.as_ref() {
        aorta_info!(
            "result.duration_ms={}",
            result_payload.duration_ms.unwrap_or(0)
        );
    }

    node.close().await?;
    Ok(())
}
