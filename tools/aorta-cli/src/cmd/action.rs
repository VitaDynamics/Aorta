use crate::util::format_stamp_age_ns as format_age;
use anyhow::{anyhow, Result};
use aorta_core::action::{
    goal_id_to_key, resolve_action_root_types_from_bfbs, ActionInfo, ActionReplyCode,
    ActionRootType, ActionStatus,
};
use aorta_core::action_server::{
    DEFAULT_ACTION_CONTROL_TIMEOUT_MS, DEFAULT_ACTION_INFO_QUERY_TIMEOUT_MS,
};
use aorta_core::time::unix_time_ns;
use aorta_core::KeyExprBuilder;
use aorta_introspect::reflect;
use serde_json::json;
use serde_json::Value;
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;
use zenoh::query::{ConsolidationMode, QueryTarget};

use crate::util::to_user_key;

#[derive(Debug, Clone, PartialEq, Eq)]
struct QueryReplySample {
    key_expr: String,
    payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PublicStatusView {
    goal_id: String,
    status: ActionStatus,
    code: ActionReplyCode,
    accepted_stamp_ns: u64,
    started_stamp_ns: u64,
    finished_stamp_ns: u64,
    last_feedback_seq: u64,
    server_epoch: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SendGoalResponseView {
    accepted: bool,
    goal_id: String,
    goal_secret: String,
    code: ActionReplyCode,
    status: ActionStatus,
    message: String,
    server_epoch: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CancelGoalResponseView {
    accepted: bool,
    goal_id: String,
    code: ActionReplyCode,
    status: ActionStatus,
    message: String,
}

#[derive(Debug, Clone, PartialEq)]
struct GetResultResponseView {
    goal_id: String,
    status: ActionStatus,
    accepted_stamp_ns: u64,
    started_stamp_ns: u64,
    finished_stamp_ns: u64,
    last_feedback_seq: u64,
    feedback_snapshot: Value,
    result: Value,
    code: ActionReplyCode,
    message: String,
    server_epoch: String,
}

#[derive(Debug, Clone, PartialEq)]
struct FeedbackView {
    goal_id: String,
    seq: u64,
    stamp_ns: u64,
    status: ActionStatus,
    data: Value,
    message: String,
}

pub async fn list(group: Option<&str>) -> Result<()> {
    let key_builder = key_builder_for(group)?;
    let group = key_builder.group().to_string();
    let session = Arc::new(crate::cmd::open_session().await?);
    let selector = action_info_selector(&group)?;
    let samples = collect_get_replies(session.as_ref(), &selector, action_query_timeout()).await?;

    let mut infos = samples
        .into_iter()
        .map(|sample| {
            ActionInfo::decode(&sample.payload).map_err(|err| {
                anyhow!(
                    "failed to decode ActionInfo for '{}': {}",
                    sample.key_expr,
                    err
                )
            })
        })
        .collect::<Result<Vec<_>>>()?;

    if infos.is_empty() {
        println!("No active actions found.");
        return Ok(());
    }

    infos.sort_by(|left, right| {
        left.action_name
            .cmp(&right.action_name)
            .then_with(|| left.server_node_name.cmp(&right.server_node_name))
            .then_with(|| left.server_epoch.cmp(&right.server_epoch))
    });

    eprintln!("Active actions in group '{}':", group);
    for info in infos {
        println!(
            "  {}  node={}  schema={}",
            to_user_key(&info.action_name),
            info.server_node_name,
            info.schema_hash.to_hex(),
        );
    }
    Ok(())
}

pub async fn info(action: &str, group: Option<&str>) -> Result<()> {
    let key_builder = key_builder_for(group)?;
    let group = key_builder.group().to_string();
    let session = Arc::new(crate::cmd::open_session().await?);
    let info = fetch_action_info_or_err(session.as_ref(), &key_builder, action).await?;
    // Root type names come from the compiled bfbs (no
    // longer derived from action_name). One extra action info-key schema
    // round-trip per `aorta action info` invocation; negligible CLI
    // overhead for the precision gained.
    let bfbs = fetch_action_schema_bfbs(session.as_ref(), &key_builder, &info).await?;
    let root_types = resolve_action_root_types_from_bfbs(&bfbs).map_err(|err| {
        anyhow!(
            "failed to resolve action root types for '{}' from bfbs: {}",
            info.action_name,
            err
        )
    })?;

    println!("Action: {}", to_user_key(&info.action_name));
    println!("Group: {}", group);
    println!("Schema hash: {}", info.schema_hash.to_hex());
    println!("Server node: {}", info.server_node_name);
    println!("Server epoch: {}", info.server_epoch);
    println!("Supports cancel: {}", yes_no(info.supports_cancel));
    println!("Max concurrency: {}", info.max_concurrency);
    println!("Result TTL (ms): {}", info.result_ttl_ms);
    println!("Dedup TTL (ms): {}", info.dedup_ttl_ms);
    println!(
        "Public status root type: {}",
        root_types.for_surface(ActionRootType::PublicStatus)
    );
    println!(
        "Get result root type: {}",
        root_types.for_surface(ActionRootType::GetResultResponse)
    );
    Ok(())
}

pub async fn status(action: &str, goal_id: &str, group: Option<&str>) -> Result<()> {
    let key_builder = key_builder_for(group)?;
    let session = Arc::new(crate::cmd::open_session().await?);
    let info = fetch_action_info_or_err(session.as_ref(), &key_builder, action).await?;
    let bfbs = fetch_action_schema_bfbs(session.as_ref(), &key_builder, &info).await?;
    let key: String = key_builder
        .action_status_key(&info.action_name, goal_id)?
        .into();
    let sample = query_single_reply(session.as_ref(), &key, action_query_timeout()).await?;
    let sample = sample.ok_or_else(|| {
        anyhow!(
            "No public status found for action '{}' and goal_id '{}'",
            to_user_key(&info.action_name),
            goal_id
        )
    })?;
    let status = decode_public_status(&info.action_name, &bfbs, &sample.payload)?;

    print_status(&info.action_name, &status);
    Ok(())
}

pub async fn goals(action: &str, all: bool, group: Option<&str>) -> Result<()> {
    let key_builder = key_builder_for(group)?;
    let group = key_builder.group().to_string();
    let session = Arc::new(crate::cmd::open_session().await?);
    let info = fetch_action_info_or_err(session.as_ref(), &key_builder, action).await?;
    let bfbs = fetch_action_schema_bfbs(session.as_ref(), &key_builder, &info).await?;
    let selector = format!("{}/*", key_builder.action_status_prefix(&info.action_name)?);
    let mut rows = collect_get_replies(session.as_ref(), &selector, action_query_timeout())
        .await?
        .into_iter()
        .map(|sample| decode_public_status(&info.action_name, &bfbs, &sample.payload))
        .collect::<Result<Vec<_>>>()?;

    rows.sort_by(|left, right| {
        left.accepted_stamp_ns
            .cmp(&right.accepted_stamp_ns)
            .then_with(|| left.goal_id.cmp(&right.goal_id))
    });

    if !all {
        rows.retain(|row| !row.status.is_terminal());
    }

    if rows.is_empty() {
        if all {
            println!(
                "No retained goals found for action '{}' in group '{}'.",
                to_user_key(&info.action_name),
                group
            );
        } else {
            println!(
                "No active goals found for action '{}' in group '{}'.",
                to_user_key(&info.action_name),
                group
            );
        }
        return Ok(());
    }

    if all {
        println!(
            "Retained goals for action '{}' in group '{}':",
            to_user_key(&info.action_name),
            group
        );
    } else {
        println!(
            "Active goals for action '{}' in group '{}':",
            to_user_key(&info.action_name),
            group
        );
    }

    let now_ns = unix_time_ns();
    for row in rows {
        println!("  goal_id: {}", row.goal_id);
        println!("    status: {}", status_label(row.status));
        println!("    code: {}", reply_code_label(row.code));
        println!("    age: {}", format_age(now_ns, row.accepted_stamp_ns));
        println!("    accepted_stamp_ns: {}", row.accepted_stamp_ns);
        println!("    started_stamp_ns: {}", row.started_stamp_ns);
        println!("    finished_stamp_ns: {}", row.finished_stamp_ns);
        println!("    last_feedback_seq: {}", row.last_feedback_seq);
        println!("    server_epoch: {}", row.server_epoch);
    }
    Ok(())
}

pub async fn feedback(
    action: &str,
    goal_id: Option<&str>,
    count: Option<usize>,
    timeout_secs: Option<u64>,
    group: Option<&str>,
) -> Result<()> {
    let key_builder = key_builder_for(group)?;
    if let Some(goal_id) = goal_id {
        validate_goal_id(goal_id)?;
    }
    let session = Arc::new(crate::cmd::open_session().await?);
    let info = fetch_action_info_or_err(session.as_ref(), &key_builder, action).await?;
    let bfbs = fetch_action_schema_bfbs(session.as_ref(), &key_builder, &info).await?;
    let key: String = key_builder
        .action_feedback_key_sub(&info.action_name)?
        .into();
    let subscriber = session
        .declare_subscriber(key.clone())
        .await
        .map_err(|err| anyhow!("failed to subscribe to '{}': {}", key, err))?;

    eprintln!("Action: {}", to_user_key(&info.action_name));
    if let Some(goal_id) = goal_id {
        eprintln!("Goal filter: {}", goal_id);
    } else {
        eprintln!("Goal filter: (all)");
    }
    eprintln!("Streaming feedback... press Ctrl+C to stop.");

    let deadline = timeout_secs.map(|secs| Instant::now() + Duration::from_secs(secs));
    let mut printed = 0usize;

    loop {
        let recv = subscriber.recv_async();
        let sample = match deadline {
            Some(deadline) => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }
                match tokio::time::timeout(remaining, recv).await {
                    Ok(result) => result.map_err(|_| {
                        anyhow!(
                            "action '{}' feedback subscriber closed unexpectedly",
                            to_user_key(&info.action_name)
                        )
                    })?,
                    // Overall deadline reached: stop cleanly.
                    Err(_) => break,
                }
            }
            None => recv.await.map_err(|_| {
                anyhow!(
                    "action '{}' feedback subscriber closed unexpectedly",
                    to_user_key(&info.action_name)
                )
            })?,
        };
        let fields: zenoh::sample::SampleFields = sample.into();
        if fields.payload.is_empty() {
            return Err(anyhow!(
                "action '{}' feedback sample on '{}' is empty",
                to_user_key(&info.action_name),
                key
            ));
        }
        let feedback =
            decode_feedback(&info.action_name, &bfbs, fields.payload.to_bytes().as_ref())?;
        if goal_id.is_some_and(|expected| expected != feedback.goal_id) {
            continue;
        }
        print_feedback(&info.action_name, &feedback)?;
        printed += 1;
        if let Some(limit) = count {
            if printed >= limit {
                break;
            }
        }
    }

    Ok(())
}

pub async fn send(
    action: &str,
    goal_json_or_file: &str,
    goal_id: Option<&str>,
    non_blocking: bool,
    group: Option<&str>,
) -> Result<()> {
    let key_builder = key_builder_for(group)?;
    let group = key_builder.group().to_string();
    let session = Arc::new(crate::cmd::open_session().await?);
    let info = fetch_action_info_or_err(session.as_ref(), &key_builder, action).await?;
    let bfbs = fetch_action_schema_bfbs(session.as_ref(), &key_builder, &info).await?;
    let goal_id = resolve_goal_id(goal_id)?;
    let goal_value = read_json_argument(goal_json_or_file)?;
    let feedback_key: String = key_builder
        .action_feedback_key_sub(&info.action_name)?
        .into();
    let status_key: String = key_builder
        .action_status_key(&info.action_name, &goal_id)?
        .into();
    let mut feedback_subscriber = if non_blocking {
        None
    } else {
        Some(
            session
                .declare_subscriber(feedback_key.clone())
                .await
                .map_err(|err| anyhow!("failed to subscribe to '{}': {}", feedback_key, err))?,
        )
    };
    let mut status_subscriber = if non_blocking {
        None
    } else {
        Some(
            session
                .declare_subscriber(status_key.clone())
                .await
                .map_err(|err| anyhow!("failed to subscribe to '{}': {}", status_key, err))?,
        )
    };
    let request_value = json!({
        "goal_id": &goal_id,
        "goal": goal_value,
    });
    let key: String = key_builder.action_send_goal_key(&info.action_name)?.into();
    let response = query_action_json(
        session.as_ref(),
        &key,
        &bfbs,
        &info.action_name,
        ActionRootType::SendGoalRequest,
        &request_value,
        ActionRootType::SendGoalResponse,
        action_control_timeout(),
    )
    .await?;
    let response = parse_send_goal_response_json(&response)?;
    if response.goal_id != goal_id {
        return Err(anyhow!(
            "action '{}' send reply goal_id mismatch: expected '{}', got '{}'",
            to_user_key(&info.action_name),
            goal_id,
            response.goal_id
        ));
    }
    if !response.accepted {
        return Err(anyhow!(
            "action '{}' send rejected: code={}, status={}, message={}",
            to_user_key(&info.action_name),
            reply_code_label(response.code),
            status_label(response.status),
            response.message
        ));
    }

    println!("Action: {}", to_user_key(&info.action_name));
    println!("Accepted: yes");
    println!("Goal ID: {}", response.goal_id);
    println!("Goal Secret: {}", response.goal_secret);
    println!("Status: {}", status_label(response.status));
    println!("Reply code: {}", reply_code_label(response.code));
    println!("Server epoch: {}", response.server_epoch);
    if !response.message.is_empty() {
        println!("Message: {}", response.message);
    }
    println!("Next steps:");
    for command in send_follow_up_commands(
        &info.action_name,
        &group,
        &response.goal_id,
        &response.goal_secret,
    ) {
        println!("  {}", command);
    }

    if non_blocking {
        return Ok(());
    }

    if let Some(status) = query_public_status(
        session.as_ref(),
        &key_builder,
        &info.action_name,
        &goal_id,
        &bfbs,
        action_query_timeout(),
    )
    .await?
    {
        if status.status.is_terminal() {
            println!(
                "Observed terminal public status: {} ({})",
                status_label(status.status),
                reply_code_label(status.code)
            );
            let response = fetch_private_result(
                session.as_ref(),
                &key_builder,
                &info,
                &bfbs,
                &goal_id,
                &response.goal_secret,
                action_control_timeout(),
            )
            .await
            .map_err(|err| {
                anyhow!(
                    "goal '{}' reached terminal public status but result retrieval failed: {}",
                    goal_id,
                    err
                )
            })?;
            print_result(&info.action_name, &response)?;
            return Ok(());
        }
    }

    enum WaitEvent<TFeedback, TStatus> {
        Feedback(TFeedback),
        Status(TStatus),
    }

    let feedback_subscriber = feedback_subscriber
        .as_mut()
        .expect("blocking send must declare feedback subscriber");
    let status_subscriber = status_subscriber
        .as_mut()
        .expect("blocking send must declare status subscriber");

    eprintln!(
        "Blocking mode: streaming feedback and waiting for terminal result. Press Ctrl+C to stop."
    );

    loop {
        let event = async {
            tokio::select! {
                sample = feedback_subscriber.recv_async() => WaitEvent::Feedback(sample),
                sample = status_subscriber.recv_async() => WaitEvent::Status(sample),
            }
        }
        .await;

        match event {
            WaitEvent::Feedback(sample) => {
                let sample = sample.map_err(|_| {
                    anyhow!(
                        "action '{}' feedback subscriber closed unexpectedly",
                        to_user_key(&info.action_name)
                    )
                })?;
                let fields: zenoh::sample::SampleFields = sample.into();
                if fields.payload.is_empty() {
                    return Err(anyhow!(
                        "action '{}' feedback sample on '{}' is empty",
                        to_user_key(&info.action_name),
                        feedback_key
                    ));
                }
                let feedback =
                    decode_feedback(&info.action_name, &bfbs, fields.payload.to_bytes().as_ref())?;
                if feedback.goal_id != goal_id {
                    continue;
                }
                print_feedback(&info.action_name, &feedback)?;
            }
            WaitEvent::Status(sample) => {
                let sample = sample.map_err(|_| {
                    anyhow!(
                        "action '{}' wait subscriber closed for goal '{}'",
                        to_user_key(&info.action_name),
                        goal_id
                    )
                })?;
                let fields: zenoh::sample::SampleFields = sample.into();
                if fields.payload.is_empty() {
                    return Err(anyhow!(
                        "action '{}' status for goal '{}' was removed before result retrieval",
                        to_user_key(&info.action_name),
                        goal_id
                    ));
                }
                let status = decode_public_status(
                    &info.action_name,
                    &bfbs,
                    fields.payload.to_bytes().as_ref(),
                )?;
                if !status.status.is_terminal() {
                    continue;
                }

                println!(
                    "Observed terminal public status: {} ({})",
                    status_label(status.status),
                    reply_code_label(status.code)
                );
                let response = fetch_private_result(
                    session.as_ref(),
                    &key_builder,
                    &info,
                    &bfbs,
                    &goal_id,
                    &response.goal_secret,
                    action_control_timeout(),
                )
                .await
                .map_err(|err| {
                    anyhow!(
                        "goal '{}' reached terminal public status but result retrieval failed: {}",
                        goal_id,
                        err
                    )
                })?;
                print_result(&info.action_name, &response)?;
                return Ok(());
            }
        }
    }
}

pub async fn cancel(
    action: &str,
    goal_id: &str,
    goal_secret: &str,
    group: Option<&str>,
) -> Result<()> {
    let key_builder = key_builder_for(group)?;
    validate_goal_id(goal_id)?;
    validate_goal_secret(goal_secret)?;
    let session = Arc::new(crate::cmd::open_session().await?);
    let info = fetch_action_info_or_err(session.as_ref(), &key_builder, action).await?;
    let bfbs = fetch_action_schema_bfbs(session.as_ref(), &key_builder, &info).await?;
    let request_value = json!({
        "goal_id": goal_id,
        "goal_secret": goal_secret,
    });
    let key: String = key_builder
        .action_cancel_goal_key(&info.action_name)?
        .into();
    let response = query_action_json(
        session.as_ref(),
        &key,
        &bfbs,
        &info.action_name,
        ActionRootType::CancelGoalRequest,
        &request_value,
        ActionRootType::CancelGoalResponse,
        action_control_timeout(),
    )
    .await?;
    let response = parse_cancel_goal_response_json(&response)?;
    if response.goal_id != goal_id {
        return Err(anyhow!(
            "action '{}' cancel reply goal_id mismatch: expected '{}', got '{}'",
            to_user_key(&info.action_name),
            goal_id,
            response.goal_id
        ));
    }
    if !response.accepted {
        return Err(anyhow!(
            "action '{}' cancel rejected: code={}, status={}, message={}",
            to_user_key(&info.action_name),
            reply_code_label(response.code),
            status_label(response.status),
            response.message
        ));
    }

    println!("Action: {}", to_user_key(&info.action_name));
    println!("Goal ID: {}", response.goal_id);
    println!("Accepted: yes");
    println!("Status: {}", status_label(response.status));
    println!("Reply code: {}", reply_code_label(response.code));
    if !response.message.is_empty() {
        println!("Message: {}", response.message);
    }
    Ok(())
}

pub async fn result(
    action: &str,
    goal_id: &str,
    goal_secret: &str,
    group: Option<&str>,
) -> Result<()> {
    let key_builder = key_builder_for(group)?;
    validate_goal_id(goal_id)?;
    validate_goal_secret(goal_secret)?;
    let session = Arc::new(crate::cmd::open_session().await?);
    let info = fetch_action_info_or_err(session.as_ref(), &key_builder, action).await?;
    let bfbs = fetch_action_schema_bfbs(session.as_ref(), &key_builder, &info).await?;
    let response = fetch_private_result(
        session.as_ref(),
        &key_builder,
        &info,
        &bfbs,
        goal_id,
        goal_secret,
        action_control_timeout(),
    )
    .await?;
    print_result(&info.action_name, &response)?;
    Ok(())
}

pub async fn wait(
    action: &str,
    goal_id: &str,
    goal_secret: &str,
    timeout: Duration,
    group: Option<&str>,
) -> Result<()> {
    let key_builder = key_builder_for(group)?;
    validate_goal_id(goal_id)?;
    validate_goal_secret(goal_secret)?;
    let session = Arc::new(crate::cmd::open_session().await?);
    let info = fetch_action_info_or_err(session.as_ref(), &key_builder, action).await?;
    let bfbs = fetch_action_schema_bfbs(session.as_ref(), &key_builder, &info).await?;
    let deadline = Instant::now() + timeout;

    if let Some(status) = query_public_status(
        session.as_ref(),
        &key_builder,
        &info.action_name,
        goal_id,
        &bfbs,
        remaining_timeout(deadline, goal_id, action_query_timeout())?,
    )
    .await?
    {
        if status.status.is_terminal() {
            let response = fetch_private_result(
                session.as_ref(),
                &key_builder,
                &info,
                &bfbs,
                goal_id,
                goal_secret,
                remaining_timeout(deadline, goal_id, action_control_timeout())?,
            )
            .await
            .map_err(|err| {
                anyhow!(
                    "goal '{}' already reached terminal state but result retrieval failed: {}",
                    goal_id,
                    err
                )
            })?;
            print_result(&info.action_name, &response)?;
            return Ok(());
        }
    }

    let status_key: String = key_builder
        .action_status_key(&info.action_name, goal_id)?
        .into();
    let subscriber = session
        .declare_subscriber(status_key.clone())
        .await
        .map_err(|err| anyhow!("failed to subscribe to '{}': {}", status_key, err))?;

    let wait_outcome = async {
        if let Some(status) = query_public_status(
            session.as_ref(),
            &key_builder,
            &info.action_name,
            goal_id,
            &bfbs,
            remaining_timeout(deadline, goal_id, action_query_timeout())?,
        )
        .await?
        {
            if status.status.is_terminal() {
                return Ok(status);
            }
        }

        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(anyhow!(
                    "timed out waiting for terminal action status '{}'",
                    goal_id
                ));
            }
            let sample = tokio::time::timeout(remaining, subscriber.recv_async())
                .await
                .map_err(|_| anyhow!("timed out waiting for terminal action status '{}'", goal_id))?
                .map_err(|_| {
                    anyhow!(
                        "action '{}' wait subscriber closed for goal '{}'",
                        to_user_key(&info.action_name),
                        goal_id
                    )
                })?;
            let fields: zenoh::sample::SampleFields = sample.into();
            if fields.payload.is_empty() {
                return Err(anyhow!(
                    "action '{}' status for goal '{}' was removed before result retrieval",
                    to_user_key(&info.action_name),
                    goal_id
                ));
            }
            let status =
                decode_public_status(&info.action_name, &bfbs, fields.payload.to_bytes().as_ref())?;
            if status.status.is_terminal() {
                return Ok(status);
            }
        }
    }
    .await;

    let _ = subscriber.undeclare().await;
    let terminal_status = wait_outcome?;
    println!(
        "Observed terminal public status: {} ({})",
        status_label(terminal_status.status),
        reply_code_label(terminal_status.code)
    );
    let response = fetch_private_result(
        session.as_ref(),
        &key_builder,
        &info,
        &bfbs,
        goal_id,
        goal_secret,
        remaining_timeout(deadline, goal_id, action_control_timeout())?,
    )
    .await
    .map_err(|err| {
        anyhow!(
            "goal '{}' reached terminal public status but result retrieval failed: {}",
            goal_id,
            err
        )
    })?;
    print_result(&info.action_name, &response)?;
    Ok(())
}

fn key_builder_for(group: Option<&str>) -> Result<KeyExprBuilder> {
    let group = group.unwrap_or("default");
    KeyExprBuilder::new(group).map_err(|err| anyhow!("Invalid group '{}': {}", group, err))
}

fn action_query_timeout() -> Duration {
    Duration::from_millis(DEFAULT_ACTION_INFO_QUERY_TIMEOUT_MS)
}

fn action_control_timeout() -> Duration {
    Duration::from_millis(DEFAULT_ACTION_CONTROL_TIMEOUT_MS)
}

fn action_info_selector(group: &str) -> Result<String> {
    Ok(format!(
        "aorta/{}/act/info/**",
        key_builder_for(Some(group))?.group()
    ))
}

async fn fetch_action_info_or_err(
    session: &zenoh::Session,
    key_builder: &KeyExprBuilder,
    action: &str,
) -> Result<ActionInfo> {
    fetch_action_info(session, key_builder, action, action_query_timeout())
        .await?
        .ok_or_else(|| {
            anyhow!(
                "No active action server found for '{}'. If you are testing against \
cpp/examples:cli_execute_task_server, run the CLI with a client Zenoh config that \
connects to tcp/127.0.0.1:17447 (for example via --zenoh-config <file>).",
                action
            )
        })
}

async fn fetch_action_schema_bfbs(
    session: &zenoh::Session,
    key_builder: &KeyExprBuilder,
    info: &ActionInfo,
) -> Result<Vec<u8>> {
    aorta_core::action_server::fetch_action_schema_bfbs(
        session,
        key_builder,
        info,
        Duration::from_secs(2),
    )
    .await
    .map_err(|err| {
        anyhow!(
            "failed to resolve action schema for '{}': {}",
            info.action_name,
            err
        )
    })
}

fn resolve_goal_id(goal_id: Option<&str>) -> Result<String> {
    match goal_id {
        Some(goal_id) => {
            validate_goal_id(goal_id)?;
            Ok(goal_id.to_string())
        }
        None => Ok(Uuid::new_v4().hyphenated().to_string()),
    }
}

fn validate_goal_id(goal_id: &str) -> Result<()> {
    goal_id_to_key(goal_id)
        .map(|_| ())
        .map_err(|err| anyhow!("invalid goal_id '{}': {}", goal_id, err))
}

fn validate_goal_secret(goal_secret: &str) -> Result<()> {
    if goal_secret.trim().is_empty() {
        return Err(anyhow!("goal_secret must not be empty"));
    }
    Ok(())
}

fn read_json_argument(value: &str) -> Result<Value> {
    let text = if let Some(path) = value.strip_prefix('@') {
        if path.trim().is_empty() {
            return Err(anyhow!("json file path after '@' must not be empty"));
        }
        std::fs::read_to_string(path)
            .map_err(|err| anyhow!("failed to read json file '{}': {}", path, err))?
    } else {
        value.to_string()
    };
    serde_json::from_str(&text).map_err(|err| anyhow!("invalid json input '{}': {}", value, err))
}

fn encode_action_json(
    bfbs: &[u8],
    action_name: &str,
    root_type: ActionRootType,
    value: &Value,
) -> Result<Vec<u8>> {
    let root_type_name = action_root_type_name(bfbs, root_type)?;
    let json = serde_json::to_string(value)
        .map_err(|err| anyhow!("failed to serialize action json value: {}", err))?;
    reflect::json_to_fb(bfbs, &json, Some(&root_type_name)).map_err(|err| {
        anyhow!(
            "failed to encode action '{}' payload with root type '{}': {}",
            action_name,
            root_type_name,
            err
        )
    })
}

fn decode_action_json(
    bfbs: &[u8],
    action_name: &str,
    root_type: ActionRootType,
    payload: &[u8],
) -> Result<Value> {
    let root_type_name = action_root_type_name(bfbs, root_type)?;
    let json = reflect::fb_to_json(bfbs, payload, Some(&root_type_name)).map_err(|err| {
        anyhow!(
            "failed to decode action '{}' payload with root type '{}': {}",
            action_name,
            root_type_name,
            err
        )
    })?;
    parse_reflected_json(&json, "action json")
}

fn parse_reflected_json(text: &str, label: &str) -> Result<Value> {
    match serde_json::from_str(text) {
        Ok(value) => Ok(value),
        Err(json_err) => json5::from_str(text).map_err(|json5_err| {
            anyhow!(
                "failed to parse {} '{}': json={}, json5={}",
                label,
                text,
                json_err,
                json5_err
            )
        }),
    }
}

/// Resolve a specific action surface's fully-qualified FlatBuffers
/// root type name from a compiled `.bfbs`. Replaces the earlier
/// path-projection helper (`derive_action_root_type_names`), which
/// constrained action names to the FlatBuffers identifier set.
fn action_root_type_name(bfbs: &[u8], root_type: ActionRootType) -> Result<String> {
    resolve_action_root_types_from_bfbs(bfbs)
        .map(|names| names.for_surface(root_type))
        .map_err(|err| {
            anyhow!(
                "failed to resolve action root type {:?} from bfbs: {}",
                root_type,
                err
            )
        })
}

async fn query_action_json(
    session: &zenoh::Session,
    selector: &str,
    bfbs: &[u8],
    action_name: &str,
    request_root: ActionRootType,
    request_value: &Value,
    response_root: ActionRootType,
    timeout: Duration,
) -> Result<Value> {
    let payload = encode_action_json(bfbs, action_name, request_root, request_value)?;
    let sample = query_single_reply_with_payload(session, selector, payload, timeout)
        .await?
        .ok_or_else(|| anyhow!("query '{}' returned no reply", selector))?;
    decode_action_json(bfbs, action_name, response_root, &sample.payload)
}

async fn fetch_private_result(
    session: &zenoh::Session,
    key_builder: &KeyExprBuilder,
    info: &ActionInfo,
    bfbs: &[u8],
    goal_id: &str,
    goal_secret: &str,
    timeout: Duration,
) -> Result<GetResultResponseView> {
    let request_value = json!({
        "goal_id": goal_id,
        "goal_secret": goal_secret,
    });
    let key: String = key_builder
        .action_get_result_key_query(&info.action_name)?
        .into();
    let response = query_action_json(
        session,
        &key,
        bfbs,
        &info.action_name,
        ActionRootType::GetResultRequest,
        &request_value,
        ActionRootType::GetResultResponse,
        timeout,
    )
    .await?;
    let response = parse_get_result_response_json(&response)?;
    if response.goal_id != goal_id {
        return Err(anyhow!(
            "action '{}' get_result reply goal_id mismatch: expected '{}', got '{}'",
            to_user_key(&info.action_name),
            goal_id,
            response.goal_id
        ));
    }
    match response.code {
        ActionReplyCode::Unauthorized => Err(anyhow!(
            "action '{}' get_result rejected: code={}, status={}, message={}",
            to_user_key(&info.action_name),
            reply_code_label(response.code),
            status_label(response.status),
            response.message
        )),
        ActionReplyCode::NotFound | ActionReplyCode::GoalExpired => Err(anyhow!(
            "action '{}' get_result rejected: code={}, status={}, message={}",
            to_user_key(&info.action_name),
            reply_code_label(response.code),
            status_label(response.status),
            response.message
        )),
        _ => Ok(response),
    }
}

async fn query_public_status(
    session: &zenoh::Session,
    key_builder: &KeyExprBuilder,
    action_name: &str,
    goal_id: &str,
    bfbs: &[u8],
    timeout: Duration,
) -> Result<Option<PublicStatusView>> {
    let key: String = key_builder.action_status_key(action_name, goal_id)?.into();
    let sample = query_single_reply(session, &key, timeout).await?;
    sample
        .map(|sample| decode_public_status(action_name, bfbs, &sample.payload))
        .transpose()
}

fn remaining_timeout(deadline: Instant, goal_id: &str, cap: Duration) -> Result<Duration> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(anyhow!(
            "timed out waiting for terminal action status '{}'",
            goal_id
        ));
    }
    Ok(remaining.min(cap))
}

async fn query_single_reply(
    session: &zenoh::Session,
    selector: &str,
    timeout: Duration,
) -> Result<Option<QueryReplySample>> {
    let samples = collect_get_replies(session, selector, timeout).await?;
    match samples.len() {
        0 => Ok(None),
        1 => Ok(samples.into_iter().next()),
        count => Err(anyhow!(
            "query '{}' returned {} replies; expected at most one",
            selector,
            count
        )),
    }
}

async fn query_single_reply_with_payload(
    session: &zenoh::Session,
    selector: &str,
    payload: Vec<u8>,
    timeout: Duration,
) -> Result<Option<QueryReplySample>> {
    let replies = tokio::time::timeout(
        timeout,
        session
            .get(selector)
            .target(QueryTarget::All)
            .consolidation(ConsolidationMode::None)
            .payload(payload)
            .timeout(timeout),
    )
    .await
    .map_err(|_| anyhow!("query '{}' timed out after {:?}", selector, timeout))?
    .map_err(|err| anyhow!("failed to query '{}': {}", selector, err))?;

    let started = Instant::now();
    let mut out = Vec::new();
    let mut first_error: Option<String> = None;
    loop {
        let elapsed = started.elapsed();
        if elapsed >= timeout {
            break;
        }
        let remaining = timeout.saturating_sub(elapsed);
        match tokio::time::timeout(remaining, replies.recv_async()).await {
            Ok(Ok(reply)) => match reply.result() {
                Ok(sample) => out.push(QueryReplySample {
                    key_expr: sample.key_expr().as_str().to_string(),
                    payload: sample.payload().to_bytes().to_vec(),
                }),
                Err(err) => {
                    if first_error.is_none() {
                        first_error = Some(aorta_core::reply_error_text(err));
                    }
                }
            },
            Ok(Err(_)) => break,
            Err(_) => break,
        }
    }

    if out.is_empty() {
        if let Some(err) = first_error {
            return Err(anyhow!(
                "query '{}' returned only errors: {}",
                selector,
                err
            ));
        }
        return Ok(None);
    }

    if out.len() > 1 {
        return Err(anyhow!(
            "query '{}' returned {} replies; expected at most one",
            selector,
            out.len()
        ));
    }

    Ok(out.into_iter().next())
}

async fn collect_get_replies(
    session: &zenoh::Session,
    selector: &str,
    timeout: Duration,
) -> Result<Vec<QueryReplySample>> {
    let replies = tokio::time::timeout(
        timeout,
        session
            .get(selector)
            .target(QueryTarget::All)
            .consolidation(ConsolidationMode::None)
            .timeout(timeout),
    )
    .await
    .map_err(|_| anyhow!("query '{}' timed out after {:?}", selector, timeout))?
    .map_err(|err| anyhow!("failed to query '{}': {}", selector, err))?;

    let started = Instant::now();
    let mut out = Vec::new();
    let mut first_error: Option<String> = None;
    loop {
        let elapsed = started.elapsed();
        if elapsed >= timeout {
            break;
        }
        let remaining = timeout.saturating_sub(elapsed);
        match tokio::time::timeout(remaining, replies.recv_async()).await {
            Ok(Ok(reply)) => match reply.result() {
                Ok(sample) => out.push(QueryReplySample {
                    key_expr: sample.key_expr().as_str().to_string(),
                    payload: sample.payload().to_bytes().to_vec(),
                }),
                Err(err) => {
                    if first_error.is_none() {
                        first_error = Some(aorta_core::reply_error_text(err));
                    }
                }
            },
            Ok(Err(_)) => break,
            Err(_) => break,
        }
    }

    if out.is_empty() {
        if let Some(err) = first_error {
            return Err(anyhow!(
                "query '{}' returned only errors: {}",
                selector,
                err
            ));
        }
    }

    Ok(out)
}

async fn fetch_action_info(
    session: &zenoh::Session,
    key_builder: &KeyExprBuilder,
    action: &str,
    timeout: Duration,
) -> Result<Option<ActionInfo>> {
    let key: String = key_builder.action_info_key(action)?.into();
    let sample = query_single_reply(session, &key, timeout).await?;
    sample
        .map(|sample| {
            ActionInfo::decode(&sample.payload).map_err(|err| {
                anyhow!(
                    "failed to decode ActionInfo for '{}': {}",
                    sample.key_expr,
                    err
                )
            })
        })
        .transpose()
}

fn decode_public_status(
    action_name: &str,
    bfbs: &[u8],
    payload: &[u8],
) -> Result<PublicStatusView> {
    let root_type = action_root_type_name(bfbs, ActionRootType::PublicStatus)?;
    let json = reflect::fb_to_json(bfbs, payload, Some(&root_type)).map_err(|err| {
        anyhow!(
            "failed to decode public status for '{}' with root type '{}': {}",
            action_name,
            root_type,
            err
        )
    })?;
    let value = parse_reflected_json(&json, "public status json")?;
    parse_public_status_json(&value)
}

fn decode_feedback(action_name: &str, bfbs: &[u8], payload: &[u8]) -> Result<FeedbackView> {
    let root_type = action_root_type_name(bfbs, ActionRootType::Feedback)?;
    let json = reflect::fb_to_json(bfbs, payload, Some(&root_type)).map_err(|err| {
        anyhow!(
            "failed to decode feedback for '{}' with root type '{}': {}",
            action_name,
            root_type,
            err
        )
    })?;
    let value = parse_reflected_json(&json, "feedback json")?;
    parse_feedback_json(&value)
}

fn parse_public_status_json(value: &Value) -> Result<PublicStatusView> {
    Ok(PublicStatusView {
        goal_id: required_string(value, "goal_id")?.to_string(),
        status: parse_status(required_value(value, "status")?)?,
        code: parse_reply_code(required_value(value, "code")?)?,
        accepted_stamp_ns: parse_u64(
            required_value(value, "accepted_stamp_ns")?,
            "accepted_stamp_ns",
        )?,
        started_stamp_ns: optional_u64(value, "started_stamp_ns")?,
        finished_stamp_ns: optional_u64(value, "finished_stamp_ns")?,
        last_feedback_seq: optional_u64(value, "last_feedback_seq")?,
        server_epoch: required_string(value, "server_epoch")?.to_string(),
    })
}

fn parse_feedback_json(value: &Value) -> Result<FeedbackView> {
    Ok(FeedbackView {
        goal_id: required_string(value, "goal_id")?.to_string(),
        seq: parse_u64(required_value(value, "seq")?, "seq")?,
        stamp_ns: parse_u64(required_value(value, "stamp_ns")?, "stamp_ns")?,
        status: parse_status(required_value(value, "status")?)?,
        data: required_value(value, "data")?.clone(),
        message: required_string(value, "message")?.to_string(),
    })
}

fn parse_send_goal_response_json(value: &Value) -> Result<SendGoalResponseView> {
    Ok(SendGoalResponseView {
        accepted: required_bool(value, "accepted")?,
        goal_id: required_string(value, "goal_id")?.to_string(),
        goal_secret: required_string(value, "goal_secret")?.to_string(),
        code: parse_reply_code(required_value(value, "code")?)?,
        status: parse_status(required_value(value, "status")?)?,
        message: required_string(value, "message")?.to_string(),
        server_epoch: required_string(value, "server_epoch")?.to_string(),
    })
}

fn parse_cancel_goal_response_json(value: &Value) -> Result<CancelGoalResponseView> {
    Ok(CancelGoalResponseView {
        accepted: required_bool(value, "accepted")?,
        goal_id: required_string(value, "goal_id")?.to_string(),
        code: parse_reply_code(required_value(value, "code")?)?,
        status: parse_status(required_value(value, "status")?)?,
        message: required_string(value, "message")?.to_string(),
    })
}

fn parse_get_result_response_json(value: &Value) -> Result<GetResultResponseView> {
    Ok(GetResultResponseView {
        goal_id: required_string(value, "goal_id")?.to_string(),
        status: parse_status(required_value(value, "status")?)?,
        accepted_stamp_ns: optional_u64(value, "accepted_stamp_ns")?,
        started_stamp_ns: optional_u64(value, "started_stamp_ns")?,
        finished_stamp_ns: optional_u64(value, "finished_stamp_ns")?,
        last_feedback_seq: optional_u64(value, "last_feedback_seq")?,
        feedback_snapshot: optional_value(value, "feedback_snapshot"),
        result: optional_value(value, "result"),
        code: parse_reply_code(required_value(value, "code")?)?,
        message: required_string(value, "message")?.to_string(),
        server_epoch: required_string(value, "server_epoch")?.to_string(),
    })
}

fn required_value<'a>(value: &'a Value, field: &str) -> Result<&'a Value> {
    value
        .get(field)
        .ok_or_else(|| anyhow!("missing field '{}' in action json", field))
}

fn optional_value(value: &Value, field: &str) -> Value {
    value.get(field).cloned().unwrap_or(Value::Null)
}

fn required_string<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    required_value(value, field)?
        .as_str()
        .ok_or_else(|| anyhow!("field '{}' must be a string", field))
}

fn required_bool(value: &Value, field: &str) -> Result<bool> {
    required_value(value, field)?
        .as_bool()
        .ok_or_else(|| anyhow!("field '{}' must be a bool", field))
}

fn optional_u64(value: &Value, field: &str) -> Result<u64> {
    value
        .get(field)
        .map_or(Ok(0), |value| parse_u64(value, field))
}

fn parse_u64(value: &Value, field: &str) -> Result<u64> {
    if let Some(number) = value.as_u64() {
        return Ok(number);
    }
    if let Some(text) = value.as_str() {
        return text
            .parse::<u64>()
            .map_err(|err| anyhow!("field '{}' invalid u64 '{}': {}", field, text, err));
    }
    Err(anyhow!("field '{}' must be a u64-compatible value", field))
}

fn parse_status(value: &Value) -> Result<ActionStatus> {
    if let Some(text) = value.as_str() {
        return match text.trim().to_ascii_uppercase().as_str() {
            "UNKNOWN" => Ok(ActionStatus::Unknown),
            "ACCEPTED" => Ok(ActionStatus::Accepted),
            "EXECUTING" => Ok(ActionStatus::Executing),
            "CANCELING" => Ok(ActionStatus::Canceling),
            "SUCCEEDED" => Ok(ActionStatus::Succeeded),
            "ABORTED" => Ok(ActionStatus::Aborted),
            "CANCELED" => Ok(ActionStatus::Canceled),
            other => Err(anyhow!("unknown action status '{}'", other)),
        };
    }

    let Some(number) = value.as_i64() else {
        return Err(anyhow!("action status must be string or integer"));
    };
    ActionStatus::try_from(
        i8::try_from(number).map_err(|err| anyhow!("action status out of range: {}", err))?,
    )
    .map_err(|err| anyhow!("invalid action status value {}: {}", number, err))
}

fn parse_reply_code(value: &Value) -> Result<ActionReplyCode> {
    if let Some(text) = value.as_str() {
        return match text.trim().to_ascii_uppercase().as_str() {
            "NONE" => Ok(ActionReplyCode::None),
            "OK" => Ok(ActionReplyCode::Ok),
            "DUPLICATE_ACCEPTED" => Ok(ActionReplyCode::DuplicateAccepted),
            "SUCCEEDED" => Ok(ActionReplyCode::Succeeded),
            "CANCELED" => Ok(ActionReplyCode::Canceled),
            "BUSY" => Ok(ActionReplyCode::Busy),
            "PAYLOAD_CONFLICT" => Ok(ActionReplyCode::PayloadConflict),
            "UNAUTHORIZED" => Ok(ActionReplyCode::Unauthorized),
            "NOT_FOUND" => Ok(ActionReplyCode::NotFound),
            "ALREADY_TERMINAL" => Ok(ActionReplyCode::AlreadyTerminal),
            "GOAL_EXPIRED" => Ok(ActionReplyCode::GoalExpired),
            "HANDLER_ERROR" => Ok(ActionReplyCode::HandlerError),
            "HANDLER_PANIC" => Ok(ActionReplyCode::HandlerPanic),
            "HANDLER_COMPLETED_WITHOUT_RESULT" => {
                Ok(ActionReplyCode::HandlerCompletedWithoutResult)
            }
            "CONTEXT_DROPPED" => Ok(ActionReplyCode::ContextDropped),
            "SERVER_CRASH" => Ok(ActionReplyCode::ServerCrash),
            "CONFLICT" => Ok(ActionReplyCode::Conflict),
            other => Err(anyhow!("unknown action reply code '{}'", other)),
        };
    }

    let Some(number) = value.as_i64() else {
        return Err(anyhow!("action reply code must be string or integer"));
    };
    ActionReplyCode::try_from(
        i16::try_from(number).map_err(|err| anyhow!("action reply code out of range: {}", err))?,
    )
    .map_err(|err| anyhow!("invalid action reply code value {}: {}", number, err))
}

fn print_status(action_name: &str, status: &PublicStatusView) {
    println!("Action: {}", to_user_key(action_name));
    println!("Goal ID: {}", status.goal_id);
    println!("Status: {}", status_label(status.status));
    println!("Reply code: {}", reply_code_label(status.code));
    println!("Accepted stamp (ns): {}", status.accepted_stamp_ns);
    println!("Started stamp (ns): {}", status.started_stamp_ns);
    println!("Finished stamp (ns): {}", status.finished_stamp_ns);
    println!("Last feedback seq: {}", status.last_feedback_seq);
    println!("Server epoch: {}", status.server_epoch);
}

fn print_result(action_name: &str, response: &GetResultResponseView) -> Result<()> {
    println!("Action: {}", to_user_key(action_name));
    println!("Goal ID: {}", response.goal_id);
    println!("Status: {}", status_label(response.status));
    println!("Reply code: {}", reply_code_label(response.code));
    println!("Accepted stamp (ns): {}", response.accepted_stamp_ns);
    println!("Started stamp (ns): {}", response.started_stamp_ns);
    println!("Finished stamp (ns): {}", response.finished_stamp_ns);
    println!("Last feedback seq: {}", response.last_feedback_seq);
    println!("Server epoch: {}", response.server_epoch);
    if !response.message.is_empty() {
        println!("Message: {}", response.message);
    }
    print_json_value("Feedback snapshot", &response.feedback_snapshot)?;
    print_json_value("Result", &response.result)?;
    Ok(())
}

fn print_feedback(action_name: &str, feedback: &FeedbackView) -> Result<()> {
    println!();
    println!("Action: {}", to_user_key(action_name));
    println!("Goal ID: {}", feedback.goal_id);
    println!("Seq: {}", feedback.seq);
    println!("Stamp (ns): {}", feedback.stamp_ns);
    println!("Status: {}", status_label(feedback.status));
    if !feedback.message.is_empty() {
        println!("Message: {}", feedback.message);
    }
    print_json_value("Data", &feedback.data)?;
    Ok(())
}

fn print_json_value(label: &str, value: &Value) -> Result<()> {
    println!("{}:", label);
    let pretty = serde_json::to_string_pretty(value)
        .map_err(|err| anyhow!("failed to render json for '{}': {}", label, err))?;
    for line in pretty.lines() {
        println!("  {}", line);
    }
    Ok(())
}

fn status_label(status: ActionStatus) -> &'static str {
    match status {
        ActionStatus::Unknown => "UNKNOWN",
        ActionStatus::Accepted => "ACCEPTED",
        ActionStatus::Executing => "EXECUTING",
        ActionStatus::Canceling => "CANCELING",
        ActionStatus::Succeeded => "SUCCEEDED",
        ActionStatus::Aborted => "ABORTED",
        ActionStatus::Canceled => "CANCELED",
    }
}

fn reply_code_label(code: ActionReplyCode) -> &'static str {
    match code {
        ActionReplyCode::None => "NONE",
        ActionReplyCode::Ok => "OK",
        ActionReplyCode::DuplicateAccepted => "DUPLICATE_ACCEPTED",
        ActionReplyCode::Succeeded => "SUCCEEDED",
        ActionReplyCode::Canceled => "CANCELED",
        ActionReplyCode::Busy => "BUSY",
        ActionReplyCode::PayloadConflict => "PAYLOAD_CONFLICT",
        ActionReplyCode::Unauthorized => "UNAUTHORIZED",
        ActionReplyCode::NotFound => "NOT_FOUND",
        ActionReplyCode::AlreadyTerminal => "ALREADY_TERMINAL",
        ActionReplyCode::GoalExpired => "GOAL_EXPIRED",
        ActionReplyCode::HandlerError => "HANDLER_ERROR",
        ActionReplyCode::HandlerPanic => "HANDLER_PANIC",
        ActionReplyCode::HandlerCompletedWithoutResult => "HANDLER_COMPLETED_WITHOUT_RESULT",
        ActionReplyCode::ContextDropped => "CONTEXT_DROPPED",
        ActionReplyCode::ServerCrash => "SERVER_CRASH",
        ActionReplyCode::Conflict => "CONFLICT",
    }
}

fn yes_no(value: bool) -> &'static str {
    if value {
        "yes"
    } else {
        "no"
    }
}

fn send_follow_up_commands(
    action_name: &str,
    group: &str,
    goal_id: &str,
    goal_secret: &str,
) -> [String; 4] {
    let action = to_user_key(action_name);
    [
        format!(
            "aorta --group {} action status {} --goal-id {}",
            group, action, goal_id
        ),
        format!(
            "aorta --group {} action feedback {} --goal-id {}",
            group, action, goal_id
        ),
        format!(
            "aorta --group {} action wait {} --goal-id {} --goal-secret {} --timeout 300",
            group, action, goal_id, goal_secret
        ),
        format!(
            "aorta --group {} action cancel {} --goal-id {} --goal-secret {}",
            group, action, goal_id, goal_secret
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::{
        action_info_selector, parse_feedback_json, parse_get_result_response_json,
        parse_public_status_json, parse_reflected_json, read_json_argument, resolve_goal_id,
        send_follow_up_commands, FeedbackView, PublicStatusView,
    };
    use aorta_core::action::{ActionReplyCode, ActionStatus};
    use serde_json::{json, Value};
    use std::fs;

    #[test]
    fn action_info_selector_uses_recursive_wildcard() {
        let selector = action_info_selector("default").expect("selector");
        assert_eq!(selector, "aorta/default/act/info/**");
    }

    #[test]
    fn parse_public_status_json_accepts_enum_strings() {
        let value = json!({
            "goal_id": "550e8400-e29b-41d4-a716-446655440000",
            "status": "EXECUTING",
            "code": "OK",
            "accepted_stamp_ns": 10,
            "started_stamp_ns": 20,
            "finished_stamp_ns": 0,
            "last_feedback_seq": 7,
            "server_epoch": "c0f90d3a-2a1d-4dc6-a316-ff1d54596ea9"
        });

        assert_eq!(
            parse_public_status_json(&value).expect("status"),
            PublicStatusView {
                goal_id: "550e8400-e29b-41d4-a716-446655440000".to_string(),
                status: ActionStatus::Executing,
                code: ActionReplyCode::Ok,
                accepted_stamp_ns: 10,
                started_stamp_ns: 20,
                finished_stamp_ns: 0,
                last_feedback_seq: 7,
                server_epoch: "c0f90d3a-2a1d-4dc6-a316-ff1d54596ea9".to_string(),
            }
        );
    }

    #[test]
    fn parse_public_status_json_accepts_numeric_enums_and_string_u64s() {
        let value = json!({
            "goal_id": "550e8400-e29b-41d4-a716-446655440000",
            "status": 4,
            "code": 100,
            "accepted_stamp_ns": "11",
            "started_stamp_ns": "22",
            "finished_stamp_ns": "33",
            "last_feedback_seq": "44",
            "server_epoch": "c0f90d3a-2a1d-4dc6-a316-ff1d54596ea9"
        });

        let status = parse_public_status_json(&value).expect("status");
        assert_eq!(status.status, ActionStatus::Succeeded);
        assert_eq!(status.code, ActionReplyCode::Succeeded);
        assert_eq!(status.accepted_stamp_ns, 11);
        assert_eq!(status.last_feedback_seq, 44);
    }

    #[test]
    fn parse_public_status_json_defaults_missing_zero_fields() {
        let value = json!({
            "goal_id": "550e8400-e29b-41d4-a716-446655440000",
            "status": "ACCEPTED",
            "code": "OK",
            "accepted_stamp_ns": 11,
            "server_epoch": "c0f90d3a-2a1d-4dc6-a316-ff1d54596ea9"
        });

        let status = parse_public_status_json(&value).expect("status");
        assert_eq!(status.started_stamp_ns, 0);
        assert_eq!(status.finished_stamp_ns, 0);
        assert_eq!(status.last_feedback_seq, 0);
    }

    #[test]
    fn parse_get_result_response_json_preserves_nested_payloads() {
        let value = json!({
            "goal_id": "550e8400-e29b-41d4-a716-446655440000",
            "status": "SUCCEEDED",
            "accepted_stamp_ns": 11,
            "started_stamp_ns": 22,
            "finished_stamp_ns": 33,
            "last_feedback_seq": 4,
            "feedback_snapshot": {"progress_pct": 100},
            "result": {"duration_ms": 1234},
            "code": "SUCCEEDED",
            "message": "done",
            "server_epoch": "c0f90d3a-2a1d-4dc6-a316-ff1d54596ea9"
        });

        let response = parse_get_result_response_json(&value).expect("response");
        assert_eq!(response.status, ActionStatus::Succeeded);
        assert_eq!(response.code, ActionReplyCode::Succeeded);
        assert_eq!(response.feedback_snapshot["progress_pct"], 100);
        assert_eq!(response.result["duration_ms"], 1234);
    }

    #[test]
    fn parse_get_result_response_json_defaults_missing_zero_fields() {
        let value = json!({
            "goal_id": "550e8400-e29b-41d4-a716-446655440000",
            "status": "UNKNOWN",
            "code": "GOAL_EXPIRED",
            "message": "expired",
            "server_epoch": "c0f90d3a-2a1d-4dc6-a316-ff1d54596ea9"
        });

        let response = parse_get_result_response_json(&value).expect("response");
        assert_eq!(response.accepted_stamp_ns, 0);
        assert_eq!(response.started_stamp_ns, 0);
        assert_eq!(response.finished_stamp_ns, 0);
        assert_eq!(response.last_feedback_seq, 0);
        assert_eq!(response.feedback_snapshot, Value::Null);
        assert_eq!(response.result, Value::Null);
        assert_eq!(response.code, ActionReplyCode::GoalExpired);
    }

    #[test]
    fn parse_feedback_json_preserves_data_payload() {
        let value = json!({
            "goal_id": "550e8400-e29b-41d4-a716-446655440000",
            "seq": 9,
            "stamp_ns": 100,
            "status": "EXECUTING",
            "data": {"progress_pct": 42, "current_node": "plan"},
            "message": "running"
        });

        assert_eq!(
            parse_feedback_json(&value).expect("feedback"),
            FeedbackView {
                goal_id: "550e8400-e29b-41d4-a716-446655440000".to_string(),
                seq: 9,
                stamp_ns: 100,
                status: ActionStatus::Executing,
                data: json!({"progress_pct": 42, "current_node": "plan"}),
                message: "running".to_string(),
            }
        );
    }

    #[test]
    fn parse_reflected_json_accepts_relaxed_json5_output() {
        let value = parse_reflected_json(
            r#"{
  accepted: true,
  goal_id: "550e8400-e29b-41d4-a716-446655440000",
  goal_secret: "secret",
  code: "OK",
  status: "ACCEPTED",
  message: "",
  server_epoch: "epoch"
}"#,
            "action json",
        )
        .expect("value");

        assert_eq!(value["accepted"], true);
        assert_eq!(value["goal_id"], "550e8400-e29b-41d4-a716-446655440000");
    }

    #[test]
    fn read_json_argument_reads_inline_json() {
        let value = read_json_argument("{\"source\":\"quick\",\"input_type\":\"DagPresetRef\"}")
            .expect("json");
        assert_eq!(value["source"], "quick");
        assert_eq!(value["input_type"], "DagPresetRef");
    }

    #[test]
    fn read_json_argument_reads_at_file() {
        let path = std::env::temp_dir().join(format!(
            "aorta_cli_action_{}.json",
            uuid::Uuid::new_v4().simple()
        ));
        fs::write(&path, "{\"source\":\"quick\"}").expect("write json");

        let value = read_json_argument(&format!("@{}", path.display())).expect("json");
        assert_eq!(value["source"], "quick");

        let _ = fs::remove_file(path);
    }

    #[test]
    fn resolve_goal_id_generates_uuid_when_missing() {
        let goal_id = resolve_goal_id(None).expect("goal_id");
        assert_eq!(goal_id.len(), 36);
        assert!(goal_id.contains('-'));
    }

    #[test]
    fn send_follow_up_commands_render_copy_pasteable_commands() {
        let commands = send_follow_up_commands(
            "demo/execute_task",
            "default",
            "550e8400-e29b-41d4-a716-446655440000",
            "secret-token",
        );

        assert_eq!(
            commands[0],
            "aorta --group default action status /demo/execute_task --goal-id 550e8400-e29b-41d4-a716-446655440000"
        );
        assert_eq!(
            commands[1],
            "aorta --group default action feedback /demo/execute_task --goal-id 550e8400-e29b-41d4-a716-446655440000"
        );
        assert_eq!(
            commands[2],
            "aorta --group default action wait /demo/execute_task --goal-id 550e8400-e29b-41d4-a716-446655440000 --goal-secret secret-token --timeout 300"
        );
        assert_eq!(
            commands[3],
            "aorta --group default action cancel /demo/execute_task --goal-id 550e8400-e29b-41d4-a716-446655440000 --goal-secret secret-token"
        );
    }
}
