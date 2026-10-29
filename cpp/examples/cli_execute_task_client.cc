#include <chrono>
#include <cstdint>
#include <string>
#include <thread>
#include <vector>

#include "aorta/aorta.h"
#include "flow_resume_probe_msg.h"
#include "local_zenoh_config.h"

namespace {

using FlowResumeProbeAction = ::aorta::action::example::FlowResumeProbeAction;
using ProbeInputMsg = ::aorta::action::example::ProbeInputMsg;
using ProbePresetMsg = ::aorta::action::example::ProbePresetMsg;
using FlowResumeProbeGoalMsg = ::aorta::action::example::FlowResumeProbeGoalMsg;

std::string StringOrEmpty(const flatbuffers::String* value) {
  return value == nullptr ? std::string() : value->str();
}

const char* StatusLabel(aorta::ActionStatus status) {
  switch (status) {
    case aorta::ActionStatus::kUnknown:
      return "UNKNOWN";
    case aorta::ActionStatus::kAccepted:
      return "ACCEPTED";
    case aorta::ActionStatus::kExecuting:
      return "EXECUTING";
    case aorta::ActionStatus::kCanceling:
      return "CANCELING";
    case aorta::ActionStatus::kSucceeded:
      return "SUCCEEDED";
    case aorta::ActionStatus::kAborted:
      return "ABORTED";
    case aorta::ActionStatus::kCanceled:
      return "CANCELED";
  }
  return "UNKNOWN";
}

}  // namespace

int main(int argc, char* argv[]) {
  return aorta::RunOrFatal([&]() -> int {
  (void)aorta::InitLoggingFromEnv();

  std::string node_name = "cli_execute_task_client";
  std::string group = "default";
  std::string source = "quick";
  std::string preset = "";
  std::string goal_id;
  std::int64_t wait_timeout_ms = 10'000;
  std::int64_t cancel_after_ms = -1;

  for (int i = 1; i < argc - 1; ++i) {
    if (std::string(argv[i]) == "--name") {
      node_name = argv[++i];
    } else if (std::string(argv[i]) == "--group") {
      group = argv[++i];
    } else if (std::string(argv[i]) == "--source") {
      source = argv[++i];
    } else if (std::string(argv[i]) == "--preset") {
      preset = argv[++i];
    } else if (std::string(argv[i]) == "--goal-id") {
      goal_id = argv[++i];
    } else if (std::string(argv[i]) == "--wait-timeout-ms") {
      wait_timeout_ms = std::stoll(argv[++i]);
    } else if (std::string(argv[i]) == "--cancel-after-ms") {
      cancel_after_ms = std::stoll(argv[++i]);
    }
  }

  if (cancel_after_ms < 0 && source == "cancel") {
    cancel_after_ms = 600;
  }
  if (preset.empty()) {
    preset = "demo/" + source;
  }

  (void)aorta::SetDefaultLogContext(node_name, group);

  auto zenoh_config = aorta::Must(
      aorta::examples::ScopedLocalZenohConfig::InstallIfNeeded(
          aorta::examples::LocalZenohConfigMode::kActionClient),
      "Install local example zenoh config");
  if (zenoh_config.installed()) {
    AORTA_LOG(INFO)
        << "using bundled loopback-only zenoh config: "
        << zenoh_config.path();
  }

  auto node = aorta::Must(aorta::Node::Create(node_name, group), "Create node");
  auto client = aorta::Must(
      node->CreateActionClient<FlowResumeProbeAction>(FlowResumeProbeAction::ActionName()),
      "Create action client");
  auto info = aorta::Must(client->Info(), "Query action info");

  AORTA_LOG(INFO) << "cli_execute_task_client node=" << node->name()
                  << " group=" << node->group()
                  << " action=" << client->ActionName();
  AORTA_LOG(INFO) << "server_node=" << info.server_node_name
                  << " server_epoch=" << info.server_epoch
                  << " schema_hash=" << info.schema_hash_hex;

  FlowResumeProbeGoalMsg goal;
  goal.source = source;
  auto preset_ref = std::make_shared<ProbePresetMsg>();
  preset_ref->path = preset;
  goal.input = ProbeInputMsg{preset_ref};

  auto handle = [&]() {
    if (goal_id.empty()) {
      return aorta::Must(client->SendGoal(goal), "Send action goal");
    }
    return aorta::Must(client->SendGoalWithId(goal_id, goal),
                       "Send action goal");
  }();

  AORTA_LOG(INFO) << "goal accepted"
                  << " goal_id=" << handle.goal_id()
                  << " goal_secret=" << handle.goal_secret()
                  << " server_epoch=" << handle.server_epoch();

  aorta::CheckOk(
      handle.SetFeedbackCallback([](const FlowResumeProbeAction::Feedback& feedback) {
        const auto* data = feedback.data();
        const auto* current_node =
            data == nullptr ? nullptr : data->current_node();
        const auto completed_nodes = data == nullptr ? 0 : data->completed_nodes();
        const auto total_nodes = data == nullptr ? 0 : data->total_nodes();
        const auto progress_pct = data == nullptr ? 0 : data->progress_pct();

        AORTA_LOG(INFO) << "feedback goal_id="
                        << StringOrEmpty(feedback.goal_id())
                        << " seq=" << feedback.seq()
                        << " status=" << static_cast<int>(feedback.status())
                        << " current_node=" << StringOrEmpty(current_node)
                        << " completed=" << completed_nodes << "/" << total_nodes
                        << " progress_pct=" << static_cast<int>(progress_pct)
                        << " message=" << StringOrEmpty(feedback.message());
      }),
      "Set action feedback callback");

  if (cancel_after_ms >= 0) {
    AORTA_LOG(INFO) << "will request cancel after " << cancel_after_ms << " ms";
    std::this_thread::sleep_for(std::chrono::milliseconds(cancel_after_ms));
    auto cancel = handle.Cancel();
    if (cancel.ok()) {
      AORTA_LOG(INFO) << "cancel accepted goal_id=" << cancel->goal_id
                      << " status=" << StatusLabel(cancel->status);
    } else {
      AORTA_LOG(WARN) << "cancel failed: " << cancel.status().message();
    }
  }

  auto result = aorta::Must(
      handle.WaitResult(std::chrono::milliseconds(wait_timeout_ms)),
      "Wait action result");

  AORTA_LOG(INFO) << "result goal_id=" << result.goal_id
                  << " status=" << StatusLabel(result.status)
                  << " code=" << static_cast<int>(result.code)
                  << " feedback_seq=" << result.last_feedback_seq
                  << " message=" << result.message
                  << " server_epoch=" << result.server_epoch;

  if (result.feedback_snapshot.current_node.has_value()) {
    AORTA_LOG(INFO) << "feedback_snapshot.current_node="
                    << result.feedback_snapshot.current_node.value();
  }
  if (result.feedback_snapshot.completed_nodes.has_value() &&
      result.feedback_snapshot.total_nodes.has_value()) {
    AORTA_LOG(INFO) << "feedback_snapshot.completed="
                    << result.feedback_snapshot.completed_nodes.value() << "/"
                    << result.feedback_snapshot.total_nodes.value();
  }
  if (result.result.has_value() && result.result->duration_ms.has_value()) {
    AORTA_LOG(INFO) << "result.duration_ms="
                    << result.result->duration_ms.value();
  }

  return 0;
  });
}
