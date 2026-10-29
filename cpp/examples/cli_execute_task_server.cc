#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <string>
#include <thread>
#include <utility>

#include "aorta/aorta.h"
#include "flow_resume_probe_msg.h"
#include "local_zenoh_config.h"

namespace {

using FlowResumeProbeAction = ::aorta::action::example::FlowResumeProbeAction;
using FlowResumeProbeFeedbackDataMsg = ::aorta::action::example::FlowResumeProbeFeedbackDataMsg;
using FlowResumeProbeResultMsg = ::aorta::action::example::FlowResumeProbeResultMsg;

std::atomic<bool> g_running{true};

void signal_handler(int) { g_running = false; }

std::pair<std::uint32_t, std::chrono::milliseconds> GoalProfile(
    const std::string& source) {
  if (source == "quick") {
    return {4, std::chrono::milliseconds(100)};
  }
  if (source == "cancel") {
    return {20, std::chrono::milliseconds(200)};
  }
  if (source == "slow") {
    return {12, std::chrono::milliseconds(200)};
  }
  return {6, std::chrono::milliseconds(150)};
}

}  // namespace

int main(int argc, char* argv[]) {
  return aorta::RunOrFatal([&]() -> int {
  std::signal(SIGINT, signal_handler);
  std::signal(SIGTERM, signal_handler);
  (void)aorta::InitLoggingFromEnv();

  std::string node_name = "cli_execute_task_server";
  std::string group = "default";

  for (int i = 1; i < argc - 1; ++i) {
    if (std::string(argv[i]) == "--name") {
      node_name = argv[++i];
    } else if (std::string(argv[i]) == "--group") {
      group = argv[++i];
    }
  }
  (void)aorta::SetDefaultLogContext(node_name, group);

  auto zenoh_config = aorta::Must(
      aorta::examples::ScopedLocalZenohConfig::InstallIfNeeded(
          aorta::examples::LocalZenohConfigMode::kActionServer),
      "Install local example zenoh config");
  if (zenoh_config.installed()) {
    AORTA_LOG(INFO)
        << "using bundled loopback-only zenoh config: "
        << zenoh_config.path();
  }

  auto node = aorta::Must(aorta::Node::Create(node_name, group), "Create node");

  auto server = aorta::Must(
      aorta::ActionServer<FlowResumeProbeAction>::Serve(
          node, FlowResumeProbeAction::ActionName(), aorta::ActionServerOptions(),
          [](aorta::RetainedActionGoal<FlowResumeProbeAction> goal,
             aorta::ActionExecutionContext<FlowResumeProbeAction> ctx) {
            const auto* goal_view = goal.message();
            const std::string source = goal_view->source() == nullptr
                                           ? std::string()
                                           : goal_view->source()->str();
            const auto [total_nodes, step_duration] = GoalProfile(source);
            const auto started = std::chrono::steady_clock::now();

            AORTA_LOG(INFO) << "execute_task accepted goal_id=" << goal.goal_id()
                            << " source=" << source;

            for (std::uint32_t completed_nodes = 1; completed_nodes <= total_nodes;
                 ++completed_nodes) {
              if (ctx.IsCancelRequested()) {
                AORTA_LOG(INFO) << "execute_task cancel requested goal_id="
                                << ctx.goal_id();
                aorta::CheckOk(
                    ctx.Cancel(aorta::ActionReplyCode::kCanceled,
                               "canceled by client"),
                    "Cancel action goal");
                return;
              }

              std::this_thread::sleep_for(step_duration);

              if (ctx.IsCancelRequested()) {
                AORTA_LOG(INFO) << "execute_task cancel requested goal_id="
                                << ctx.goal_id();
                aorta::CheckOk(
                    ctx.Cancel(aorta::ActionReplyCode::kCanceled,
                               "canceled by client"),
                    "Cancel action goal");
                return;
              }

              FlowResumeProbeFeedbackDataMsg feedback;
              feedback.current_node = std::string("step-") +
                                      std::to_string(completed_nodes);
              feedback.completed_nodes = completed_nodes;
              feedback.total_nodes = total_nodes;
              feedback.progress_pct =
                  static_cast<std::uint8_t>((completed_nodes * 100) / total_nodes);
              aorta::CheckOk(ctx.PublishFeedback(feedback, "working"),
                             "Publish action feedback");
            }

            FlowResumeProbeResultMsg result;
            result.duration_ms = static_cast<std::uint64_t>(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - started)
                    .count());
            aorta::CheckOk(ctx.Succeed(result), "Complete action goal");
          }),
      "Create action server");

  AORTA_LOG(INFO) << "cli_execute_task_server node=" << node->name()
                  << " group=" << node->group()
                  << " action=" << FlowResumeProbeAction::ActionName();
  AORTA_LOG(INFO) << "action schema hash="
                  << aorta::SchemaMeta<FlowResumeProbeAction::Goal>::SchemaHashHex();
  AORTA_LOG(INFO) << "Action server is ready.";
  AORTA_LOG(INFO) << "Press Ctrl+C to exit";

  while (g_running.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  AORTA_LOG(INFO) << "Shutting down...";
  (void)server;
  return 0;
  });
}
