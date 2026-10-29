#include <atomic>
#include <chrono>
#include <cmath>
#include <csignal>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "aorta/aorta.h"
#include "navigation_msg.h"
#include "schema_attach_policy.h"

namespace {

namespace nav = aorta::services::navigation;
namespace common = aorta::services::common;

std::atomic<bool> g_running{true};

void signal_handler(int) { g_running = false; }

flatbuffers::Offset<aorta::sys::AortaHeader> BuildHeader(
    flatbuffers::FlatBufferBuilder& builder, const std::string& node_name,
    uint64_t sequence, uint64_t publish_stamp_ns) {
  auto node = builder.CreateString(node_name);
  return aorta::sys::CreateAortaHeader(builder, publish_stamp_ns, node, sequence);
}

std::vector<uint8_t> BuildGetPlanResponse(
    const std::string& node_name, uint64_t sequence, uint64_t now_ns,
    const nav::GetPlanRequest* request) {
  const float dx = request->goal_x() - request->start_x();
  const float dy = request->goal_y() - request->start_y();
  const float dz = request->goal_z() - request->start_z();
  const float path_length = std::sqrt(dx * dx + dy * dy + dz * dz);

  std::vector<float> waypoints{
      request->start_x(),
      request->start_y(),
      request->start_z(),
      (request->start_x() + request->goal_x()) * 0.5f,
      (request->start_y() + request->goal_y()) * 0.5f,
      (request->start_z() + request->goal_z()) * 0.5f,
      request->goal_x(),
      request->goal_y(),
      request->goal_z(),
  };

  flatbuffers::FlatBufferBuilder builder;
  auto header = BuildHeader(builder, node_name, sequence, now_ns);
  auto waypoints_offset = builder.CreateVector(waypoints);
  auto response = nav::CreateGetPlanResponse(
      builder, header, common::ServiceStatus_SUCCESS, waypoints_offset, 3,
      path_length, 2, 0);
  builder.Finish(response);
  const auto* ptr = builder.GetBufferPointer();
  return std::vector<uint8_t>(ptr, ptr + builder.GetSize());
}

std::vector<uint8_t> BuildGetPlanInvalidResponse(
    const std::string& node_name, uint64_t sequence, uint64_t now_ns,
    const std::string& error_message) {
  flatbuffers::FlatBufferBuilder builder;
  auto header = BuildHeader(builder, node_name, sequence, now_ns);
  auto error = builder.CreateString(error_message);
  auto response = nav::CreateGetPlanResponse(
      builder, header, common::ServiceStatus_INVALID_REQUEST, 0, 0, 0.0f, 0,
      error);
  builder.Finish(response);
  const auto* ptr = builder.GetBufferPointer();
  return std::vector<uint8_t>(ptr, ptr + builder.GetSize());
}

std::vector<uint8_t> BuildCancelPlanResponse(
    const std::string& node_name, uint64_t sequence, uint64_t now_ns,
    const nav::CancelPlanRequest* request) {
  const auto* plan_id_fb = request->plan_id();
  const std::string plan_id = plan_id_fb ? plan_id_fb->str() : std::string();
  const bool cancelled = request->force() || !plan_id.empty();
  const auto status = cancelled ? common::ServiceStatus_SUCCESS
                                : common::ServiceStatus_INVALID_REQUEST;
  const std::string error_message =
      cancelled ? "" : "plan_id is empty and force=false";

  flatbuffers::FlatBufferBuilder builder;
  auto header = BuildHeader(builder, node_name, sequence, now_ns);
  auto error = error_message.empty() ? 0 : builder.CreateString(error_message);
  auto response =
      nav::CreateCancelPlanResponse(builder, header, status, cancelled, error);
  builder.Finish(response);
  const auto* ptr = builder.GetBufferPointer();
  return std::vector<uint8_t>(ptr, ptr + builder.GetSize());
}

std::vector<uint8_t> BuildCancelPlanInvalidResponse(
    const std::string& node_name, uint64_t sequence, uint64_t now_ns,
    const std::string& error_message) {
  flatbuffers::FlatBufferBuilder builder;
  auto header = BuildHeader(builder, node_name, sequence, now_ns);
  auto error = builder.CreateString(error_message);
  auto response = nav::CreateCancelPlanResponse(
      builder, header, common::ServiceStatus_INVALID_REQUEST, false, error);
  builder.Finish(response);
  const auto* ptr = builder.GetBufferPointer();
  return std::vector<uint8_t>(ptr, ptr + builder.GetSize());
}

std::vector<uint8_t> BuildCurrentPoseResponse(const std::string& node_name,
                                              uint64_t sequence, uint64_t now_ns,
                                              const std::string& frame_id) {
  flatbuffers::FlatBufferBuilder builder;
  auto header = BuildHeader(builder, node_name, sequence, now_ns);
  auto frame = builder.CreateString(frame_id);
  auto response = nav::CreateGetCurrentPoseResponse(
      builder, header, common::ServiceStatus_SUCCESS, 1.2f, -0.3f, 0.0f, 0.0f,
      0.0f, 0.0f, 1.0f, now_ns, frame);
  builder.Finish(response);
  const auto* ptr = builder.GetBufferPointer();
  return std::vector<uint8_t>(ptr, ptr + builder.GetSize());
}

std::vector<uint8_t> BuildCurrentPoseInvalidResponse(
    const std::string& node_name, uint64_t sequence, uint64_t now_ns) {
  flatbuffers::FlatBufferBuilder builder;
  auto header = BuildHeader(builder, node_name, sequence, now_ns);
  auto frame = builder.CreateString("map");
  auto response = nav::CreateGetCurrentPoseResponse(
      builder, header, common::ServiceStatus_INVALID_REQUEST, 0.0f, 0.0f, 0.0f,
      0.0f, 0.0f, 0.0f, 1.0f, now_ns, frame);
  builder.Finish(response);
  const auto* ptr = builder.GetBufferPointer();
  return std::vector<uint8_t>(ptr, ptr + builder.GetSize());
}

}  // namespace

int main(int argc, char* argv[]) {
  return aorta::RunOrFatal([&]() -> int {
  std::signal(SIGINT, signal_handler);
  std::signal(SIGTERM, signal_handler);
  (void)aorta::InitLoggingFromEnv();

  std::string node_name = "navigation_service_server";
  std::string group = "default";
  std::string get_plan_service = "/get_plan";
  std::string cancel_plan_service = "/cancel_plan";
  std::string current_pose_service = "/get_current_pose";
  std::string schema_attach = "never";

  for (int i = 1; i < argc - 1; i++) {
    if (std::string(argv[i]) == "--name") {
      node_name = argv[++i];
    } else if (std::string(argv[i]) == "--group") {
      group = argv[++i];
    } else if (std::string(argv[i]) == "--get-plan-service") {
      get_plan_service = argv[++i];
    } else if (std::string(argv[i]) == "--cancel-plan-service") {
      cancel_plan_service = argv[++i];
    } else if (std::string(argv[i]) == "--current-pose-service") {
      current_pose_service = argv[++i];
    } else if (std::string(argv[i]) == "--schema-attach") {
      schema_attach = argv[++i];
    }
  }
  (void)aorta::SetDefaultLogContext(node_name, group);

  aorta::SchemaAttachPolicy attach_policy = aorta::SchemaAttachPolicy::kNever;
  if (!aorta::examples::ParseSchemaAttachPolicy(schema_attach, &attach_policy)) {
    AORTA_LOG(ERROR) << "Invalid --schema-attach value: " << schema_attach
                     << " (expected never|always|inherit)";
    return 2;
  }

  auto node = aorta::Must(aorta::Node::Create(node_name, group), "Create node");

  aorta::ServiceOptions options;
  options.schema_attach_policy = attach_policy;

  AORTA_LOG(INFO) << "Navigation service node=" << node->name()
                  << " group=" << node->group();
  AORTA_LOG(INFO) << "navigation schema hash="
                  << aorta::SchemaMeta<nav::GetPlanRequest>::SchemaHashHex();

  auto get_plan_seq = std::make_shared<std::atomic<uint64_t>>(0);
  auto cancel_plan_seq = std::make_shared<std::atomic<uint64_t>>(0);
  auto pose_seq = std::make_shared<std::atomic<uint64_t>>(0);
  auto server_name = node->name();

  auto get_plan_result =
      node->CreateService<nav::GetPlanRequest, nav::GetPlanResponse>(
          get_plan_service,
          [server_name, get_plan_seq](const aorta::MessageView& request,
                                      aorta::ServiceRequest&& req) {
            const uint64_t seq = get_plan_seq->fetch_add(1, std::memory_order_relaxed);
            uint64_t now_ns = 0;
            auto now_or = aorta::NowNs();
            if (!now_or.ok()) {
              AORTA_LOG(WARN) << "Failed to read global core clock: "
                              << now_or.status().message();
            } else if (now_or.value() > 0) {
              now_ns = static_cast<uint64_t>(now_or.value());
            }
            auto payload = request.ToVector();
            std::vector<uint8_t> response;

            flatbuffers::Verifier verifier(payload.data(), payload.size());
            if (!verifier.VerifyBuffer<nav::GetPlanRequest>(nullptr)) {
              response = BuildGetPlanInvalidResponse(
                  server_name, seq, now_ns, "invalid GetPlanRequest payload");
            } else {
              const auto* msg = flatbuffers::GetRoot<nav::GetPlanRequest>(payload.data());
              response = BuildGetPlanResponse(server_name, seq, now_ns, msg);
            }

            aorta::CheckOk(req.Reply(std::move(response)),
                           "Reply GetPlan response");
          },
          options);
  auto get_plan =
      aorta::Must(std::move(get_plan_result), "Create get_plan service");

  auto cancel_plan_result =
      node->CreateService<nav::CancelPlanRequest, nav::CancelPlanResponse>(
          cancel_plan_service,
          [server_name, cancel_plan_seq](const aorta::MessageView& request,
                                         aorta::ServiceRequest&& req) {
            const uint64_t seq = cancel_plan_seq->fetch_add(1, std::memory_order_relaxed);
            uint64_t now_ns = 0;
            auto now_or = aorta::NowNs();
            if (!now_or.ok()) {
              AORTA_LOG(WARN) << "Failed to read global core clock: "
                              << now_or.status().message();
            } else if (now_or.value() > 0) {
              now_ns = static_cast<uint64_t>(now_or.value());
            }
            auto payload = request.ToVector();
            std::vector<uint8_t> response;

            flatbuffers::Verifier verifier(payload.data(), payload.size());
            if (!verifier.VerifyBuffer<nav::CancelPlanRequest>(nullptr)) {
              response = BuildCancelPlanInvalidResponse(
                  server_name, seq, now_ns, "invalid CancelPlanRequest payload");
            } else {
              const auto* msg = flatbuffers::GetRoot<nav::CancelPlanRequest>(payload.data());
              response = BuildCancelPlanResponse(server_name, seq, now_ns, msg);
            }

            aorta::CheckOk(req.Reply(std::move(response)),
                           "Reply CancelPlan response");
          },
          options);
  auto cancel_plan =
      aorta::Must(std::move(cancel_plan_result), "Create cancel_plan service");

  auto current_pose_result =
      node->CreateService<nav::GetCurrentPoseRequest, nav::GetCurrentPoseResponse>(
          current_pose_service,
          [server_name, pose_seq](const aorta::MessageView& request,
                                  aorta::ServiceRequest&& req) {
            const uint64_t seq = pose_seq->fetch_add(1, std::memory_order_relaxed);
            uint64_t now_ns = 0;
            auto now_or = aorta::NowNs();
            if (!now_or.ok()) {
              AORTA_LOG(WARN) << "Failed to read global core clock: "
                              << now_or.status().message();
            } else if (now_or.value() > 0) {
              now_ns = static_cast<uint64_t>(now_or.value());
            }
            auto payload = request.ToVector();
            std::vector<uint8_t> response;

            flatbuffers::Verifier verifier(payload.data(), payload.size());
            if (!verifier.VerifyBuffer<nav::GetCurrentPoseRequest>(nullptr)) {
              response = BuildCurrentPoseInvalidResponse(server_name, seq, now_ns);
            } else {
              const auto* msg =
                  flatbuffers::GetRoot<nav::GetCurrentPoseRequest>(payload.data());
              const auto* frame_id_fb = msg->frame_id();
              const std::string frame_id = frame_id_fb ? frame_id_fb->str() : "map";
              response = BuildCurrentPoseResponse(server_name, seq, now_ns, frame_id);
            }

            aorta::CheckOk(req.Reply(std::move(response)),
                           "Reply GetCurrentPose response");
          },
          options);
  auto current_pose = aorta::Must(std::move(current_pose_result),
                                  "Create get_current_pose service");

  (void)get_plan;
  (void)cancel_plan;
  (void)current_pose;

  AORTA_LOG(INFO) << "Navigation services ready:";
  AORTA_LOG(INFO) << "  - " << get_plan_service;
  AORTA_LOG(INFO) << "  - " << cancel_plan_service;
  AORTA_LOG(INFO) << "  - " << current_pose_service;
  AORTA_LOG(INFO) << "Press Ctrl+C to exit";

  while (g_running) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  AORTA_LOG(INFO) << "Shutting down...";
  return 0;
  });
}
