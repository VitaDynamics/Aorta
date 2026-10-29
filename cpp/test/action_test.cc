#include "aorta/action.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "flow_resume_probe_msg.h"
#include "mock_aorta.h"

namespace aorta {
namespace {

using TestAction = ::aorta::action::example::FlowResumeProbeAction;
using ::testing::_;
using ::testing::AllOf;
using ::testing::Ge;
using ::testing::Invoke;
using ::testing::Le;
using ::testing::Return;
using ::testing::StrEq;

constexpr const char* kGoalId = "550e8400-e29b-41d4-a716-446655440000";
constexpr const char* kGoalSecret = "secret-token";
constexpr const char* kServerEpoch = "550e8400-e29b-41d4-a716-446655440001";

struct MockOwnedBytesStorage {
  std::vector<uint8_t> payload;
  aorta_bytes_slice_t slice{};
};

void FillOwnedBytesResponse(aorta_owned_bytes_t* out,
                            std::vector<uint8_t> payload) {
  auto* storage = new MockOwnedBytesStorage();
  storage->payload = std::move(payload);
  storage->slice = aorta_bytes_slice_t{storage->payload.data(),
                                       storage->payload.size()};
  out->view.slices = storage->payload.empty() ? nullptr : &storage->slice;
  out->view.slice_count = storage->payload.empty() ? 0 : 1;
  out->view.total_len = storage->payload.size();
  out->internal = storage;
}

void FreeOwnedBytesResponse(aorta_owned_bytes_t* bytes) {
  auto* storage = static_cast<MockOwnedBytesStorage*>(bytes->internal);
  delete storage;
  *bytes = aorta_owned_bytes_t{};
}

std::vector<uint8_t> CopyBytesViewPayload(const aorta_bytes_view_t* view) {
  if (view == nullptr || view->slice_count == 0 || view->slices == nullptr) {
    return {};
  }

  std::vector<uint8_t> payload;
  payload.reserve(view->total_len);
  for (size_t i = 0; i < view->slice_count; ++i) {
    const auto& slice = view->slices[i];
    if (slice.len == 0 || slice.data == nullptr) {
      continue;
    }
    payload.insert(payload.end(), slice.data, slice.data + slice.len);
  }
  return payload;
}

std::string CopyStringViewPayload(const aorta_string_view_t& view) {
  if (view.data == nullptr || view.len == 0) {
    return {};
  }
  return std::string(reinterpret_cast<const char*>(view.data), view.len);
}

::aorta::action::common::ActionReplyCode WireReplyCode(ActionReplyCode code) {
  return static_cast<::aorta::action::common::ActionReplyCode>(
      static_cast<int16_t>(code));
}

::aorta::action::common::ActionStatus WireStatus(ActionStatus status) {
  return static_cast<::aorta::action::common::ActionStatus>(
      static_cast<int8_t>(status));
}

std::vector<uint8_t> BuildActionInfoPayload() {
  flatbuffers::FlatBufferBuilder builder;
  auto action_name = builder.CreateString("example/flow_resume_probe");
  auto schema_hash = builder.CreateString("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef");
  auto server_node = builder.CreateString("planner_server");
  auto server_epoch = builder.CreateString(kServerEpoch);
  auto root = ::aorta::action::common::CreateActionInfo(
      builder, action_name, schema_hash, true, 4, 60000, 300000, server_node,
      server_epoch);
  builder.Finish(root);
  return std::vector<uint8_t>(builder.GetBufferPointer(),
                              builder.GetBufferPointer() + builder.GetSize());
}

std::vector<uint8_t> BuildSendGoalResponsePayload(
    bool accepted, ActionReplyCode code = ActionReplyCode::kOk,
    ActionStatus status = ActionStatus::kAccepted) {
  flatbuffers::FlatBufferBuilder builder;
  auto goal_id = builder.CreateString(kGoalId);
  auto goal_secret = builder.CreateString(kGoalSecret);
  auto message = builder.CreateString(accepted ? "accepted" : "rejected");
  auto server_epoch = builder.CreateString(kServerEpoch);
  auto root = ::aorta::action::example::CreateFlowResumeProbeSendGoalResponse(
      builder, accepted, goal_id, goal_secret, WireReplyCode(code),
      WireStatus(status), message, server_epoch);
  builder.Finish(root);
  return std::vector<uint8_t>(builder.GetBufferPointer(),
                              builder.GetBufferPointer() + builder.GetSize());
}

std::vector<uint8_t> BuildPublicStatusPayload(
    ActionStatus status = ActionStatus::kExecuting,
    ActionReplyCode code = ActionReplyCode::kOk) {
  flatbuffers::FlatBufferBuilder builder;
  auto goal_id = builder.CreateString(kGoalId);
  auto server_epoch = builder.CreateString(kServerEpoch);
  auto root = ::aorta::action::example::CreateFlowResumeProbePublicStatus(
      builder, goal_id, WireStatus(status), WireReplyCode(code), 100, 200,
      status == ActionStatus::kSucceeded ? 300 : 0, 7, server_epoch);
  builder.Finish(root);
  return std::vector<uint8_t>(builder.GetBufferPointer(),
                              builder.GetBufferPointer() + builder.GetSize());
}

std::vector<uint8_t> BuildCancelGoalResponsePayload(
    bool accepted, ActionReplyCode code = ActionReplyCode::kOk,
    ActionStatus status = ActionStatus::kCanceling) {
  flatbuffers::FlatBufferBuilder builder;
  auto goal_id = builder.CreateString(kGoalId);
  auto message = builder.CreateString(accepted ? "canceling" : "rejected");
  auto root = ::aorta::action::example::CreateFlowResumeProbeCancelGoalResponse(
      builder, accepted, goal_id, WireReplyCode(code), WireStatus(status),
      message);
  builder.Finish(root);
  return std::vector<uint8_t>(builder.GetBufferPointer(),
                              builder.GetBufferPointer() + builder.GetSize());
}

std::vector<uint8_t> BuildGetResultResponsePayload(
    ActionStatus status, bool include_result,
    ActionReplyCode code = ActionReplyCode::kOk) {
  flatbuffers::FlatBufferBuilder builder;
  auto goal_id = builder.CreateString(kGoalId);
  auto current_node = builder.CreateString("node_c");
  auto message = builder.CreateString(
      status == ActionStatus::kSucceeded ? "done" : "working");
  auto server_epoch = builder.CreateString(kServerEpoch);
  auto feedback = ::aorta::action::example::CreateFlowResumeProbeFeedbackData(
      builder, current_node, 3, 5, 60);
  flatbuffers::Offset<::aorta::action::example::FlowResumeProbeResult> result;
  if (include_result) {
    result = ::aorta::action::example::CreateFlowResumeProbeResult(builder, 1234);
  }
  auto root = ::aorta::action::example::CreateFlowResumeProbeGetResultResponse(
      builder, goal_id, WireStatus(status), 100, 200,
      status == ActionStatus::kSucceeded ? 300 : 0, 9, feedback,
      include_result ? result
                     : flatbuffers::Offset<::aorta::action::example::FlowResumeProbeResult>(),
      WireReplyCode(code), message, server_epoch);
  builder.Finish(root);
  return std::vector<uint8_t>(builder.GetBufferPointer(),
                              builder.GetBufferPointer() + builder.GetSize());
}

std::vector<uint8_t> BuildFeedbackPayload(const char* goal_id,
                                          const char* current_node) {
  ::aorta::action::example::FlowResumeProbeFeedbackMsg msg;
  msg.goal_id = goal_id;
  msg.seq = 7;
  msg.stamp_ns = 456;
  msg.status = WireStatus(ActionStatus::kExecuting);
  auto data = std::make_shared<::aorta::action::example::FlowResumeProbeFeedbackDataMsg>();
  data->current_node = std::string(current_node);
  data->completed_nodes = 1;
  data->total_nodes = 3;
  data->progress_pct = 33;
  msg.data = data;
  msg.message = std::string("working");

  flatbuffers::FlatBufferBuilder builder;
  auto root = ::aorta::detail::typed_aorta_action_example_flow_resume_probe::
      BuildFlowResumeProbeFeedbackTyped(builder, msg, {});
  builder.Finish(root);
  return std::vector<uint8_t>(builder.GetBufferPointer(),
                              builder.GetBufferPointer() + builder.GetSize());
}

TEST(ActionTest, ServeInfoOnlyUsesActionNameSchemaAndOptions) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 500;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock,
              aorta_action_server_info_new_with_options(500,
                                                        StrEq("example/flow_resume_probe"),
                                                        _, _, _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*, const uint8_t* schema_bfbs,
                          size_t schema_bfbs_len,
                          const aorta_action_server_options_t* opts,
                          aorta_action_server_t* out) {
        const auto* expected_bfbs =
            SchemaMeta<TestAction::Goal>::SchemaBfbs();
        const auto expected_bfbs_len =
            SchemaMeta<TestAction::Goal>::SchemaBfbsSize();
        EXPECT_NE(schema_bfbs, nullptr);
        EXPECT_EQ(schema_bfbs_len, expected_bfbs_len);
        EXPECT_TRUE(std::equal(schema_bfbs, schema_bfbs + schema_bfbs_len,
                               expected_bfbs));
        EXPECT_NE(opts, nullptr);
        EXPECT_EQ(opts->supports_cancel, 0u);
        EXPECT_EQ(opts->max_concurrency, 3u);
        EXPECT_EQ(opts->result_ttl_ms, 1200u);
        EXPECT_EQ(opts->dedup_ttl_ms, 3400u);
        *out = 501;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_action_server_free(501)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(500)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("action_server_node");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();

  ActionServerOptions options;
  options.supports_cancel = false;
  options.max_concurrency = 3;
  options.result_ttl = std::chrono::milliseconds(1200);
  options.dedup_ttl = std::chrono::milliseconds(3400);

  auto server_result =
      ActionServer<TestAction>::ServeInfoOnly(node, TestAction::ActionName(),
                                              options);
  ASSERT_TRUE(server_result.ok());
  auto server = server_result.value();
  EXPECT_EQ(server->ActionName(), "example/flow_resume_probe");
}

TEST(ActionTest, CreateActionClientUsesActionNameAndOptions) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 400;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock,
              aorta_action_client_new_with_options(400, StrEq("example/flow_resume_probe"),
                                                   _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*,
                          const aorta_action_client_options_t* opts,
                          aorta_action_client_t* out) {
        EXPECT_NE(opts, nullptr);
        EXPECT_EQ(opts->control_timeout_ms, 1200u);
        EXPECT_EQ(opts->info_timeout_ms, 3400u);
        *out = 401;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_action_client_free(401)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(400)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("action_client_node");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();

  ActionClientOptions options;
  options.control_timeout = std::chrono::milliseconds(1200);
  options.info_timeout = std::chrono::milliseconds(3400);
  auto client_result =
      node->CreateActionClient<TestAction>(TestAction::ActionName(), options);
  ASSERT_TRUE(client_result.ok());
}

TEST(ActionTest, CreateActionClientNormalizesActionName) {
  // A denormalized action name (leading/trailing '/') must be normalized
  // before it reaches the FFI ctor AND before it is stored for client-side
  // key building (status subscriptions), so it matches the keys the server
  // publishes under.
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 470;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_action_client_new_with_options(
                        470, StrEq("example/flow_resume_probe"), _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*,
                          const aorta_action_client_options_t*,
                          aorta_action_client_t* out) {
        *out = 471;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_action_client_free(471)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(470)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("action_client_norm_node");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();

  auto client_result =
      node->CreateActionClient<TestAction>("/example/flow_resume_probe/");
  ASSERT_TRUE(client_result.ok());
  EXPECT_EQ(client_result.value()->ActionName(), "example/flow_resume_probe");
}

TEST(ActionTest, ServeUsesCodecAndOptions) {
  MockAortaFFI mock;
  aorta_action_server_codec_t captured_codec{};

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 470;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_action_server_new_with_callbacks(
                        470, StrEq("example/flow_resume_probe"), _, _, _, _, _, _))
      .WillOnce(Invoke([&](aorta_node_t, const char*, const uint8_t* schema_bfbs,
                           size_t schema_bfbs_len,
                           const aorta_action_server_options_t* opts,
                           const aorta_action_server_codec_t* codec,
                           const aorta_action_server_callbacks_t* callbacks,
                           aorta_action_server_t* out) -> aorta_result_t {
        const auto* expected_bfbs = SchemaMeta<TestAction::Goal>::SchemaBfbs();
        const auto expected_bfbs_len =
            SchemaMeta<TestAction::Goal>::SchemaBfbsSize();
        EXPECT_NE(schema_bfbs, nullptr);
        EXPECT_EQ(schema_bfbs_len, expected_bfbs_len);
        EXPECT_TRUE(std::equal(schema_bfbs, schema_bfbs + schema_bfbs_len,
                               expected_bfbs));
        if (opts == nullptr || codec == nullptr || callbacks == nullptr ||
            out == nullptr) {
          return AORTA_ERR_INVALID_ARGUMENT;
        }
        EXPECT_EQ(opts->supports_cancel, 1u);
        EXPECT_EQ(opts->max_concurrency, 2u);
        EXPECT_EQ(opts->result_ttl_ms, 1500u);
        EXPECT_EQ(opts->dedup_ttl_ms, 4500u);
        EXPECT_NE(codec->decode_send_goal_request_goal_id, nullptr);
        EXPECT_NE(codec->decode_cancel_goal_request, nullptr);
        EXPECT_NE(codec->decode_get_result_request, nullptr);
        EXPECT_NE(codec->encode_send_goal_response, nullptr);
        EXPECT_NE(codec->encode_cancel_goal_response, nullptr);
        EXPECT_NE(codec->encode_public_status, nullptr);
        EXPECT_NE(codec->encode_feedback, nullptr);
        EXPECT_NE(codec->encode_get_result_response, nullptr);
        EXPECT_NE(callbacks->execute, nullptr);
        EXPECT_EQ(callbacks->cancel_requested, nullptr);
        EXPECT_NE(callbacks->user_data, nullptr);
        EXPECT_NE(callbacks->user_data_release, nullptr);
        captured_codec = *codec;
        *out = 471;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_action_server_free(471)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(470)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("action_full_server_node");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();

  ActionServerOptions options;
  options.supports_cancel = true;
  options.max_concurrency = 2;
  options.result_ttl = std::chrono::milliseconds(1500);
  options.dedup_ttl = std::chrono::milliseconds(4500);

  auto server_result = ActionServer<TestAction>::Serve(
      node, TestAction::ActionName(), options, [](auto, auto) {});
  ASSERT_TRUE(server_result.ok());
  EXPECT_EQ(server_result.value()->ActionName(), "example/flow_resume_probe");

  ::aorta::action::example::FlowResumeProbeGoalMsg goal;
  goal.source = std::string("codec");
  const auto send_goal_payload = TestAction::EncodeSendGoalRequest(kGoalId, goal);
  aorta_bytes_slice_t send_goal_slice{
      send_goal_payload.data(), send_goal_payload.size()};
  aorta_bytes_view_t send_goal_view{&send_goal_slice, 1, send_goal_payload.size()};
  aorta_string_view_t decoded_goal_id{};
  ASSERT_EQ(captured_codec.decode_send_goal_request_goal_id(
                &send_goal_view, &decoded_goal_id),
            AORTA_OK);
  EXPECT_EQ(CopyStringViewPayload(decoded_goal_id), kGoalId);

  const auto cancel_payload =
      TestAction::EncodeCancelGoalRequest(kGoalId, kGoalSecret);
  aorta_bytes_slice_t cancel_slice{cancel_payload.data(), cancel_payload.size()};
  aorta_bytes_view_t cancel_view{&cancel_slice, 1, cancel_payload.size()};
  aorta_string_view_t decoded_cancel_goal_id{};
  aorta_string_view_t decoded_cancel_goal_secret{};
  ASSERT_EQ(captured_codec.decode_cancel_goal_request(
                &cancel_view, &decoded_cancel_goal_id,
                &decoded_cancel_goal_secret),
            AORTA_OK);
  EXPECT_EQ(CopyStringViewPayload(decoded_cancel_goal_id), kGoalId);
  EXPECT_EQ(CopyStringViewPayload(decoded_cancel_goal_secret), kGoalSecret);

  const auto get_result_payload =
      TestAction::EncodeGetResultRequest(kGoalId, kGoalSecret);
  aorta_bytes_slice_t get_result_slice{get_result_payload.data(),
                                       get_result_payload.size()};
  aorta_bytes_view_t get_result_view{
      &get_result_slice, 1, get_result_payload.size()};
  aorta_string_view_t decoded_result_goal_id{};
  aorta_string_view_t decoded_result_goal_secret{};
  ASSERT_EQ(captured_codec.decode_get_result_request(
                &get_result_view, &decoded_result_goal_id,
                &decoded_result_goal_secret),
            AORTA_OK);
  EXPECT_EQ(CopyStringViewPayload(decoded_result_goal_id), kGoalId);
  EXPECT_EQ(CopyStringViewPayload(decoded_result_goal_secret), kGoalSecret);
}

TEST(ActionTest, SendGoalBuildsRequestAndReturnsGoalHandle) {
  MockAortaFFI mock;

  ON_CALL(mock, aorta_owned_bytes_free(_))
      .WillByDefault(Invoke([](aorta_owned_bytes_t* bytes) {
        FreeOwnedBytesResponse(bytes);
      }));

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 410;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock,
              aorta_action_client_new_with_options(410, StrEq("example/flow_resume_probe"),
                                                   _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*,
                          const aorta_action_client_options_t*,
                          aorta_action_client_t* out) {
        *out = 411;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_action_client_send_goal(411, _, _))
      .WillOnce(Invoke([](aorta_action_client_t, const aorta_bytes_view_t* request,
                          aorta_owned_bytes_t* response) -> aorta_result_t {
        EXPECT_NE(request, nullptr);
        if (request == nullptr) {
          return AORTA_ERR_INTERNAL;
        }
        EXPECT_EQ(request->slice_count, 1u);
        EXPECT_NE(request->slices, nullptr);
        if (request->slice_count != 1u || request->slices == nullptr) {
          return AORTA_ERR_INTERNAL;
        }
        const auto& slice = request->slices[0];
        EXPECT_TRUE(TypedSubscribeTraits<
                    ::aorta::action::example::FlowResumeProbeSendGoalRequest>::Verify(
            slice.data, slice.len));
        const auto* root =
            TypedSubscribeTraits<
                ::aorta::action::example::FlowResumeProbeSendGoalRequest>::GetRoot(
                slice.data);
        EXPECT_NE(root, nullptr);
        if (root == nullptr) {
          return AORTA_ERR_INTERNAL;
        }
        EXPECT_NE(root->goal_id(), nullptr);
        EXPECT_STREQ(root->goal_id()->c_str(), kGoalId);
        EXPECT_NE(root->goal(), nullptr);
        if (root->goal() == nullptr) {
          return AORTA_ERR_INTERNAL;
        }
        EXPECT_NE(root->goal()->source(), nullptr);
        EXPECT_STREQ(root->goal()->source()->c_str(), "cli");
        FillOwnedBytesResponse(response, BuildSendGoalResponsePayload(true));
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_owned_bytes_free(_)).Times(1);
  EXPECT_CALL(mock, aorta_action_client_free(411)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(410)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("action_send_goal_node");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();
  auto client_result =
      node->CreateActionClient<TestAction>(TestAction::ActionName());
  ASSERT_TRUE(client_result.ok());
  auto client = client_result.value();

  ::aorta::action::example::FlowResumeProbeGoalMsg goal;
  goal.source = std::string("cli");
  auto handle_result = client->SendGoalWithId(kGoalId, goal);
  ASSERT_TRUE(handle_result.ok());
  auto handle = std::move(handle_result).value();
  EXPECT_EQ(handle.goal_id(), kGoalId);
  EXPECT_EQ(handle.goal_secret(), kGoalSecret);
  EXPECT_EQ(handle.server_epoch(), kServerEpoch);
}

TEST(ActionTest, ServeCallbackDecodesGoalAndUsesContextMethods) {
  MockAortaFFI mock;

  void (*execute_cb)(const aorta_bytes_view_t*, aorta_action_context_t, void*) =
      nullptr;
  void* execute_user_data = nullptr;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 480;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_action_server_new_with_callbacks(
                        480, StrEq("example/flow_resume_probe"), _, _, _, _, _, _))
      .WillOnce(Invoke([&](aorta_node_t, const char*, const uint8_t*, size_t,
                           const aorta_action_server_options_t*,
                           const aorta_action_server_codec_t*,
                           const aorta_action_server_callbacks_t* callbacks,
                           aorta_action_server_t* out) {
        execute_cb = callbacks->execute;
        execute_user_data = callbacks->user_data;
        *out = 481;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_action_context_is_cancel_requested(900, _))
      .WillOnce(Invoke([](aorta_action_context_t, uint8_t* out_requested) {
        *out_requested = 1;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_action_context_publish_feedback(900, _, StrEq("progress")))
      .WillOnce(Invoke([](aorta_action_context_t,
                          const aorta_bytes_view_t* feedback_data,
                          const char*) -> aorta_result_t {
        const auto payload = CopyBytesViewPayload(feedback_data);
        EXPECT_TRUE(TypedSubscribeTraits<
                    ::aorta::action::example::FlowResumeProbeFeedbackData>::Verify(
            payload.data(), payload.size()));
        const auto* root = TypedSubscribeTraits<
            ::aorta::action::example::FlowResumeProbeFeedbackData>::GetRoot(
            payload.data());
        if (root == nullptr || root->current_node() == nullptr) {
          return AORTA_ERR_INVALID_ARGUMENT;
        }
        EXPECT_STREQ(root->current_node()->c_str(), "planner");
        EXPECT_EQ(root->completed_nodes(), 1u);
        EXPECT_EQ(root->total_nodes(), 3u);
        EXPECT_EQ(root->progress_pct(), 33u);
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_action_context_succeed(900, _))
      .WillOnce(Invoke([](aorta_action_context_t,
                          const aorta_bytes_view_t* result_payload) -> aorta_result_t {
        const auto payload = CopyBytesViewPayload(result_payload);
        EXPECT_TRUE(TypedSubscribeTraits<
                    ::aorta::action::example::FlowResumeProbeResult>::Verify(
            payload.data(), payload.size()));
        const auto* root = TypedSubscribeTraits<
            ::aorta::action::example::FlowResumeProbeResult>::GetRoot(payload.data());
        if (root == nullptr) {
          return AORTA_ERR_INVALID_ARGUMENT;
        }
        EXPECT_EQ(root->duration_ms(), 42u);
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_action_context_free(900)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_action_server_free(481)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(480)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("action_server_callback_node");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();

  bool handler_called = false;
  std::string observed_goal_id;
  std::string observed_source;
  bool cancel_requested = false;

  auto server_result = ActionServer<TestAction>::Serve(
      node, TestAction::ActionName(), ActionServerOptions{},
      [&](RetainedActionGoal<TestAction> goal,
          ActionExecutionContext<TestAction> ctx) {
        handler_called = true;
        ASSERT_TRUE(goal.valid());
        ASSERT_NE(goal.request(), nullptr);
        ASSERT_NE(goal.message(), nullptr);
        observed_goal_id = goal.goal_id();
        ASSERT_NE(goal.message()->source(), nullptr);
        observed_source = goal.message()->source()->str();
        EXPECT_EQ(ctx.goal_id(), kGoalId);
        cancel_requested = ctx.IsCancelRequested();

        ::aorta::action::example::FlowResumeProbeFeedbackDataMsg feedback;
        feedback.current_node = std::string("planner");
        feedback.completed_nodes = 1;
        feedback.total_nodes = 3;
        feedback.progress_pct = 33;
        auto feedback_status = ctx.PublishFeedback(feedback, "progress");
        EXPECT_TRUE(feedback_status.ok());

        ::aorta::action::example::FlowResumeProbeResultMsg result;
        result.duration_ms = 42;
        auto succeed_status = ctx.Succeed(result);
        EXPECT_TRUE(succeed_status.ok());
      });
  ASSERT_TRUE(server_result.ok());
  ASSERT_NE(execute_cb, nullptr);
  ASSERT_NE(execute_user_data, nullptr);

  ::aorta::action::example::FlowResumeProbeGoalMsg goal;
  goal.source = std::string("scheduler");
  const auto payload = TestAction::EncodeSendGoalRequest(kGoalId, goal);
  aorta_bytes_slice_t slice{payload.data(), payload.size()};
  aorta_bytes_view_t view{&slice, 1, payload.size()};

  execute_cb(&view, 900, execute_user_data);

  EXPECT_TRUE(handler_called);
  EXPECT_EQ(observed_goal_id, kGoalId);
  EXPECT_EQ(observed_source, "scheduler");
  EXPECT_TRUE(cancel_requested);
}

TEST(ActionReentrancyDeathTest, DestroyingOwnServerFromExecuteAborts) {
  EXPECT_DEATH(
      ([] {
        setenv("AORTA_REENTRANT_ASSERT", "2", 1);
        SetMockReentrantContext(false);
        MockAortaFFI mock;
        void (*execute_cb)(const aorta_bytes_view_t*, aorta_action_context_t,
                           void*) = nullptr;
        void* execute_user_data = nullptr;
        EXPECT_CALL(mock, aorta_node_new(_, _, _))
            .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
              *out = 700;
              return AORTA_OK;
            }));
        EXPECT_CALL(mock, aorta_action_server_new_with_callbacks(
                              700, _, _, _, _, _, _, _))
            .WillOnce(Invoke(
                [&](aorta_node_t, const char*, const uint8_t*, size_t,
                    const aorta_action_server_options_t*,
                    const aorta_action_server_codec_t*,
                    const aorta_action_server_callbacks_t* callbacks,
                    aorta_action_server_t* out) {
                  execute_cb = callbacks->execute;
                  execute_user_data = callbacks->user_data;
                  *out = 701;
                  return AORTA_OK;
                }));
        EXPECT_CALL(mock, aorta_action_server_free(701))
            .WillOnce(Return(AORTA_OK));

        auto node = Node::Create("action_own_destroy_node").value();
        std::shared_ptr<ActionServer<TestAction>> server;
        server = ActionServer<TestAction>::Serve(
                     node, TestAction::ActionName(), ActionServerOptions{},
                     [&](auto, auto) { server.reset(); })
                     .value();
        ::aorta::action::example::FlowResumeProbeGoalMsg goal;
        goal.source = std::string("own-destroy");
        const auto payload =
            TestAction::EncodeSendGoalRequest(kGoalId, goal);
        aorta_bytes_slice_t slice{payload.data(), payload.size()};
        aorta_bytes_view_t view{&slice, 1, payload.size()};
        execute_cb(&view, 901, execute_user_data);
      }()),
      "ActionServer::~ActionServer");
}

TEST(ActionTest, ExecuteHandlerMayDestroyDifferentServer) {
  setenv("AORTA_REENTRANT_ASSERT", "2", 1);
  SetMockReentrantContext(false);
  MockAortaFFI mock;
  void (*execute_a)(const aorta_bytes_view_t*, aorta_action_context_t, void*) =
      nullptr;
  void* execute_a_data = nullptr;
  int server_count = 0;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 710;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_action_server_new_with_callbacks(
                        710, _, _, _, _, _, _, _))
      .Times(2)
      .WillRepeatedly(Invoke(
          [&](aorta_node_t, const char*, const uint8_t*, size_t,
              const aorta_action_server_options_t*,
              const aorta_action_server_codec_t*,
              const aorta_action_server_callbacks_t* callbacks,
              aorta_action_server_t* out) {
            ++server_count;
            *out = 710 + server_count;
            if (server_count == 2) {
              execute_a = callbacks->execute;
              execute_a_data = callbacks->user_data;
            }
            return AORTA_OK;
          }));
  EXPECT_CALL(mock, aorta_action_server_free(711)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_action_server_free(712)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_action_context_free(902)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(710)).WillOnce(Return(AORTA_OK));

  auto node = Node::Create("action_other_destroy_node").value();
  auto server_b = ActionServer<TestAction>::Serve(
                      node, TestAction::ActionName(), ActionServerOptions{},
                      [](auto, auto) {})
                      .value();
  auto server_a = ActionServer<TestAction>::Serve(
                      node, TestAction::ActionName(), ActionServerOptions{},
                      [&](auto, auto) { server_b.reset(); })
                      .value();
  ASSERT_NE(execute_a, nullptr);

  ::aorta::action::example::FlowResumeProbeGoalMsg goal;
  goal.source = std::string("other-destroy");
  const auto payload = TestAction::EncodeSendGoalRequest(kGoalId, goal);
  aorta_bytes_slice_t slice{payload.data(), payload.size()};
  aorta_bytes_view_t view{&slice, 1, payload.size()};
  execute_a(&view, 902, execute_a_data);

  EXPECT_EQ(server_b, nullptr);
  server_a.reset();
}

TEST(ActionTest, InfoParsesActionInfo) {
  MockAortaFFI mock;

  ON_CALL(mock, aorta_owned_bytes_free(_))
      .WillByDefault(Invoke([](aorta_owned_bytes_t* bytes) {
        FreeOwnedBytesResponse(bytes);
      }));

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 420;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock,
              aorta_action_client_new_with_options(420, StrEq("example/flow_resume_probe"),
                                                   _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*,
                          const aorta_action_client_options_t*,
                          aorta_action_client_t* out) {
        *out = 421;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_action_client_info(421, _))
      .WillOnce(Invoke([](aorta_action_client_t, aorta_owned_bytes_t* response) {
        FillOwnedBytesResponse(response, BuildActionInfoPayload());
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_owned_bytes_free(_)).Times(1);
  EXPECT_CALL(mock, aorta_action_client_free(421)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(420)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("action_info_node");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();
  auto client_result =
      node->CreateActionClient<TestAction>(TestAction::ActionName());
  ASSERT_TRUE(client_result.ok());
  auto client = client_result.value();

  auto info_result = client->Info();
  ASSERT_TRUE(info_result.ok());
  const auto& info = info_result.value();
  EXPECT_EQ(info.action_name, "example/flow_resume_probe");
  EXPECT_EQ(info.server_node_name, "planner_server");
  EXPECT_EQ(info.server_epoch, kServerEpoch);
  EXPECT_EQ(info.max_concurrency, 4u);
}

TEST(ActionTest, QueryStatusParsesPublicStatus) {
  MockAortaFFI mock;

  ON_CALL(mock, aorta_owned_bytes_free(_))
      .WillByDefault(Invoke([](aorta_owned_bytes_t* bytes) {
        FreeOwnedBytesResponse(bytes);
      }));

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 430;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock,
              aorta_action_client_new_with_options(430, StrEq("example/flow_resume_probe"),
                                                   _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*,
                          const aorta_action_client_options_t*,
                          aorta_action_client_t* out) {
        *out = 431;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_action_client_query_status(431, StrEq(kGoalId), _))
      .WillOnce(Invoke([](aorta_action_client_t, const char*,
                          aorta_owned_bytes_t* response) {
        FillOwnedBytesResponse(response, BuildPublicStatusPayload());
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_owned_bytes_free(_)).Times(1);
  EXPECT_CALL(mock, aorta_action_client_free(431)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(430)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("action_status_node");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();
  auto client_result =
      node->CreateActionClient<TestAction>(TestAction::ActionName());
  ASSERT_TRUE(client_result.ok());
  auto client = client_result.value();

  auto status_result = client->QueryStatus(kGoalId);
  ASSERT_TRUE(status_result.ok());
  ASSERT_TRUE(status_result->has_value());
  EXPECT_EQ(status_result->value().goal_id, kGoalId);
  EXPECT_EQ(status_result->value().status, ActionStatus::kExecuting);
  EXPECT_EQ(status_result->value().code, ActionReplyCode::kOk);
  EXPECT_EQ(status_result->value().last_feedback_seq, 7u);
}

TEST(ActionTest, CancelUsesPersistedSecretAndReturnsAcceptedResponse) {
  MockAortaFFI mock;

  ON_CALL(mock, aorta_owned_bytes_free(_))
      .WillByDefault(Invoke([](aorta_owned_bytes_t* bytes) {
        FreeOwnedBytesResponse(bytes);
      }));

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 435;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock,
              aorta_action_client_new_with_options(435, StrEq("example/flow_resume_probe"),
                                                   _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*,
                          const aorta_action_client_options_t*,
                          aorta_action_client_t* out) {
        *out = 436;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_action_client_cancel(436, _, _))
      .WillOnce(Invoke([](aorta_action_client_t, const aorta_bytes_view_t* request,
                          aorta_owned_bytes_t* response) -> aorta_result_t {
        EXPECT_NE(request, nullptr);
        if (request == nullptr || request->slice_count != 1u) {
          return AORTA_ERR_INTERNAL;
        }
        const auto& slice = request->slices[0];
        EXPECT_TRUE(TypedSubscribeTraits<
                    ::aorta::action::example::FlowResumeProbeCancelGoalRequest>::Verify(
            slice.data, slice.len));
        const auto* root =
            TypedSubscribeTraits<
                ::aorta::action::example::FlowResumeProbeCancelGoalRequest>::GetRoot(
                slice.data);
        EXPECT_NE(root, nullptr);
        if (root == nullptr || root->goal_id() == nullptr ||
            root->goal_secret() == nullptr) {
          return AORTA_ERR_INTERNAL;
        }
        EXPECT_STREQ(root->goal_id()->c_str(), kGoalId);
        EXPECT_STREQ(root->goal_secret()->c_str(), kGoalSecret);
        FillOwnedBytesResponse(response, BuildCancelGoalResponsePayload(true));
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_owned_bytes_free(_)).Times(1);
  EXPECT_CALL(mock, aorta_action_client_free(436)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(435)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("action_cancel_node");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();
  auto client_result =
      node->CreateActionClient<TestAction>(TestAction::ActionName());
  ASSERT_TRUE(client_result.ok());
  auto client = client_result.value();

  auto handle_result =
      GoalHandle<TestAction>::FromPersisted(client, kGoalId, kGoalSecret);
  ASSERT_TRUE(handle_result.ok());
  auto handle = std::move(handle_result).value();

  auto cancel_result = handle.Cancel();
  ASSERT_TRUE(cancel_result.ok());
  EXPECT_TRUE(cancel_result->accepted);
  EXPECT_EQ(cancel_result->goal_id, kGoalId);
  EXPECT_EQ(cancel_result->code, ActionReplyCode::kOk);
  EXPECT_EQ(cancel_result->status, ActionStatus::kCanceling);
}

TEST(ActionTest, GetResultReturnsTypedResponse) {
  MockAortaFFI mock;

  ON_CALL(mock, aorta_owned_bytes_free(_))
      .WillByDefault(Invoke([](aorta_owned_bytes_t* bytes) {
        FreeOwnedBytesResponse(bytes);
      }));

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 440;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock,
              aorta_action_client_new_with_options(440, StrEq("example/flow_resume_probe"),
                                                   _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*,
                          const aorta_action_client_options_t*,
                          aorta_action_client_t* out) {
        *out = 441;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_action_client_get_result(441, _, _))
      .WillOnce(Invoke([](aorta_action_client_t, const aorta_bytes_view_t* request,
                          aorta_owned_bytes_t* response) -> aorta_result_t {
        EXPECT_NE(request, nullptr);
        if (request == nullptr) {
          return AORTA_ERR_INTERNAL;
        }
        EXPECT_EQ(request->slice_count, 1u);
        if (request->slice_count != 1u) {
          return AORTA_ERR_INTERNAL;
        }
        const auto& slice = request->slices[0];
        EXPECT_TRUE(TypedSubscribeTraits<
                    ::aorta::action::example::FlowResumeProbeGetResultRequest>::Verify(
            slice.data, slice.len));
        const auto* root =
            TypedSubscribeTraits<
                ::aorta::action::example::FlowResumeProbeGetResultRequest>::GetRoot(
                slice.data);
        EXPECT_NE(root, nullptr);
        if (root == nullptr) {
          return AORTA_ERR_INTERNAL;
        }
        EXPECT_NE(root->goal_id(), nullptr);
        EXPECT_NE(root->goal_secret(), nullptr);
        if (root->goal_id() == nullptr || root->goal_secret() == nullptr) {
          return AORTA_ERR_INTERNAL;
        }
        EXPECT_STREQ(root->goal_id()->c_str(), kGoalId);
        EXPECT_STREQ(root->goal_secret()->c_str(), kGoalSecret);
        FillOwnedBytesResponse(
            response,
            BuildGetResultResponsePayload(ActionStatus::kSucceeded, true));
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_owned_bytes_free(_)).Times(1);
  EXPECT_CALL(mock, aorta_action_client_free(441)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(440)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("action_result_node");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();
  auto client_result =
      node->CreateActionClient<TestAction>(TestAction::ActionName());
  ASSERT_TRUE(client_result.ok());
  auto client = client_result.value();

  auto handle_result = GoalHandle<TestAction>::FromPersisted(client, kGoalId,
                                                             kGoalSecret);
  ASSERT_TRUE(handle_result.ok());
  auto handle = std::move(handle_result).value();
  auto result = handle.GetResult();
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(result->goal_id, kGoalId);
  EXPECT_EQ(result->status, ActionStatus::kSucceeded);
  EXPECT_EQ(result->feedback_snapshot.current_node.value_or(""), "node_c");
  ASSERT_TRUE(result->result.has_value());
  EXPECT_EQ(result->result->duration_ms.value_or(0), 1234u);
}

TEST(ActionTest, SetFeedbackCallbackAllSubscribesAndDecodesFeedback) {
  MockAortaFFI mock;

  aorta_on_msg_cb ffi_cb = nullptr;
  void* ffi_user_data = nullptr;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 450;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock,
              aorta_action_client_new_with_options(450, StrEq("example/flow_resume_probe"),
                                                   _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*,
                          const aorta_action_client_options_t*,
                          aorta_action_client_t* out) {
        *out = 451;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_action_client_subscribe_feedback(451, _, _, _))
      .WillOnce(Invoke([&](aorta_action_client_t, aorta_on_msg_cb cb,
                           void* user_data,
                           aorta_action_feedback_sub_t* out_sub) {
        ffi_cb = cb;
        ffi_user_data = user_data;
        *out_sub = 452;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_action_feedback_sub_free(452))
      .WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_action_client_free(451)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(450)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("action_feedback_node");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();
  auto client_result =
      node->CreateActionClient<TestAction>(TestAction::ActionName());
  ASSERT_TRUE(client_result.ok());
  auto client = client_result.value();

  bool called = false;
  auto status = client->SetFeedbackCallbackAll(
      [&](const TestAction::Feedback& feedback) {
        called = true;
        ASSERT_NE(feedback.goal_id(), nullptr);
        EXPECT_STREQ(feedback.goal_id()->c_str(), kGoalId);
        ASSERT_NE(feedback.data(), nullptr);
        ASSERT_NE(feedback.data()->current_node(), nullptr);
        EXPECT_STREQ(feedback.data()->current_node()->c_str(), "step-1");
      });
  ASSERT_TRUE(status.ok());
  ASSERT_NE(ffi_cb, nullptr);
  ASSERT_NE(ffi_user_data, nullptr);

  const auto payload = BuildFeedbackPayload(kGoalId, "step-1");
  aorta_bytes_slice_t slice{payload.data(), payload.size()};
  aorta_bytes_view_t view{&slice, 1, payload.size()};
  ffi_cb(&view, nullptr, nullptr, ffi_user_data);

  EXPECT_TRUE(called);
}

TEST(ActionTest, GoalHandleFeedbackCallbackFiltersByGoalId) {
  MockAortaFFI mock;

  aorta_on_msg_cb ffi_cb = nullptr;
  void* ffi_user_data = nullptr;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 460;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock,
              aorta_action_client_new_with_options(460, StrEq("example/flow_resume_probe"),
                                                   _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*,
                          const aorta_action_client_options_t*,
                          aorta_action_client_t* out) {
        *out = 461;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_action_client_subscribe_feedback(461, _, _, _))
      .WillOnce(Invoke([&](aorta_action_client_t, aorta_on_msg_cb cb,
                           void* user_data,
                           aorta_action_feedback_sub_t* out_sub) {
        ffi_cb = cb;
        ffi_user_data = user_data;
        *out_sub = 462;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_action_feedback_sub_free(462))
      .WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_action_client_free(461)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(460)).WillOnce(Return(AORTA_OK));

  auto node_result = Node::Create("action_goal_feedback_node");
  ASSERT_TRUE(node_result.ok());
  auto node = node_result.value();
  auto client_result =
      node->CreateActionClient<TestAction>(TestAction::ActionName());
  ASSERT_TRUE(client_result.ok());
  auto client = client_result.value();

  auto handle_result =
      GoalHandle<TestAction>::FromPersisted(client, kGoalId, kGoalSecret);
  ASSERT_TRUE(handle_result.ok());
  auto handle = std::move(handle_result).value();

  int callback_count = 0;
  auto status = handle.SetFeedbackCallback(
      [&](const TestAction::Feedback& feedback) {
        ++callback_count;
        ASSERT_NE(feedback.goal_id(), nullptr);
        EXPECT_STREQ(feedback.goal_id()->c_str(), kGoalId);
      });
  ASSERT_TRUE(status.ok());
  ASSERT_NE(ffi_cb, nullptr);
  ASSERT_NE(ffi_user_data, nullptr);

  const auto other_payload =
      BuildFeedbackPayload("550e8400-e29b-41d4-a716-446655440099", "step-x");
  aorta_bytes_slice_t other_slice{other_payload.data(), other_payload.size()};
  aorta_bytes_view_t other_view{&other_slice, 1, other_payload.size()};
  ffi_cb(&other_view, nullptr, nullptr, ffi_user_data);

  const auto matching_payload = BuildFeedbackPayload(kGoalId, "step-2");
  aorta_bytes_slice_t matching_slice{matching_payload.data(),
                                     matching_payload.size()};
  aorta_bytes_view_t matching_view{&matching_slice, 1,
                                   matching_payload.size()};
  ffi_cb(&matching_view, nullptr, nullptr, ffi_user_data);

  EXPECT_EQ(callback_count, 1);
}

TEST(ActionTest, WaitResultReturnsTimeoutWhenGoalNeverTerminates) {
  // WaitResult now blocks on the event-driven watcher (subscribe_result). The
  // watcher times out → it delivers a TIMEOUT wait failure, which WaitResult
  // surfaces (no status-subscriber + condvar on the node executor any more).
  MockAortaFFI mock;
  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 445;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock,
              aorta_action_client_new_with_options(445, StrEq("example/flow_resume_probe"),
                                                   _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*,
                          const aorta_action_client_options_t*,
                          aorta_action_client_t* out) {
        *out = 446;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock,
              aorta_action_client_subscribe_result(446, StrEq(kGoalId), _, _,
                                                   uint64_t{200}, _, _, _, _))
      .WillOnce(Invoke([](aorta_action_client_t, const char*, const uint8_t*,
                          uintptr_t, uint64_t, aorta_action_status_decode_fn,
                          aorta_action_result_cb cb, void* user_data,
                          aorta_action_result_sub_t* out_sub) {
        aorta_action_result_view_t ev{};
        ev.error = AORTA_ERR_TIMEOUT;
        cb(&ev, user_data);
        *out_sub = 99;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_action_result_sub_free(99)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_action_client_free(446)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(445)).WillOnce(Return(AORTA_OK));

  auto node = Node::Create("action_wait_timeout_node").value();
  auto client = node->CreateActionClient<TestAction>(TestAction::ActionName()).value();
  auto handle =
      GoalHandle<TestAction>::FromPersisted(client, kGoalId, kGoalSecret).value();

  auto result = handle.WaitResult(std::chrono::milliseconds(200));
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), AORTA_ERR_TIMEOUT);
}

TEST(ActionTest, WaitResultDeliversTerminalResult) {
  // WaitResult blocks on the event-driven watcher (subscribe_result); the
  // watcher delivers the terminal GetResultResponse in-band, which WaitResult
  // decodes + validates.
  MockAortaFFI mock;
  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 450;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock,
              aorta_action_client_new_with_options(450, StrEq("example/flow_resume_probe"),
                                                   _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*,
                          const aorta_action_client_options_t*,
                          aorta_action_client_t* out) {
        *out = 451;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_action_client_subscribe_result(451, StrEq(kGoalId), _,
                                                         _, _, _, _, _, _))
      .WillOnce(Invoke([](aorta_action_client_t, const char*, const uint8_t*,
                          uintptr_t, uint64_t, aorta_action_status_decode_fn,
                          aorta_action_result_cb cb, void* user_data,
                          aorta_action_result_sub_t* out_sub) {
        auto payload =
            BuildGetResultResponsePayload(ActionStatus::kSucceeded, true);
        aorta_bytes_slice_t slice{payload.data(), payload.size()};
        aorta_action_result_view_t ev{};
        ev.error = AORTA_OK;
        ev.result_payload = aorta_bytes_view_t{&slice, 1, payload.size()};
        cb(&ev, user_data);  // deliver synchronously (payload alive here)
        *out_sub = 88;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_action_result_sub_free(88)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_action_client_free(451)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(450)).WillOnce(Return(AORTA_OK));

  auto node = Node::Create("action_wait_node").value();
  auto client = node->CreateActionClient<TestAction>(TestAction::ActionName()).value();
  auto handle =
      GoalHandle<TestAction>::FromPersisted(client, kGoalId, kGoalSecret).value();

  auto result = handle.WaitResult(std::chrono::milliseconds(500));
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(result->status, ActionStatus::kSucceeded);
  ASSERT_TRUE(result->result.has_value());
  EXPECT_EQ(result->result->duration_ms.value_or(0), 1234u);
}

::aorta::action::example::FlowResumeProbeGoalMsg MakeCliGoal() {
  ::aorta::action::example::FlowResumeProbeGoalMsg goal;
  goal.source = std::string("cli");
  return goal;
}

std::vector<MockTraceStageRecord> StagesUnder(const MockTraceState& trace,
                                              trace::StageId stage) {
  std::vector<MockTraceStageRecord> out;
  for (const auto& rec : trace.stages_under) {
    if (rec.stage_id == static_cast<uint8_t>(stage)) {
      out.push_back(rec);
    }
  }
  return out;
}

TEST(ActionTest, SetResultCallbackRecordsGoalLifetimeOnTerminalResult) {
  // The goal-lifetime span must close on EVERY result-delivery path. Here the
  // terminal result arrives through SetResultCallback (the event-driven
  // watcher), not a poll or a blocking wait — the span must still be emitted,
  // anchored at the SendGoal instant and joined under the goal_sent span. A
  // later terminal GetResult() poll must NOT emit a second one (shared
  // one-shot guard across delivery paths).
  MockTraceState trace;
  trace.now_ns = 1111;

  MockAortaFFI mock;
  ON_CALL(mock, aorta_owned_bytes_free(_))
      .WillByDefault(Invoke([](aorta_owned_bytes_t* bytes) {
        FreeOwnedBytesResponse(bytes);
      }));

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 460;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock,
              aorta_action_client_new_with_options(460, StrEq("example/flow_resume_probe"),
                                                   _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*,
                          const aorta_action_client_options_t*,
                          aorta_action_client_t* out) {
        *out = 461;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_action_client_send_goal(461, _, _))
      .WillOnce(Invoke([](aorta_action_client_t, const aorta_bytes_view_t*,
                          aorta_owned_bytes_t* response) {
        FillOwnedBytesResponse(response, BuildSendGoalResponsePayload(true));
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_action_client_subscribe_result(461, StrEq(kGoalId), _,
                                                         _, _, _, _, _, _))
      .WillOnce(Invoke([](aorta_action_client_t, const char*, const uint8_t*,
                          uintptr_t, uint64_t, aorta_action_status_decode_fn,
                          aorta_action_result_cb cb, void* user_data,
                          aorta_action_result_sub_t* out_sub) {
        auto payload =
            BuildGetResultResponsePayload(ActionStatus::kSucceeded, true);
        aorta_bytes_slice_t slice{payload.data(), payload.size()};
        aorta_action_result_view_t ev{};
        ev.error = AORTA_OK;
        ev.result_payload = aorta_bytes_view_t{&slice, 1, payload.size()};
        cb(&ev, user_data);  // deliver synchronously (payload alive here)
        *out_sub = 77;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_action_client_get_result(461, _, _))
      .WillOnce(Invoke([](aorta_action_client_t, const aorta_bytes_view_t*,
                          aorta_owned_bytes_t* response) {
        FillOwnedBytesResponse(
            response,
            BuildGetResultResponsePayload(ActionStatus::kSucceeded, true));
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_owned_bytes_free(_)).Times(2);
  EXPECT_CALL(mock, aorta_action_result_sub_free(77)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_action_client_free(461)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(460)).WillOnce(Return(AORTA_OK));

  {
    auto node = Node::Create("action_cb_lifetime_node").value();
    auto client =
        node->CreateActionClient<TestAction>(TestAction::ActionName()).value();
    auto handle = client->SendGoalWithId(kGoalId, MakeCliGoal()).value();

    std::atomic<bool> delivered{false};
    ASSERT_TRUE(handle
                    .SetResultCallback(
                        [&delivered](
                            StatusOr<ActionGetResultResponse<TestAction>> result) {
                          ASSERT_TRUE(result.ok());
                          EXPECT_EQ(result->status, ActionStatus::kSucceeded);
                          delivered.store(true);
                        })
                    .ok());
    ASSERT_TRUE(delivered.load());

    auto lifetimes = StagesUnder(trace, trace::StageId::kActionGoalLifetime);
    ASSERT_EQ(lifetimes.size(), 1u);
    EXPECT_EQ(lifetimes[0].name, "example/flow_resume_probe");
    EXPECT_EQ(lifetimes[0].start_mono_ns, 1111u);  // the SendGoal instant
    EXPECT_TRUE(lifetimes[0].has_trace_id);
    EXPECT_EQ(lifetimes[0].parent_span_id, trace.goal_span_id);

    // A repeat terminal observation via the polling path stays a no-op.
    ASSERT_TRUE(handle.GetResult().ok());
    EXPECT_EQ(StagesUnder(trace, trace::StageId::kActionGoalLifetime).size(),
              1u);
  }
}

TEST(ActionTest, WaitResultRecordsGoalLifetimeOnTerminalResult) {
  // WaitResult consumes the same watcher delivery path — a successful blocking
  // wait must close the goal-lifetime span exactly once, anchored at SendGoal.
  MockTraceState trace;
  trace.now_ns = 2222;

  MockAortaFFI mock;
  ON_CALL(mock, aorta_owned_bytes_free(_))
      .WillByDefault(Invoke([](aorta_owned_bytes_t* bytes) {
        FreeOwnedBytesResponse(bytes);
      }));

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 470;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock,
              aorta_action_client_new_with_options(470, StrEq("example/flow_resume_probe"),
                                                   _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*,
                          const aorta_action_client_options_t*,
                          aorta_action_client_t* out) {
        *out = 471;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_action_client_send_goal(471, _, _))
      .WillOnce(Invoke([](aorta_action_client_t, const aorta_bytes_view_t*,
                          aorta_owned_bytes_t* response) {
        FillOwnedBytesResponse(response, BuildSendGoalResponsePayload(true));
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_action_client_subscribe_result(471, StrEq(kGoalId), _,
                                                         _, _, _, _, _, _))
      .WillOnce(Invoke([](aorta_action_client_t, const char*, const uint8_t*,
                          uintptr_t, uint64_t, aorta_action_status_decode_fn,
                          aorta_action_result_cb cb, void* user_data,
                          aorta_action_result_sub_t* out_sub) {
        auto payload =
            BuildGetResultResponsePayload(ActionStatus::kSucceeded, true);
        aorta_bytes_slice_t slice{payload.data(), payload.size()};
        aorta_action_result_view_t ev{};
        ev.error = AORTA_OK;
        ev.result_payload = aorta_bytes_view_t{&slice, 1, payload.size()};
        cb(&ev, user_data);  // deliver synchronously (payload alive here)
        *out_sub = 78;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_owned_bytes_free(_)).Times(1);
  EXPECT_CALL(mock, aorta_action_result_sub_free(78)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_action_client_free(471)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(470)).WillOnce(Return(AORTA_OK));

  auto node = Node::Create("action_wait_lifetime_node").value();
  auto client =
      node->CreateActionClient<TestAction>(TestAction::ActionName()).value();
  auto handle = client->SendGoalWithId(kGoalId, MakeCliGoal()).value();

  auto result = handle.WaitResult(std::chrono::milliseconds(500));
  ASSERT_TRUE(result.ok());

  auto lifetimes = StagesUnder(trace, trace::StageId::kActionGoalLifetime);
  ASSERT_EQ(lifetimes.size(), 1u);
  EXPECT_EQ(lifetimes[0].start_mono_ns, 2222u);  // the SendGoal instant
  EXPECT_EQ(lifetimes[0].parent_span_id, trace.goal_span_id);
}


// Parity: a goal-scoped stream opened on the CLIENT (no GoalHandle
// yet, pre-generated goal_id) ingests feedback delivered before any handle
// exists, filters other goals, and ends itself (Recv ->
// nullopt, no consumer timer) once WaitResult observes the terminal result.
TEST(ActionTest, FeedbackStreamForReceivesEarlyFeedbackAndEndsOnTerminal) {
  MockAortaFFI mock;

  aorta_on_msg_cb ffi_cb = nullptr;
  void* ffi_user_data = nullptr;

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 470;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock,
              aorta_action_client_new_with_options(470, StrEq("example/flow_resume_probe"),
                                                   _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*,
                          const aorta_action_client_options_t*,
                          aorta_action_client_t* out) {
        *out = 471;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_action_client_subscribe_feedback2(471, _, _, _, _, _))
      .WillOnce(Invoke([&](aorta_action_client_t, uint32_t, uint8_t,
                           aorta_on_msg_cb cb, void* user_data,
                           aorta_action_feedback_sub_t* out_sub) {
        ffi_cb = cb;
        ffi_user_data = user_data;
        *out_sub = 472;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_action_client_subscribe_result(471, StrEq(kGoalId), _,
                                                         _, _, _, _, _, _))
      .WillOnce(Invoke([](aorta_action_client_t, const char*, const uint8_t*,
                          uintptr_t, uint64_t, aorta_action_status_decode_fn,
                          aorta_action_result_cb cb, void* user_data,
                          aorta_action_result_sub_t* out_sub) {
        auto payload =
            BuildGetResultResponsePayload(ActionStatus::kSucceeded, true);
        aorta_bytes_slice_t slice{payload.data(), payload.size()};
        aorta_action_result_view_t ev{};
        ev.error = AORTA_OK;
        ev.result_payload = aorta_bytes_view_t{&slice, 1, payload.size()};
        cb(&ev, user_data);  // deliver synchronously (payload alive here)
        *out_sub = 473;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_action_result_sub_free(473)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_action_feedback_sub2_free(472))
      .WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_action_client_free(471)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(470)).WillOnce(Return(AORTA_OK));

  auto node = Node::Create("action_stream_for_node").value();
  auto client =
      node->CreateActionClient<TestAction>(TestAction::ActionName()).value();

  // Subscribe BEFORE any goal handle exists (the subscribe-first ordering).
  auto stream_or = client->OpenFeedbackStreamFor(kGoalId);
  ASSERT_TRUE(stream_or.ok());
  auto stream = std::move(stream_or).value();
  ASSERT_NE(ffi_cb, nullptr);

  // Early feedback (would race SendGoal in the old API shape) + another
  // goal's sample that must be filtered out.
  const auto early = BuildFeedbackPayload(kGoalId, "early");
  aorta_bytes_slice_t early_slice{early.data(), early.size()};
  aorta_bytes_view_t early_view{&early_slice, 1, early.size()};
  ffi_cb(&early_view, nullptr, nullptr, ffi_user_data);

  const auto other =
      BuildFeedbackPayload("550e8400-e29b-41d4-a716-446655440099", "other");
  aorta_bytes_slice_t other_slice{other.data(), other.size()};
  aorta_bytes_view_t other_view{&other_slice, 1, other.size()};
  ffi_cb(&other_view, nullptr, nullptr, ffi_user_data);

  auto handle =
      GoalHandle<TestAction>::FromPersisted(client, kGoalId, kGoalSecret).value();
  auto result = handle.WaitResult(std::chrono::milliseconds(500));
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(result->status, ActionStatus::kSucceeded);

  // Drain-then-end: the early sample is delivered, then Recv reports EOS
  // instead of blocking forever (no consumer-side timer needed).
  auto sample = stream.Recv();
  ASSERT_TRUE(sample.has_value());
  ASSERT_TRUE(sample->valid());
  ASSERT_NE(sample->message()->data(), nullptr);
  EXPECT_STREQ(sample->message()->data()->current_node()->c_str(), "early");
  EXPECT_FALSE(stream.Recv().has_value());

  // Post-terminal deliveries are dropped by the closed ingest.
  ffi_cb(&early_view, nullptr, nullptr, ffi_user_data);
  EXPECT_FALSE(stream.TryRecv().has_value());
}

// Same end-on-terminal parity for the delegated GoalHandle::OpenFeedbackStream and the
// polling GetResult path: a terminal GetResult observation ends the goal's
// stream even when it never buffered a sample.
TEST(ActionTest, GoalHandleFeedbackStreamEndsOnTerminalGetResult) {
  MockAortaFFI mock;

  ON_CALL(mock, aorta_owned_bytes_free(_))
      .WillByDefault(Invoke([](aorta_owned_bytes_t* bytes) {
        FreeOwnedBytesResponse(bytes);
      }));

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 480;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock,
              aorta_action_client_new_with_options(480, StrEq("example/flow_resume_probe"),
                                                   _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*,
                          const aorta_action_client_options_t*,
                          aorta_action_client_t* out) {
        *out = 481;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_action_client_subscribe_feedback2(481, _, _, _, _, _))
      .WillOnce(Invoke([](aorta_action_client_t, uint32_t, uint8_t,
                          aorta_on_msg_cb, void*,
                          aorta_action_feedback_sub_t* out_sub) {
        *out_sub = 482;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_action_client_get_result(481, _, _))
      .WillOnce(Invoke([](aorta_action_client_t, const aorta_bytes_view_t*,
                          aorta_owned_bytes_t* response) -> aorta_result_t {
        FillOwnedBytesResponse(
            response,
            BuildGetResultResponsePayload(ActionStatus::kSucceeded, true));
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_owned_bytes_free(_)).Times(1);
  EXPECT_CALL(mock, aorta_action_feedback_sub2_free(482))
      .WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_action_client_free(481)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(480)).WillOnce(Return(AORTA_OK));

  auto node = Node::Create("action_stream_eos_node").value();
  auto client =
      node->CreateActionClient<TestAction>(TestAction::ActionName()).value();
  auto handle =
      GoalHandle<TestAction>::FromPersisted(client, kGoalId, kGoalSecret).value();

  auto stream_or = handle.OpenFeedbackStream();
  ASSERT_TRUE(stream_or.ok());
  auto stream = std::move(stream_or).value();

  auto result = handle.GetResult();
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(result->status, ActionStatus::kSucceeded);

  // No buffered samples: Recv reports EOS immediately instead of parking.
  EXPECT_FALSE(stream.Recv().has_value());
}


// Terminal-before-open ordering: a goal-scoped
// stream opened AFTER the client already observed the goal's terminal result
// must be born closed (Recv -> nullopt immediately), not park forever.
TEST(ActionTest, FeedbackStreamOpenedAfterTerminalIsBornClosed) {
  MockAortaFFI mock;

  ON_CALL(mock, aorta_owned_bytes_free(_))
      .WillByDefault(Invoke([](aorta_owned_bytes_t* bytes) {
        FreeOwnedBytesResponse(bytes);
      }));

  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 490;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock,
              aorta_action_client_new_with_options(490, StrEq("example/flow_resume_probe"),
                                                   _, _))
      .WillOnce(Invoke([](aorta_node_t, const char*,
                          const aorta_action_client_options_t*,
                          aorta_action_client_t* out) {
        *out = 491;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_action_client_get_result(491, _, _))
      .WillOnce(Invoke([](aorta_action_client_t, const aorta_bytes_view_t*,
                          aorta_owned_bytes_t* response) -> aorta_result_t {
        FillOwnedBytesResponse(
            response,
            BuildGetResultResponsePayload(ActionStatus::kSucceeded, true));
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_owned_bytes_free(_)).Times(1);
  EXPECT_CALL(mock, aorta_action_client_subscribe_feedback2(491, _, _, _, _, _))
      .WillOnce(Invoke([](aorta_action_client_t, uint32_t, uint8_t,
                          aorta_on_msg_cb, void*,
                          aorta_action_feedback_sub_t* out_sub) {
        *out_sub = 492;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_action_feedback_sub2_free(492))
      .WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_action_client_free(491)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(490)).WillOnce(Return(AORTA_OK));

  auto node = Node::Create("action_born_closed_node").value();
  auto client =
      node->CreateActionClient<TestAction>(TestAction::ActionName()).value();
  auto handle =
      GoalHandle<TestAction>::FromPersisted(client, kGoalId, kGoalSecret).value();

  // Observe terminal FIRST...
  auto result = handle.GetResult();
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(result->status, ActionStatus::kSucceeded);

  // ...then open the goal-scoped stream: born closed, Recv never parks.
  auto stream_or = client->OpenFeedbackStreamFor(kGoalId);
  ASSERT_TRUE(stream_or.ok());
  auto stream = std::move(stream_or).value();
  EXPECT_FALSE(stream.Recv().has_value());
}

TEST(ActionTest, ServeWithCancelHandlerDeliversGoalId) {
  SetMockReentrantContext(false);
  MockAortaFFI mock;
  aorta_action_server_callbacks_t captured{};
  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 720;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_action_server_new_with_callbacks(
                        720, _, _, _, _, _, _, _))
      .WillOnce(Invoke([&](aorta_node_t, const char*, const uint8_t*, size_t,
                           const aorta_action_server_options_t*,
                           const aorta_action_server_codec_t*,
                           const aorta_action_server_callbacks_t* callbacks,
                           aorta_action_server_t* out) {
        captured = *callbacks;
        *out = 721;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_action_server_free(721)).WillOnce(Return(AORTA_OK));
  EXPECT_CALL(mock, aorta_node_free(720)).WillOnce(Return(AORTA_OK));

  auto node = Node::Create("action_cancel_cb_node").value();
  std::vector<std::string> cancelled;
  auto server = ActionServer<TestAction>::Serve(
                    node, TestAction::ActionName(), ActionServerOptions{},
                    [](auto, auto) {},
                    [&](const std::string& goal_id) {
                      cancelled.push_back(goal_id);
                    })
                    .value();
  ASSERT_NE(captured.cancel_requested, nullptr);
  ASSERT_NE(captured.user_data_release, nullptr);

  const std::string goal_id = kGoalId;
  captured.cancel_requested(
      aorta_string_view_t{reinterpret_cast<const uint8_t*>(goal_id.data()),
                          goal_id.size()},
      captured.user_data);
  ASSERT_EQ(cancelled.size(), 1u);
  EXPECT_EQ(cancelled[0], goal_id);
}

TEST(ActionTest, ServeRejectsCancelHandlerWithoutCancelSupport) {
  SetMockReentrantContext(false);
  MockAortaFFI mock;
  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 730;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_action_server_new_with_callbacks(_, _, _, _, _, _,
                                                           _, _))
      .Times(0);
  EXPECT_CALL(mock, aorta_node_free(730)).WillOnce(Return(AORTA_OK));

  auto node = Node::Create("action_cancel_cb_unsupported_node").value();
  ActionServerOptions options;
  options.supports_cancel = false;
  auto server_or = ActionServer<TestAction>::Serve(
      node, TestAction::ActionName(), options, [](auto, auto) {},
      [](const std::string&) {});
  ASSERT_FALSE(server_or.ok());
  EXPECT_EQ(server_or.status().code(), AORTA_ERR_INVALID_ARGUMENT);
}

TEST(ActionTest, FailedServeReleasesHandlerState) {
  SetMockReentrantContext(false);
  MockAortaFFI mock;
  EXPECT_CALL(mock, aorta_node_new(_, _, _))
      .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
        *out = 740;
        return AORTA_OK;
      }));
  EXPECT_CALL(mock, aorta_action_server_new_with_callbacks(
                        740, _, _, _, _, _, _, _))
      .WillOnce(Return(AORTA_ERR_CONCURRENT_CONFLICT));
  EXPECT_CALL(mock, aorta_node_free(740)).WillOnce(Return(AORTA_OK));

  auto node = Node::Create("action_serve_fail_node").value();
  auto sentinel = std::make_shared<int>(0);
  auto server_or = ActionServer<TestAction>::Serve(
      node, TestAction::ActionName(), ActionServerOptions{},
      [sentinel](auto, auto) {});
  ASSERT_FALSE(server_or.ok());
  // The handler (and the sentinel it captured) must not be kept alive by
  // the library's reference after a failed creation.
  EXPECT_EQ(sentinel.use_count(), 1);
}

TEST(ActionTest, ServerStateOutlivesTheServerUntilReleased) {
  SetMockReentrantContext(false);
  auto sentinel = std::make_shared<int>(0);
  {
    MockAortaFFI mock;
    EXPECT_CALL(mock, aorta_node_new(_, _, _))
        .WillOnce(Invoke([](const char*, const char*, aorta_node_t* out) {
          *out = 750;
          return AORTA_OK;
        }));
    EXPECT_CALL(mock, aorta_action_server_new_with_callbacks(
                          750, _, _, _, _, _, _, _))
        .WillOnce(Invoke([](aorta_node_t, const char*, const uint8_t*, size_t,
                            const aorta_action_server_options_t*,
                            const aorta_action_server_codec_t*,
                            const aorta_action_server_callbacks_t*,
                            aorta_action_server_t* out) {
          *out = 751;
          return AORTA_OK;
        }));
    EXPECT_CALL(mock, aorta_action_server_free(751))
        .WillOnce(Return(AORTA_OK));
    EXPECT_CALL(mock, aorta_node_free(750)).WillOnce(Return(AORTA_OK));

    auto node = Node::Create("action_state_release_node").value();
    auto server = ActionServer<TestAction>::Serve(
                      node, TestAction::ActionName(), ActionServerOptions{},
                      [sentinel](auto, auto) {})
                      .value();
    server.reset();
    // Freed, but the (emulated) library has not released yet: a callback
    // still running at this point would find its state intact.
    EXPECT_EQ(sentinel.use_count(), 2);
  }
  // The mock runs the owed release when it is destroyed.
  EXPECT_EQ(sentinel.use_count(), 1);
}

}  // namespace
}  // namespace aorta
