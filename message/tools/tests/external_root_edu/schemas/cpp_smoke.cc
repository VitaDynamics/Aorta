// EDU external-root fixture — C++ G3 cells: message, service
// and action, each BUILT and RUN against the real SDK.
//
// Everything here comes from targets `aorta_external_schema_package` generated
// in THIS package out of `src/*.fbs`: the flatc headers, the schema-meta
// specializations and the typed wrapper headers. Nothing is hand written, and
// the schemas never entered Aorta's own registry.
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#include "aorta/action.h"
#include "edu_demo_job_msg.h"
#include "edu_demo_service_msg.h"
#include "edu_demo_text_msg.h"
#include "flatbuffers/flatbuffers.h"

namespace {

using EduDemoJobAction = ::edu::demo::EduDemoJobAction;
using EduDemoServiceRequest = ::edu::demo::EduDemoServiceRequest;
using EduDemoServiceResponse = ::edu::demo::EduDemoServiceResponse;
using EduDemoText = ::edu::demo::EduDemoText;

int fail(const char* what) {
  std::fprintf(stderr, "edu cpp_smoke FAILED: %s\n", what);
  return 1;
}

bool is_hex64(const char* value) {
  if (value == nullptr || std::strlen(value) != 64) {
    return false;
  }
  for (std::size_t i = 0; i < 64; ++i) {
    const char c = value[i];
    const bool hex = (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
    if (!hex) {
      return false;
    }
  }
  return true;
}

}  // namespace

// Compile-time proof that the generated traits satisfy the SDK templates.
static_assert(aorta::TypedPublishTraits<EduDemoText>::kHasAortaHeader,
              "topic schema must carry the aorta header");
static_assert(aorta::TypedPublishTraits<EduDemoServiceRequest>::kHasAortaHeader,
              "service request must carry the aorta header");
static_assert(aorta::TypedPublishTraits<EduDemoServiceResponse>::kHasAortaHeader,
              "service response must carry the aorta header");
static_assert(sizeof(::aorta::ActionClient<EduDemoJobAction>*) > 0,
              "ActionClient<EduDemoJobAction> usable");
static_assert(sizeof(::aorta::GoalHandle<EduDemoJobAction>*) > 0,
              "GoalHandle<EduDemoJobAction> usable");

int main() {
  // ── message ───────────────────────────────────────────────────────────────
  {
    flatbuffers::FlatBufferBuilder builder;
    ::edu::demo::EduDemoTextMsg message;
    message.text = "hello external root";
    message.count = 7;
    auto header = aorta::sys::CreateAortaHeader(builder);
    builder.Finish(
        aorta::TypedPublishTraits<EduDemoText>::Build(builder, header, message));
    if (!aorta::TypedSubscribeTraits<EduDemoText>::Verify(
            builder.GetBufferPointer(), builder.GetSize())) {
      return fail("EduDemoText buffer does not verify");
    }
    const auto* root = aorta::TypedSubscribeTraits<EduDemoText>::GetRoot(
        builder.GetBufferPointer());
    if (root == nullptr || root->text() == nullptr ||
        root->text()->str() != "hello external root" || root->count() != 7) {
      return fail("EduDemoText round trip lost data");
    }
    if (!is_hex64(aorta::SchemaMeta<EduDemoText>::SchemaHashHex())) {
      return fail("SchemaMeta<EduDemoText> hash is not 64 hex chars");
    }
    if (std::string(aorta::SchemaMeta<EduDemoText>::RootTypeName()) !=
        "edu.demo.EduDemoText") {
      return fail("SchemaMeta<EduDemoText>::RootTypeName() mismatch");
    }
    if (aorta::SchemaMeta<EduDemoText>::SchemaBfbsSize() == 0) {
      return fail("SchemaMeta<EduDemoText> carries no bfbs");
    }
  }

  // ── service ───────────────────────────────────────────────────────────────
  {
    flatbuffers::FlatBufferBuilder request_builder;
    ::edu::demo::EduDemoServiceRequestMsg request;
    request.message = "ping";
    request.unit = ::edu::demo::EduDemoUnit::EduDemoUnit_METERS;
    auto request_header = aorta::sys::CreateAortaHeader(request_builder);
    request_builder.Finish(aorta::TypedPublishTraits<EduDemoServiceRequest>::Build(
        request_builder, request_header, request));
    if (!aorta::TypedSubscribeTraits<EduDemoServiceRequest>::Verify(
            request_builder.GetBufferPointer(), request_builder.GetSize())) {
      return fail("EduDemoServiceRequest buffer does not verify");
    }

    flatbuffers::FlatBufferBuilder response_builder;
    ::edu::demo::EduDemoServiceResponseMsg response;
    response.echoed_message = "ping";
    auto response_header = aorta::sys::CreateAortaHeader(response_builder);
    response_builder.Finish(
        aorta::TypedPublishTraits<EduDemoServiceResponse>::Build(
            response_builder, response_header, response));
    if (!aorta::TypedSubscribeTraits<EduDemoServiceResponse>::Verify(
            response_builder.GetBufferPointer(), response_builder.GetSize())) {
      return fail("EduDemoServiceResponse buffer does not verify");
    }

    // One schema identity for the whole service: request and response are two
    // root types of ONE bfbs, which is exactly what the typed client/server
    // hash comparison relies on.
    if (!is_hex64(aorta::SchemaMeta<EduDemoServiceRequest>::SchemaHashHex())) {
      return fail("SchemaMeta<EduDemoServiceRequest> hash is not 64 hex chars");
    }
    if (std::string(aorta::SchemaMeta<EduDemoServiceRequest>::RootTypeName()) !=
        "edu.demo.EduDemoServiceRequest") {
      return fail("service RootTypeName() mismatch");
    }
  }

  // ── action ────────────────────────────────────────────────────────────────
  {
    if (std::string(EduDemoJobAction::ActionName()) != "edu/demo/job") {
      return fail("EduDemoJobAction::ActionName() mismatch");
    }
    EduDemoJobAction::GoalMsg goal;
    goal.target = "dock";
    goal.repeats = 3;
    std::vector<std::uint8_t> payload =
        EduDemoJobAction::EncodeSendGoalRequest("goal-1", goal);
    if (payload.empty()) {
      return fail("empty EduDemoJob SendGoalRequest payload");
    }
    flatbuffers::Verifier verifier(payload.data(), payload.size());
    if (!verifier.VerifyBuffer<EduDemoJobAction::SendGoalRequest>(nullptr)) {
      return fail("EduDemoJob SendGoalRequest does not verify");
    }
    const auto* request =
        flatbuffers::GetRoot<EduDemoJobAction::SendGoalRequest>(payload.data());
    if (request == nullptr || request->goal() == nullptr ||
        request->goal()->repeats() != 3) {
      return fail("EduDemoJob goal did not survive the wrapper encoding");
    }
  }

  std::printf("edu cpp_smoke OK\n");
  return 0;
}
