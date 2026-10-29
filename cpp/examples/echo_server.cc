#include <atomic>
#include <chrono>
#include <csignal>
#include <string>
#include <thread>

#include "aorta/aorta.h"
#include "echo_msg.h"
#include "schema_attach_policy.h"

std::atomic<bool> g_running{true};

void signal_handler(int) { g_running = false; }

int main(int argc, char* argv[]) {
  return aorta::RunOrFatal([&]() -> int {
  std::signal(SIGINT, signal_handler);
  std::signal(SIGTERM, signal_handler);
  (void)aorta::InitLoggingFromEnv();

  std::string node_name = "echo_server";
  std::string group = "default";
  std::string service_name = "/echo";
  std::string schema_attach = "never";

  for (int i = 1; i < argc - 1; i++) {
    if (std::string(argv[i]) == "--name") {
      node_name = argv[++i];
    } else if (std::string(argv[i]) == "--group") {
      group = argv[++i];
    } else if (std::string(argv[i]) == "--service") {
      service_name = argv[++i];
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

  AORTA_LOG(INFO) << "Echo server node=" << node->name() << " group="
                  << node->group() << " service=" << service_name;
  AORTA_LOG(INFO) << "schema_attach=" << schema_attach;

  aorta::ServiceOptions options;
  options.schema_attach_policy = attach_policy;

  auto service_result = node->CreateServiceTypedView<demo::EchoRequest,
                                                     demo::EchoResponse>(
      service_name,
      [](const demo::EchoRequest& request,
         aorta::TypedServiceResponder<demo::EchoResponse> reply) {
        if (request.message() == nullptr) {
          AORTA_LOG(WARN) << "Echo request missing message field";
          aorta::CheckOk(reply.Reply([](demo::EchoResponseMsg& msg) {
                           msg.echoed_message = std::string();
                         }),
                         "Reply empty typed echo response");
          return;
        }

        const std::string echoed = request.message()->str();
        aorta::CheckOk(reply.Reply([&](demo::EchoResponseMsg& msg) {
                         msg.echoed_message = echoed;
                       }),
                       "Reply typed echo response");
        AORTA_LOG(INFO) << "Replied typed echo request text=\"" << echoed << "\"";
      },
      options);

  auto service = aorta::Must(std::move(service_result), "Create typed service");
  (void)service;

  AORTA_LOG(INFO) << "Echo server ready, replying with typed EchoResponse.";
  AORTA_LOG(INFO) << "Press Ctrl+C to exit";

  while (g_running) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  AORTA_LOG(INFO) << "Shutting down...";
  return 0;
  });
}
