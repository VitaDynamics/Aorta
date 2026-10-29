#include <atomic>
#include <chrono>
#include <cstdint>
#include <csignal>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "all_types_msg.h"
#include "aorta/aorta.h"

namespace {

std::atomic<bool> g_running{true};

void SignalHandler(int) { g_running = false; }

}  // namespace

int main(int argc, char* argv[]) {
  return aorta::RunOrFatal([&]() -> int {
  std::signal(SIGINT, SignalHandler);
  std::signal(SIGTERM, SignalHandler);
  (void)aorta::InitLoggingFromEnv();

  std::string node_name = "cpp_all_types_talker";
  std::string group = "default";
  std::string topic = "/demo/all_types";

  for (int i = 1; i < argc - 1; ++i) {
    if (std::string(argv[i]) == "--name") {
      node_name = argv[++i];
    } else if (std::string(argv[i]) == "--group") {
      group = argv[++i];
    } else if (std::string(argv[i]) == "--topic") {
      topic = argv[++i];
    }
  }

  auto node = aorta::Must(aorta::Node::Create(node_name, group), "Create node");

  auto publisher = aorta::Must(node->CreatePublisher<demo::AllTypes>(topic),
                               "Create publisher");

  AORTA_LOG(INFO) << "cpp all types talker ready: node=" << node->name()
                  << " group=" << node->group() << " topic=" << topic;

  uint64_t sequence = 0;
  while (g_running.load(std::memory_order_relaxed)) {
    aorta::CheckOk(
        publisher->PublishTyped<demo::AllTypes>([&](demo::AllTypesMsg& msg) {
          msg.flag = true;
          msg.val_int8 = static_cast<int8_t>(-8);
          msg.val_uint8 = static_cast<uint8_t>(8);
          msg.val_int16 = static_cast<int16_t>(-16);
          msg.val_uint16 = static_cast<uint16_t>(16);
          msg.val_int32 = static_cast<int32_t>(-32);
          msg.val_uint32 = static_cast<uint32_t>(32);
          msg.val_int64 = static_cast<int64_t>(-64);
          msg.val_uint64 = static_cast<uint64_t>(64 + sequence);
          msg.val_float32 = 1.25f;
          msg.val_float64 = 2.5;

          msg.text = std::string("all-types-") + std::to_string(sequence);
          msg.color = demo::Color_RED;
          msg.mode = demo::Mode_RUN;
          msg.pose = demo::Vec3(1.0f, 2.0f, 3.0f);

          auto child = std::make_shared<demo::ChildMsg>();
          child->id = 100 + static_cast<uint32_t>(sequence);
          child->name = std::string("child-") + std::to_string(sequence);
          child->pose = demo::Vec3(4.0f, 5.0f, 6.0f);
          msg.child = child;
          msg.payload = demo::PayloadUnionMsg{child};

          auto meta = std::make_shared<demo::MetaInfoMsg>();
          meta->tag = std::string("meta-") + std::to_string(sequence);
          meta->level = 9;
          msg.meta = meta;

          const std::vector<uint8_t> bytes_data{1, 2, 3, 4};
          const std::vector<int32_t> ints_data{10, 20, 30};
          const std::vector<float> floats_data{0.5f, 1.5f};
          msg.bytes = bytes_data;
          msg.ints = ints_data;
          msg.floats = floats_data;
          msg.names = std::vector<std::string>{"alice", "bob"};
          msg.poses = std::vector<demo::Vec3>{demo::Vec3(0.0f, 1.0f, 2.0f)};
          msg.children = std::vector<std::shared_ptr<demo::ChildMsg>>{child};
          msg.colors =
              std::vector<demo::Color>{demo::Color_GREEN, demo::Color_BLUE};
          msg.modes = std::vector<demo::Mode>{demo::Mode_IDLE, demo::Mode_STOP};
        }),
        "Publish all-types sample");
    ++sequence;
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  }

  AORTA_LOG(INFO) << "Shutting down...";
  return 0;
  });
}
