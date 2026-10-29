#include <atomic>
#include <chrono>
#include <cstdint>
#include <csignal>
#include <string>
#include <thread>
#include <vector>

#include "aorta/aorta.h"
#include "foxglove_compressed_video_msg.h"

namespace {

std::atomic<bool> g_running{true};

void SignalHandler(int) { g_running = false; }

foxglove::CompressedVideoMsg BuildCompressedVideoMsg(uint64_t sequence,
                                                     int64_t now_ns,
                                                     const std::string& frame_id) {
  foxglove::CompressedVideoMsg msg;

  uint64_t safe_now_ns = 0;
  if (now_ns > 0) {
    safe_now_ns = static_cast<uint64_t>(now_ns);
  }
  msg.timestamp = foxglove::Time(static_cast<uint32_t>(safe_now_ns / 1000000000ULL),
                                 static_cast<uint32_t>(safe_now_ns % 1000000000ULL));
  msg.frame_id = frame_id;
  msg.format = std::string("h264");
  msg.data = std::vector<uint8_t>{
      0x00, 0x00, 0x00, 0x01, 0x09, 0x10, static_cast<uint8_t>(sequence & 0xff)};
  return msg;
}

}  // namespace

int main(int argc, char* argv[]) {
  return aorta::RunOrFatal([&]() -> int {
  std::signal(SIGINT, SignalHandler);
  std::signal(SIGTERM, SignalHandler);
  (void)aorta::InitLoggingFromEnv();

  std::string node_name = "cpp_foxglove_cv_publisher";
  std::string group = "default";
  std::string topic = "/foxglove/compressed_video";

  for (int i = 1; i < argc - 1; ++i) {
    if (std::string(argv[i]) == "--name") {
      node_name = argv[++i];
    } else if (std::string(argv[i]) == "--group") {
      group = argv[++i];
    } else if (std::string(argv[i]) == "--topic") {
      topic = argv[++i];
    }
  }
  (void)aorta::SetDefaultLogContext(node_name, group);

  auto node = aorta::Must(aorta::Node::Create(node_name, group), "Create node");

  auto publisher =
      aorta::Must(node->CreatePublisher<foxglove::CompressedVideo>(topic),
                  "Create publisher");

  AORTA_LOG(INFO) << "foxglove compressed video C++ publisher ready: node=" << node->name()
                  << " group=" << node->group() << " topic=" << topic;

  uint64_t sequence = 0;
  while (g_running.load(std::memory_order_relaxed)) {
    auto now_result = aorta::NowNs();
    const int64_t now_ns = now_result.ok() ? now_result.value() : 0;
    const std::string frame_id = "camera/" + node->name();
    aorta::CheckOk(
        publisher->PublishTyped<foxglove::CompressedVideo>(
            [&](foxglove::CompressedVideoMsg& msg) {
              msg = BuildCompressedVideoMsg(sequence, now_ns, frame_id);
            }),
        "Publish foxglove CompressedVideo");

    if ((sequence % 10) == 0) {
      AORTA_LOG(INFO) << "published foxglove CompressedVideo frame seq=" << sequence
                      << " topic=" << topic;
    }

    ++sequence;
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }

  AORTA_LOG(INFO) << "Shutting down...";
  return 0;
  });
}
