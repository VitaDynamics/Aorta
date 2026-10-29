// SDK-only two-process transport and ContextStore proof.
#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>
#include <vector>
#include "aorta/aorta.h"
#include "aorta/action.h"
#include "aorta/contextstore.h"
#include "aorta_base_generated.h"

int main() {
  return aorta::RunOrFatal([]() -> int {
    auto node = aorta::Must(aorta::Node::Create("sdk_proof_server", "default"), "node");
    std::atomic<int> topics{0}, calls{0};
    auto sub = aorta::Must(node->CreateSubscriber("sdk_proof/topic",
        [&](const aorta::MessageView& view, const aorta::MessageContextView&) {
          const auto bytes = view.ToVector();
          flatbuffers::Verifier verifier(bytes.data(), bytes.size());
          if (verifier.VerifyBuffer<aorta::sys::AortaHeader>(nullptr) &&
              flatbuffers::GetRoot<aorta::sys::AortaHeader>(bytes.data())->sequence() == 42) {
            ++topics;
          }
        }), "subscriber");
    auto service = aorta::Must(node->CreateService("/sdk_proof/echo",
        [&](const aorta::MessageView& view, aorta::ServiceRequest&& request) {
          auto bytes = view.ToVector();
          if (bytes == std::vector<uint8_t>{1, 2, 3}) {
            aorta::CheckOk(request.Reply({4, 5, 6}), "reply");
            ++calls;
          } else {
            aorta::CheckOk(request.ReplyError("unexpected request"), "reply error");
          }
        }), "service");
    auto store = aorta::Must(node->CreateContextStore(), "store");
    aorta::CheckOk(store->DeclareOwnedPrefix("sdk_proof/**"), "prefix");
    aorta::CheckOk(store->SetString("sdk_proof/frame_id", "base_link"), "set");
    auto value = aorta::Must(store->GetString("sdk_proof/frame_id"), "get");
    const bool context = value.value == "base_link";
    std::printf("PROOF context local get key=sdk_proof/frame_id value=%s\n", value.value.c_str());
    std::printf("PROOF server ready\n"); std::fflush(stdout);
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
    while (std::chrono::steady_clock::now() < deadline && (topics < 3 || calls < 1)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    std::this_thread::sleep_for(std::chrono::seconds(3));
    std::printf("PROOF server summary topic=%s service=%s context=%s\n",
        topics >= 3 ? "PASS" : "FAIL", calls == 1 ? "PASS" : "FAIL", context ? "PASS" : "FAIL");
    return topics >= 3 && calls == 1 && context ? 0 : 1;
  });
}
