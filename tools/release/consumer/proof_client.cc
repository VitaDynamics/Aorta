// SDK-only consumer: middleware base FlatBuffer, raw RPC, and remote context read.
#include <chrono>
#include <cstdio>
#include <thread>
#include <vector>
#include "aorta/aorta.h"
#include "aorta/contextstore.h"
#include "aorta_base_generated.h"

int main() {
  return aorta::RunOrFatal([]() -> int {
    auto node = aorta::Must(aorta::Node::Create("sdk_proof_client", "default"), "node");
    auto publisher = aorta::Must(node->CreatePublisher("sdk_proof/topic"), "publisher");
    std::this_thread::sleep_for(std::chrono::seconds(2));
    flatbuffers::FlatBufferBuilder builder;
    builder.Finish(aorta::sys::CreateAortaHeader(builder, 0, 0, 42));
    for (int i = 0; i < 5; ++i) {
      aorta::CheckOk(publisher->PublishBytes(builder.GetBufferPointer(), builder.GetSize()), "publish");
      std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    aorta::ClientOptions options;
    options.timeout_ms = 5000; options.allow_retry = false;
    auto client = aorta::Must(node->CreateClient("/sdk_proof/echo", options), "client");
    auto reply = aorta::Must(client->Call({1, 2, 3}), "call");
    const bool service = reply.ToVector() == std::vector<uint8_t>{4, 5, 6};
    auto store = aorta::Must(node->CreateContextStore(), "store");
    auto value = aorta::Must(store->GetString("sdk_proof/frame_id"), "remote get");
    const bool context = value.value == "base_link";
    std::printf("PROOF context remote get key=sdk_proof/frame_id value=%s\n", value.value.c_str());
    std::printf("PROOF client summary topic=PASS service=%s context=%s\n",
        service ? "PASS" : "FAIL", context ? "PASS" : "FAIL");
    return service && context ? 0 : 1;
  });
}
