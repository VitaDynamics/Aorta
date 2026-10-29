#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include <aorta.h>

#include "aorta/types.h"

namespace aorta::internal {

uint64_t LogIntervalNs();
uint64_t NowNs();
std::string ResolveGroupFromEnv();

void MaybeLogCallbackException(const char* kind,
                               std::atomic<uint64_t>& last_log_ns);
void LogWarn(const char* scope, const std::string& message);
void LogWarnf(const char* scope, const char* format, ...);

MessageContext ToMessageContext(const aorta_msg_context_t* ctx);
MessageContextView ToMessageContextView(const aorta_msg_context_t* ctx);
MessageView ToMessageView(const aorta_bytes_view_t* payload,
                          MessageSlice* single_slice,
                          std::vector<MessageSlice>* multi_slices);

void BuildOwnedBytesView(std::vector<uint8_t>&& payload,
                         aorta_owned_bytes_slice_t* out_slice,
                         aorta_owned_bytes_view_t* out_view);

}  // namespace aorta::internal
