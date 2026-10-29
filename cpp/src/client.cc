#include "aorta/client.h"

#include "aorta/detail/reentrancy_check.h"

#include <mutex>
#include <utility>
#include <vector>

#include "internal/common.h"
#include "internal/ffi.h"

namespace aorta {

Client::~Client() {
  if (handle_ != 0) {
    aorta_client_free(handle_);
    handle_ = 0;
  }
}

ClientResponse::~ClientResponse() {
  if (bytes_.internal != nullptr) {
    aorta_owned_bytes_free(&bytes_);
  }
}

ClientResponse::ClientResponse(ClientResponse&& other) noexcept
    : bytes_(other.bytes_) {
  // slices_init_flag_ is default-constructed (not yet called).
  // slices_cache_ will be rebuilt on demand from the new bytes_.
  other.bytes_ = aorta_owned_bytes_t{};
  other.slices_cache_.clear();
}

ClientResponse& ClientResponse::operator=(ClientResponse&& other) noexcept {
  if (this != &other) {
    if (bytes_.internal != nullptr) {
      aorta_owned_bytes_free(&bytes_);
    }
    bytes_ = other.bytes_;
    // Reset once_flag via optional::emplace() — safely destroys the old flag
    // and constructs a fresh one in place, without manual placement-new.
    slices_init_flag_.emplace();
    slices_cache_.clear();
    other.bytes_ = aorta_owned_bytes_t{};
    other.slices_cache_.clear();
  }
  return *this;
}

bool ClientResponse::IsValid() const {
  return bytes_.internal != nullptr || bytes_.view.total_len == 0;
}

bool ClientResponse::HasPayload() const {
  return bytes_.internal != nullptr && bytes_.view.slice_count > 0 &&
         bytes_.view.slices != nullptr;
}

MessageView ClientResponse::View() const {
  MessageView out;
  out.size = bytes_.view.total_len;

  if (!HasPayload()) {
    return out;
  }

  std::call_once(*slices_init_flag_, [this]() {
    slices_cache_.reserve(bytes_.view.slice_count);
    for (std::size_t i = 0; i < bytes_.view.slice_count; ++i) {
      const auto& slice = bytes_.view.slices[i];
      slices_cache_.push_back(MessageSlice{slice.data, slice.len});
    }
  });

  out.slices = slices_cache_.data();
  out.slice_count = slices_cache_.size();
  if (out.slice_count == 1) {
    out.data = out.slices[0].data;
    out.size = out.slices[0].size;
  }
  return out;
}

StatusOr<ClientResponse> Client::Call(std::vector<uint8_t>&& request) const {
  if (request.empty()) {
    return StatusOr<ClientResponse>(
        Status(AORTA_ERR_INVALID_ARGUMENT, "empty request"));
  }

  AORTA_CHECK_NOT_REENTRANT("Client::Call");

  aorta_owned_bytes_slice_t ffi_request_slice{};
  aorta_owned_bytes_view_t ffi_request{};
  internal::BuildOwnedBytesView(std::move(request), &ffi_request_slice,
                                &ffi_request);

  aorta_owned_bytes_t response{};
  aorta_result_t rc = aorta_client_call(handle_, &ffi_request, &response);

  if (rc != AORTA_OK) {
    return StatusOr<ClientResponse>(Status::FromResult(rc));
  }

  return StatusOr<ClientResponse>(ClientResponse(response));
}

Client::Stats Client::GetStats() const {
  Stats out;
  if (handle_ == 0) {
    return out;
  }

  aorta_client_stats_t ffi{};
  if (aorta_client_get_stats(handle_, &ffi) != AORTA_OK) {
    return out;
  }

  out.call_count = ffi.call_count;
  out.success_count = ffi.success_count;
  out.timeout_count = ffi.timeout_count;
  out.concurrent_conflict_count = ffi.concurrent_conflict_count;
  out.error_count = ffi.error_count;
  out.retry_count = ffi.retry_count;
  out.last_call_started_ns = ffi.last_call_started_ns;
  out.last_call_finished_ns = ffi.last_call_finished_ns;
  out.last_call_latency_ns = ffi.last_call_latency_ns;
  return out;
}

StatusOr<bool> Client::MatchingStatus() const {
  AORTA_CHECK_NOT_REENTRANT("Client::MatchingStatus");
  uint8_t matching = 0;
  aorta_result_t rc = aorta_client_matching_status(handle_, &matching);
  if (rc != AORTA_OK) {
    return Status::FromResult(rc);
  }
  return matching != 0;
}

StatusOr<bool> Client::WaitForMatching(uint64_t timeout_ms) const {
  AORTA_CHECK_NOT_REENTRANT("Client::WaitForMatching");
  uint8_t matching = 0;
  aorta_result_t rc =
      aorta_client_wait_for_matching(handle_, timeout_ms, &matching);
  if (rc != AORTA_OK) {
    return Status::FromResult(rc);
  }
  return matching != 0;
}

}  // namespace aorta
