#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "aorta/status.h"
#include "aorta/types.h"

namespace aorta {

class Node;
class ClientResponse {
 public:
  ClientResponse() = default;
  ~ClientResponse();

  ClientResponse(ClientResponse&& other) noexcept;
  ClientResponse& operator=(ClientResponse&& other) noexcept;
  ClientResponse(const ClientResponse&) = delete;
  ClientResponse& operator=(const ClientResponse&) = delete;

  /// Returns true if the response holds valid data. An empty response
  /// (total_len == 0) from a successful RPC call is also considered valid.
  bool IsValid() const;
  /// Returns a view into the response payload. The returned pointers are valid
  /// while this ClientResponse object is alive, including across repeated
  /// View() calls. Thread-safe: the internal slice cache is built exactly once.
  MessageView View() const;
  bool IsContiguous() const { return View().IsContiguous(); }
  std::vector<uint8_t> ToVector() const { return View().ToVector(); }

 private:
  friend class Client;
  explicit ClientResponse(aorta_owned_bytes_t bytes) : bytes_(bytes) {}

  /// Returns true if the underlying bytes contain actual slice data.
  bool HasPayload() const;

  aorta_owned_bytes_t bytes_{};
  mutable std::optional<std::once_flag> slices_init_flag_{std::in_place};
  mutable std::vector<MessageSlice> slices_cache_;
};

/// RPC client that sends requests to services and receives responses.
class Client {
 public:
  struct Stats {
    uint64_t call_count = 0;
    uint64_t success_count = 0;
    uint64_t timeout_count = 0;
    uint64_t concurrent_conflict_count = 0;
    uint64_t error_count = 0;
    uint64_t retry_count = 0;
    uint64_t last_call_started_ns = 0;
    uint64_t last_call_finished_ns = 0;
    uint64_t last_call_latency_ns = 0;
  };

  ~Client();

  const std::string& ServiceName() const { return service_name_; }

  StatusOr<ClientResponse> Call(std::vector<uint8_t>&& request) const;
  Stats GetStats() const;

  /// One-shot readiness check: true if at least one matching service provider
  /// currently exists for this client's service key. Reads local routing
  /// tables (no network round-trip).
  StatusOr<bool> MatchingStatus() const;

  /// Wait up to `timeout_ms` for a service provider to appear; returns the
  /// final state (`false` on timeout, which is not an error). Use to gate the
  /// first `Call()` on a fresh client.
  StatusOr<bool> WaitForMatching(uint64_t timeout_ms) const;

  /// The raw C-ABI handle — for the Flow C++20 layer (flow::call) and
  /// other C-interop. OWNED by this wrapper: never free it yourself.
  aorta_client_t handle() const { return handle_; }

 private:
  friend class Node;

  Client(aorta_client_t handle, std::string service_name,
         std::shared_ptr<Node> node)
      : handle_(handle),
        service_name_(std::move(service_name)),
        node_(std::move(node)) {}

  aorta_client_t handle_{0};
  std::string service_name_;
  std::shared_ptr<Node> node_;
};

}  // namespace aorta
