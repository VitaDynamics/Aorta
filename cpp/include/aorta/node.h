#pragma once

#include <chrono>
#include <cstring>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

#include "aorta/options.h"
#include "aorta/pull_subscriber.h"
#include "aorta/qos.h"
#include "aorta/schema_meta.h"
#include "aorta/capture.h"
#include "aorta/executor.h"
#include "aorta/logging.h"
#include "aorta/service.h"
#include "aorta/status.h"
#include "aorta/subscriber.h"
#include "aorta/timer.h"
#include "aorta/typed_publish.h"
#include "aorta/typed_service.h"
#include "aorta/typed_subscribe.h"

namespace aorta {

namespace detail {
template <typename T, typename = void>
struct SchemaRootTypeName {
  static const char* Get() { return nullptr; }
};

template <typename T>
struct SchemaRootTypeName<
    T, std::void_t<decltype(SchemaMeta<T>::RootTypeName())>> {
  static const char* Get() { return SchemaMeta<T>::RootTypeName(); }
};

template <typename T, typename = void>
struct SchemaFixedSize {
  static std::optional<std::size_t> Get() { return std::nullopt; }
};

template <typename T>
struct SchemaFixedSize<T, std::void_t<decltype(SchemaMeta<T>::FixedSize())>> {
  static std::optional<std::size_t> Get() { return SchemaMeta<T>::FixedSize(); }
};

template <typename T, typename = void>
struct SchemaShmCapacityHint {
  static std::optional<std::size_t> Get() { return std::nullopt; }
};

template <typename T>
struct SchemaShmCapacityHint<
    T, std::void_t<decltype(SchemaMeta<T>::ShmCapacityHint())>> {
  static std::optional<std::size_t> Get() {
    return SchemaMeta<T>::ShmCapacityHint();
  }
};

template <typename T, typename = void>
struct SchemaHashHex {
  static const char* Get() { return nullptr; }
};

template <typename T>
struct SchemaHashHex<T, std::void_t<decltype(SchemaMeta<T>::SchemaHashHex())>> {
  static const char* Get() { return SchemaMeta<T>::SchemaHashHex(); }
};

template <typename RequestSchema, typename ResponseSchema>
Status InjectTypedClientSchemaOptions(ClientOptions* options) {
  const char* request_hash = SchemaHashHex<RequestSchema>::Get();
  const char* response_hash = SchemaHashHex<ResponseSchema>::Get();

  if (request_hash != nullptr && response_hash != nullptr &&
      std::strcmp(request_hash, response_hash) != 0) {
    return Status(AORTA_ERR_INVALID_ARGUMENT,
                  "typed client request/response must use the same service "
                  "schema hash");
  }

  const char* selected_hash =
      request_hash != nullptr ? request_hash : response_hash;
  if (selected_hash == nullptr || selected_hash[0] == '\0') {
    return Status(AORTA_ERR_INVALID_ARGUMENT,
                  "typed client requires service schema hash");
  }

  if (!options->service_schema_hash_hex.empty() &&
      options->service_schema_hash_hex != selected_hash) {
    return Status(
        AORTA_ERR_INVALID_ARGUMENT,
        "typed client service schema hash does not match request/response "
        "schema type");
  }

  if (options->service_schema_hash_hex.empty()) {
    options->service_schema_hash_hex = selected_hash;
  }
  return Status::Ok();
}

// Shared decode-failure path for the two typed service bridges (view and
// retained): log, then send a genuine error reply so the caller observes the
// decode failure instead of an empty-OK payload. `bridge_kind` is the log
// prefix ("typed service" / "typed retained service").
inline void ReplyTypedDecodeFailure(ServiceRequest& req,
                                    const std::string& service_name,
                                    const char* bridge_kind,
                                    const Status& decode_status) {
  AORTA_LOG(WARN) << bridge_kind << " request decode failed for "
                  << service_name << ": " << decode_status.message();
  if (req.IsValid()) {
    auto reply_status = req.ReplyError(std::string(bridge_kind) +
                                       " request decode failed: " +
                                       decode_status.message());
    if (!reply_status.ok()) {
      AORTA_LOG(WARN) << bridge_kind << " decode-failure reply failed for "
                      << service_name << ": " << reply_status.message();
    }
  }
}
}  // namespace detail

class Publisher;
class Subscriber;
class Service;
class Client;
template <typename Action>
class ActionClient;
class Timer;
class Executor;

/// Central handle to the Aorta communication layer.
///
/// Thread-safety:
///   - Create() must be called from a single thread.
///   - CreatePublisher/Subscriber/Service/Client/Timer and Now() are safe to
///     call concurrently from multiple threads once the Node is constructed.
///   - The destructor stops the internal executor and joins its thread; it must
///     not race with any of the Create* methods.
class Node : public std::enable_shared_from_this<Node> {
 public:
  static StatusOr<std::shared_ptr<Node>> Create(
      const std::string& name, const std::string& group = "default");

  ~Node();

  StatusOr<int64_t> Now() const;

  StatusOr<std::shared_ptr<Publisher>> CreatePublisher(
      const std::string& topic, const QoS& qos = QoS(),
      const PublisherOptions& options = PublisherOptions());

  template <typename T>
  StatusOr<std::shared_ptr<Publisher>> CreatePublisher(
      const std::string& topic, const QoS& qos = QoS(),
      PublisherOptions options = PublisherOptions()) {
    if (options.schema_bfbs.empty()) {
      const auto* bfbs = SchemaMeta<T>::SchemaBfbs();
      const auto size = SchemaMeta<T>::SchemaBfbsSize();
      if (bfbs && size > 0) {
        options.schema_bfbs.assign(bfbs, bfbs + size);
        if (options.schema_root_type.empty()) {
          const char* root_type = detail::SchemaRootTypeName<T>::Get();
          if (root_type != nullptr) {
            options.schema_root_type = root_type;
          }
        }
      }
    }

    if (!options.max_message_size.has_value()) {
      auto fixed = detail::SchemaFixedSize<T>::Get();
      if (fixed.has_value()) {
        options.max_message_size = fixed.value();
      } else {
        auto hint = detail::SchemaShmCapacityHint<T>::Get();
        if (hint.has_value()) {
          options.max_message_size = hint.value();
        }
      }
    }

    if (options.schema_bfbs.empty()) {
      return StatusOr<std::shared_ptr<Publisher>>(
          Status(AORTA_ERR_INVALID_ARGUMENT, "schema bfbs is required"));
    }

    return CreatePublisher(topic, qos, options);
  }

  StatusOr<std::shared_ptr<Subscriber>> CreateSubscriber(
      const std::string& topic, Subscriber::Callback callback,
      const SubscriberOptions& options = SubscriberOptions());

  // Backward-compatible overload: explicit QoS argument.
  StatusOr<std::shared_ptr<Subscriber>> CreateSubscriber(
      const std::string& topic, Subscriber::Callback callback, const QoS& qos,
      const SubscriberOptions& options = SubscriberOptions());

  // Pull-mode (Tier B-pull) subscriber: a Zenoh-native keep-last-`depth` ring
  // with no callback; drain it via `PullSubscriber::Recv` / `TryRecv` from your
  // own thread. Structural per-topic isolation, latest-wins.
  StatusOr<PullSubscriber> CreatePullSubscriber(const std::string& topic,
                                                std::size_t depth);

  // Pull-mode with full options control (e.g. context fields). `options.delivery`
  // and `options.receive_depth` are forced to InlinePull / `depth`.
  StatusOr<PullSubscriber> CreatePullSubscriber(const std::string& topic,
                                                std::size_t depth,
                                                SubscriberOptions options);

  template <typename T>
  StatusOr<std::shared_ptr<Subscriber>> CreateSubscriber(
      const std::string& topic, Subscriber::Callback callback,
      SubscriberOptions options = SubscriberOptions()) {
    if (options.schema_bfbs.empty()) {
      const auto* bfbs = SchemaMeta<T>::SchemaBfbs();
      const auto size = SchemaMeta<T>::SchemaBfbsSize();
      if (bfbs != nullptr && size > 0) {
        options.schema_bfbs.assign(bfbs, bfbs + size);
      }
    }
    return CreateSubscriber(topic, std::move(callback), options);
  }

  template <typename T>
  StatusOr<std::shared_ptr<Subscriber>> CreateSubscriber(
      const std::string& topic, Subscriber::Callback callback, const QoS& qos,
      SubscriberOptions options = SubscriberOptions()) {
    options.qos = qos;
    return CreateSubscriber<T>(topic, std::move(callback), std::move(options));
  }

  template <typename T>
  StatusOr<std::shared_ptr<Subscriber>> CreateSubscriberTypedView(
      const std::string& topic,
      std::function<void(const T&, const MessageContextView&)> callback,
      SubscriberOptions options = SubscriberOptions()) {
    if (options.schema_bfbs.empty()) {
      const auto* bfbs = SchemaMeta<T>::SchemaBfbs();
      const auto size = SchemaMeta<T>::SchemaBfbsSize();
      if (bfbs != nullptr && size > 0) {
        options.schema_bfbs.assign(bfbs, bfbs + size);
      }
    }

    auto bridge = [typed_cb = std::move(callback)](
                      const MessageView& view,
                      const MessageContextView& ctx_view) {
      if (!typed_cb) {
        return;
      }

      std::vector<uint8_t> owned_payload;
      const uint8_t* payload = nullptr;
      std::size_t payload_size = view.size;

      if (view.IsContiguous()) {
        payload = view.data;
      } else {
        owned_payload = view.ToVector();
        payload = owned_payload.empty() ? nullptr : owned_payload.data();
        payload_size = owned_payload.size();
      }

      if (!TypedSubscribeTraits<T>::Verify(payload, payload_size)) {
        return;
      }

      const T* message = TypedSubscribeTraits<T>::GetRoot(payload);
      if (message == nullptr) {
        return;
      }
      typed_cb(*message, ctx_view);
    };

    return CreateSubscriber(topic, std::move(bridge), options);
  }

  // ---- Raw capture + schema resolution ----
  // `CreateCaptureSubscriber` is a recorder/observer snoop over any aorta
  // namespace (no header parse / dedup / reorder); the callback runs on the
  // Zenoh callback thread, keep it short. `CreateSchemaResolver` resolves a
  // captured message's schema lazily, off the capture path.
  StatusOr<std::shared_ptr<CaptureSubscriber>> CreateCaptureSubscriber(
      std::vector<CaptureSelector> selectors,
      CaptureSubscriber::SampleCallback callback);

  StatusOr<std::shared_ptr<SchemaResolver>> CreateSchemaResolver(
      const SchemaResolverOptions& options = SchemaResolverOptions());

  template <typename T>
  StatusOr<std::shared_ptr<Subscriber>> CreateSubscriberTypedView(
      const std::string& topic,
      std::function<void(const T&, const MessageContextView&)> callback,
      const QoS& qos,
      SubscriberOptions options = SubscriberOptions()) {
    options.qos = qos;
    return CreateSubscriberTypedView<T>(topic, std::move(callback),
                                        std::move(options));
  }

  template <typename T>
  StatusOr<std::shared_ptr<Subscriber>> CreateSubscriberTypedRetained(
      const std::string& topic,
      std::function<void(TypedRetainedSamplePtr<T>)> callback,
      SubscriberOptions options = SubscriberOptions()) {
    if (options.schema_bfbs.empty()) {
      const auto* bfbs = SchemaMeta<T>::SchemaBfbs();
      const auto size = SchemaMeta<T>::SchemaBfbsSize();
      if (bfbs != nullptr && size > 0) {
        options.schema_bfbs.assign(bfbs, bfbs + size);
      }
    }

    auto bridge = [typed_cb = std::move(callback)](
                      const MessageView& view,
                      const MessageContextView& ctx_view) {
      if (!typed_cb) {
        return;
      }

      std::vector<uint8_t> owned_payload = view.ToVector();
      const uint8_t* payload =
          owned_payload.empty() ? nullptr : owned_payload.data();
      const std::size_t payload_size = owned_payload.size();

      if (!TypedSubscribeTraits<T>::Verify(payload, payload_size)) {
        return;
      }

      auto sample = std::shared_ptr<TypedRetainedSample<T>>(
          new TypedRetainedSample<T>(
              payload, payload_size, ctx_view.ToOwned(), std::move(owned_payload)));
      typed_cb(sample);
    };

    return CreateSubscriber(topic, std::move(bridge), options);
  }

  template <typename T>
  StatusOr<std::shared_ptr<Subscriber>> CreateSubscriberTypedRetained(
      const std::string& topic,
      std::function<void(TypedRetainedSamplePtr<T>)> callback, const QoS& qos,
      SubscriberOptions options = SubscriberOptions()) {
    options.qos = qos;
    return CreateSubscriberTypedRetained<T>(topic, std::move(callback),
                                            std::move(options));
  }

  StatusOr<std::shared_ptr<Service>> CreateService(
      const std::string& service_name, Service::RequestCallback callback,
      const ServiceOptions& options = ServiceOptions());

  // Typed RPC service API: inject request/response BFBS from SchemaMeta at
  // init.
  template <typename RequestSchema, typename ResponseSchema>
  StatusOr<std::shared_ptr<Service>> CreateService(
      const std::string& service_name, Service::RequestCallback callback,
      ServiceOptions options = ServiceOptions()) {
    if (options.service_schema_bfbs.empty()) {
      const auto* request_ptr = SchemaMeta<RequestSchema>::SchemaBfbs();
      const auto request_size = SchemaMeta<RequestSchema>::SchemaBfbsSize();
      const auto* response_ptr = SchemaMeta<ResponseSchema>::SchemaBfbs();
      const auto response_size = SchemaMeta<ResponseSchema>::SchemaBfbsSize();
      const bool has_request_schema =
          request_ptr != nullptr && request_size > 0;
      const bool has_response_schema =
          response_ptr != nullptr && response_size > 0;

      if (has_request_schema && has_response_schema) {
        const bool is_same_pointer = request_ptr == response_ptr;
        const bool has_same_size = request_size == response_size;
        const bool has_same_content =
            is_same_pointer ||
            (has_same_size &&
             std::memcmp(request_ptr, response_ptr, request_size) == 0);
        if (!has_same_content) {
          return StatusOr<std::shared_ptr<Service>>(
              Status(AORTA_ERR_INVALID_ARGUMENT,
                     "typed service request/response must use the same service "
                     "schema bfbs"));
        }
      }

      const uint8_t* selected_schema_ptr = nullptr;
      std::size_t selected_schema_size = 0;
      if (has_request_schema) {
        selected_schema_ptr = request_ptr;
        selected_schema_size = request_size;
      } else if (has_response_schema) {
        selected_schema_ptr = response_ptr;
        selected_schema_size = response_size;
      }

      if (selected_schema_ptr != nullptr && selected_schema_size > 0) {
        options.service_schema_bfbs.assign(
            selected_schema_ptr, selected_schema_ptr + selected_schema_size);
      }
    }

    if (options.request_root_type.empty()) {
      const char* request_root =
          detail::SchemaRootTypeName<RequestSchema>::Get();
      if (request_root != nullptr && request_root[0] != '\0') {
        options.request_root_type = request_root;
      }
    }
    if (options.response_root_type.empty()) {
      const char* response_root =
          detail::SchemaRootTypeName<ResponseSchema>::Get();
      if (response_root != nullptr && response_root[0] != '\0') {
        options.response_root_type = response_root;
      }
    }

    if (options.service_schema_bfbs.empty()) {
      return StatusOr<std::shared_ptr<Service>>(
          Status(AORTA_ERR_INVALID_ARGUMENT,
                 "typed service requires service schema bfbs"));
    }

    return CreateService(service_name, std::move(callback), options);
  }

  StatusOr<std::shared_ptr<Client>> CreateClient(
      const std::string& service_name,
      const ClientOptions& options = ClientOptions());

  StatusOr<std::shared_ptr<Client>> CreateClient(
      const std::string& service_name, uint64_t timeout_ms);

  template <typename Action>
  StatusOr<std::shared_ptr<ActionClient<Action>>> CreateActionClient(
      const std::string& action_name,
      ActionClientOptions options = ActionClientOptions());

  template <typename RequestSchema, typename ResponseSchema>
  StatusOr<std::shared_ptr<TypedClient<RequestSchema, ResponseSchema>>>
  CreateClientTyped(const std::string& service_name,
                    ClientOptions options = ClientOptions()) {
    auto status = detail::InjectTypedClientSchemaOptions<RequestSchema,
                                                         ResponseSchema>(
        &options);
    if (!status.ok()) {
      return StatusOr<std::shared_ptr<TypedClient<RequestSchema, ResponseSchema>>>(
          status);
    }

    auto client_result = CreateClient(service_name, options);
    if (!client_result.ok()) {
      return StatusOr<std::shared_ptr<TypedClient<RequestSchema, ResponseSchema>>>(
          client_result.status());
    }

    return StatusOr<std::shared_ptr<TypedClient<RequestSchema, ResponseSchema>>>(
        std::make_shared<TypedClient<RequestSchema, ResponseSchema>>(
            std::move(client_result).value(), name()));
  }

  template <typename RequestSchema, typename ResponseSchema>
  StatusOr<std::shared_ptr<Service>> CreateServiceTypedView(
      const std::string& service_name,
      std::function<void(const RequestSchema&,
                         TypedServiceResponder<ResponseSchema>)> callback,
      ServiceOptions options = ServiceOptions()) {
    auto sequence = std::make_shared<std::atomic<uint64_t>>(0);
    auto node_name = name();
    auto service_name_copy = service_name;
    auto bridge =
        [typed_cb = std::move(callback), sequence = std::move(sequence),
         node_name = std::move(node_name),
         service_name_copy = std::move(service_name_copy)](
            const MessageView& view, ServiceRequest&& req) mutable {
          if (!typed_cb) {
            return;
          }

          std::vector<uint8_t> owned_payload;
          const uint8_t* payload = nullptr;
          std::size_t payload_size = view.size;

          if (view.IsContiguous()) {
            payload = view.data;
          } else {
            owned_payload = view.ToVector();
            payload = owned_payload.empty() ? nullptr : owned_payload.data();
            payload_size = owned_payload.size();
          }

          auto decoded = detail::DecodeTypedPayload<RequestSchema>(
              payload, payload_size);
          if (!decoded.ok()) {
            detail::ReplyTypedDecodeFailure(req, service_name_copy,
                                            "typed service", decoded.status());
            return;
          }

          typed_cb(*decoded.value(),
                   TypedServiceResponder<ResponseSchema>(
                       std::move(req), node_name, sequence));
        };

    return CreateService<RequestSchema, ResponseSchema>(service_name,
                                                        std::move(bridge),
                                                        std::move(options));
  }

  template <typename RequestSchema, typename ResponseSchema>
  StatusOr<std::shared_ptr<Service>> CreateServiceTypedRetained(
      const std::string& service_name,
      std::function<void(
          TypedRetainedServiceRequest<RequestSchema, ResponseSchema>)> callback,
      ServiceOptions options = ServiceOptions()) {
    auto sequence = std::make_shared<std::atomic<uint64_t>>(0);
    auto node_name = name();
    auto service_name_copy = service_name;
    auto bridge =
        [typed_cb = std::move(callback), sequence = std::move(sequence),
         node_name = std::move(node_name),
         service_name_copy = std::move(service_name_copy)](
            const MessageView& view, ServiceRequest&& req) mutable {
          if (!typed_cb) {
            return;
          }

          std::vector<uint8_t> owned_payload = view.ToVector();
          auto decoded = detail::DecodeTypedPayload<RequestSchema>(
              owned_payload.data(), owned_payload.size());
          if (!decoded.ok()) {
            detail::ReplyTypedDecodeFailure(req, service_name_copy,
                                            "typed retained service",
                                            decoded.status());
            return;
          }

          typed_cb(TypedRetainedServiceRequest<RequestSchema, ResponseSchema>(
              std::move(owned_payload),
              TypedServiceResponder<ResponseSchema>(std::move(req), node_name,
                                                    sequence)));
        };

    return CreateService<RequestSchema, ResponseSchema>(service_name,
                                                        std::move(bridge),
                                                        std::move(options));
  }

  StatusOr<std::shared_ptr<Timer>> CreateTimer(std::chrono::nanoseconds period,
                                               Timer::Callback callback);
  StatusOr<std::shared_ptr<Timer>> CreateTimer(std::chrono::nanoseconds period,
                                               Timer::Callback callback,
                                               const TimerOptions& options);
  StatusOr<ExecutionGroup> CreateExecutionGroup(
      ExecutionPolicy policy = ExecutionPolicy::kSerialized);
  Status FreeExecutionGroup(CallbackGroupHandle handle);
  Status FreeExecutionGroup(ExecutionGroup& group) {
    return group.Release();
  }

  // Defined in contextstore.h; declared here so existing code that
  // includes aorta/node.h (most files) sees the convenience method.
  // Implementation lives in cpp/src/contextstore.cc to avoid making
  // node.h include contextstore.h transitively for everyone.
  StatusOr<std::shared_ptr<class ContextStore>> CreateContextStore();

  aorta_node_t handle() const { return handle_; }
  const std::string& name() const { return name_; }
  const std::string& id() const { return id_; }
  const std::string& group() const { return group_; }

 private:
  explicit Node(aorta_node_t handle, std::string name, std::string id,
                std::string group)
      : handle_(handle),
        name_(std::move(name)),
        id_(std::move(id)),
        group_(std::move(group)) {}

  Status EnsureExecutor();

  aorta_node_t handle_{0};
  std::string name_;
  std::string id_;
  std::string group_;
  mutable std::mutex executor_mutex_;
  std::shared_ptr<Executor> executor_;
  std::unique_ptr<std::thread> executor_thread_;
};

}  // namespace aorta
