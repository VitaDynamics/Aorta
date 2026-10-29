#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "aorta/client.h"
#include "aorta/flatbuffers.h"
#include "aorta/header.h"
#include "aorta/service.h"
#include "aorta/status.h"
#include "aorta/time.h"
#include "aorta/typed_publish.h"
#include "aorta/typed_subscribe.h"

namespace aorta {

namespace detail {

inline uint64_t TypedPublishTimestampNs() {
  auto now = NowNs();
  if (!now.ok()) {
    return 0;
  }
  return static_cast<uint64_t>(now.value());
}

template <typename T>
std::vector<uint8_t> BuildTypedPayload(
    const std::string& publisher_node, uint64_t sequence,
    const typename TypedPublishTraits<T>::Msg& msg) {
  static_assert(
      detail::HasTypedPublishTraits<T>::value,
      "TypedPublishTraits<T> specialization is missing. "
      "Include the generated message wrapper header (for example *_msg.h).");
  static_assert(
      TypedPublishTraits<T>::kHasAortaHeader,
      "Typed RPC requires request/response tables with 'aorta_header'.");

  flatbuffers::FlatBufferBuilder builder;
  auto node = builder.CreateString(publisher_node);
  auto header = aorta::sys::CreateAortaHeader(builder, TypedPublishTimestampNs(),
                                              node, sequence);
  auto root = TypedPublishTraits<T>::Build(builder, header, msg);
  builder.Finish(root);
  return std::vector<uint8_t>(builder.GetBufferPointer(),
                              builder.GetBufferPointer() + builder.GetSize());
}

template <typename T>
StatusOr<const T*> DecodeTypedPayload(const uint8_t* data, std::size_t size) {
  if (!TypedSubscribeTraits<T>::Verify(data, size)) {
    return StatusOr<const T*>(Status(
        AORTA_ERR_INVALID_ARGUMENT,
        "typed payload failed schema verification"));
  }
  const T* root = TypedSubscribeTraits<T>::GetRoot(data);
  if (root == nullptr) {
    return StatusOr<const T*>(Status(
        AORTA_ERR_INVALID_ARGUMENT,
        "typed payload root decode returned null"));
  }
  return StatusOr<const T*>(root);
}

}  // namespace detail

template <typename Response>
class TypedServiceResponder {
 public:
  TypedServiceResponder(ServiceRequest&& request, std::string node_name,
                        std::shared_ptr<std::atomic<uint64_t>> sequence)
      : request_(std::move(request)),
        node_name_(std::move(node_name)),
        sequence_(std::move(sequence)) {}

  TypedServiceResponder(TypedServiceResponder&&) noexcept = default;
  TypedServiceResponder& operator=(TypedServiceResponder&&) noexcept = default;
  TypedServiceResponder(const TypedServiceResponder&) = delete;
  TypedServiceResponder& operator=(const TypedServiceResponder&) = delete;

  template <typename FillFn>
  Status Reply(FillFn&& fill) {
    typename TypedPublishTraits<Response>::Msg msg{};
    std::forward<FillFn>(fill)(msg);
    return ReplyMessage(msg);
  }

  Status ReplyMessage(const typename TypedPublishTraits<Response>::Msg& msg) {
    const uint64_t sequence =
        sequence_ ? sequence_->fetch_add(1, std::memory_order_relaxed) : 0;
    return request_.Reply(
        detail::BuildTypedPayload<Response>(node_name_, sequence, msg));
  }

  Status ReplyBytes(std::vector<uint8_t>&& response) {
    return request_.Reply(std::move(response));
  }

  Status ReplyError(const std::string& error_msg) {
    return request_.ReplyError(error_msg);
  }

  bool IsValid() const { return request_.IsValid(); }
  ServiceRequest& Raw() { return request_; }

 private:
  ServiceRequest request_;
  std::string node_name_;
  std::shared_ptr<std::atomic<uint64_t>> sequence_;
};

template <typename Request, typename Response>
class TypedRetainedServiceRequest {
 public:
  TypedRetainedServiceRequest(std::vector<uint8_t>&& payload,
                              TypedServiceResponder<Response>&& responder)
      : payload_(std::move(payload)),
        message_(TypedSubscribeTraits<Request>::GetRoot(payload_.data())),
        responder_(std::move(responder)) {}

  TypedRetainedServiceRequest(TypedRetainedServiceRequest&&) noexcept = default;
  TypedRetainedServiceRequest& operator=(TypedRetainedServiceRequest&&) noexcept =
      default;
  TypedRetainedServiceRequest(const TypedRetainedServiceRequest&) = delete;
  TypedRetainedServiceRequest& operator=(const TypedRetainedServiceRequest&) =
      delete;

  const Request* Message() const { return message_; }
  const uint8_t* Data() const { return payload_.data(); }
  std::size_t Size() const { return payload_.size(); }

  template <typename FillFn>
  Status Reply(FillFn&& fill) {
    return responder_.Reply(std::forward<FillFn>(fill));
  }

  Status ReplyMessage(const typename TypedPublishTraits<Response>::Msg& msg) {
    return responder_.ReplyMessage(msg);
  }

  Status ReplyBytes(std::vector<uint8_t>&& response) {
    return responder_.ReplyBytes(std::move(response));
  }

  Status ReplyError(const std::string& error_msg) {
    return responder_.ReplyError(error_msg);
  }

  TypedServiceResponder<Response>& RawResponder() { return responder_; }

 private:
  std::vector<uint8_t> payload_;
  const Request* message_ = nullptr;
  TypedServiceResponder<Response> responder_;
};

template <typename Response>
class TypedClientResponse {
 public:
  explicit TypedClientResponse(ClientResponse&& response)
      : response_(std::move(response)) {
    auto view = response_.View();
    if (!view.IsContiguous()) {
      owned_payload_ = view.ToVector();
    }
    RefreshMessagePointer();
  }

  TypedClientResponse(TypedClientResponse&& other) noexcept
      : response_(std::move(other.response_)),
        owned_payload_(std::move(other.owned_payload_)) {
    RefreshMessagePointer();
  }
  TypedClientResponse& operator=(TypedClientResponse&& other) noexcept {
    if (this != &other) {
      response_ = std::move(other.response_);
      owned_payload_ = std::move(other.owned_payload_);
      RefreshMessagePointer();
    }
    return *this;
  }
  TypedClientResponse(const TypedClientResponse&) = delete;
  TypedClientResponse& operator=(const TypedClientResponse&) = delete;

  const Response* Message() const { return message_; }
  MessageView View() const { return response_.View(); }
  std::vector<uint8_t> ToVector() const { return response_.ToVector(); }
  const ClientResponse& Raw() const { return response_; }

 private:
  void RefreshMessagePointer() {
    if (!owned_payload_.empty()) {
      message_ = TypedSubscribeTraits<Response>::GetRoot(owned_payload_.data());
      return;
    }

    auto view = response_.View();
    if (view.IsContiguous() && view.data != nullptr && view.size > 0) {
      message_ = TypedSubscribeTraits<Response>::GetRoot(view.data);
    } else {
      message_ = nullptr;
    }
  }

  ClientResponse response_;
  std::vector<uint8_t> owned_payload_;
  const Response* message_ = nullptr;
};

template <typename Request, typename Response>
class TypedClient {
 public:
  TypedClient(std::shared_ptr<Client> inner, std::string node_name)
      : inner_(std::move(inner)), node_name_(std::move(node_name)) {}

  template <typename FillFn>
  StatusOr<TypedClientResponse<Response>> Call(FillFn&& fill) const {
    typename TypedPublishTraits<Request>::Msg msg{};
    std::forward<FillFn>(fill)(msg);
    return CallMessage(msg);
  }

  StatusOr<TypedClientResponse<Response>> CallMessage(
      const typename TypedPublishTraits<Request>::Msg& msg) const {
    const uint64_t sequence = sequence_.fetch_add(1, std::memory_order_relaxed);
    auto call_result = inner_->Call(
        detail::BuildTypedPayload<Request>(node_name_, sequence, msg));
    if (!call_result.ok()) {
      return StatusOr<TypedClientResponse<Response>>(call_result.status());
    }

    auto response = std::move(call_result).value();
    auto view = response.View();
    const uint8_t* data = nullptr;
    std::size_t size = view.size;
    std::vector<uint8_t> owned_payload;
    if (view.IsContiguous()) {
      data = view.data;
    } else {
      owned_payload = view.ToVector();
      data = owned_payload.data();
      size = owned_payload.size();
    }

    auto decode = detail::DecodeTypedPayload<Response>(data, size);
    if (!decode.ok()) {
      return StatusOr<TypedClientResponse<Response>>(decode.status());
    }

    return StatusOr<TypedClientResponse<Response>>(
        TypedClientResponse<Response>(std::move(response)));
  }

  const Client& Raw() const { return *inner_; }

 private:
  std::shared_ptr<Client> inner_;
  std::string node_name_;
  mutable std::atomic<uint64_t> sequence_{0};
};

}  // namespace aorta
