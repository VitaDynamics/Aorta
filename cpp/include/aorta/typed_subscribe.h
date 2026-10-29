#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "aorta/flatbuffers.h"
#include "aorta/types.h"

namespace aorta {

template <typename T>
struct TypedSubscribeTraits {
  static bool Verify(const uint8_t* data, std::size_t size) {
    if (data == nullptr || size == 0) {
      return false;
    }
    flatbuffers::Verifier verifier(data, size);
    return verifier.VerifyBuffer<T>(nullptr);
  }

  static const T* GetRoot(const uint8_t* data) {
    if (data == nullptr) {
      return nullptr;
    }
    return flatbuffers::GetRoot<T>(data);
  }
};

template <typename T>
class TypedRetainedSample {
 public:
  const T* Message() const { return message_; }
  const MessageContext& Context() const { return context_; }

  const uint8_t* Data() const { return PayloadData(); }
  std::size_t Size() const { return size_; }

  bool IsOwned() const { return !owned_payload_.empty(); }

 private:
  friend class Node;

  TypedRetainedSample(const uint8_t* borrowed_data, std::size_t size,
                      MessageContext context, std::vector<uint8_t>&& owned_payload)
      : borrowed_data_(borrowed_data),
        size_(size),
        context_(std::move(context)),
        owned_payload_(std::move(owned_payload)) {
    RefreshMessagePointer();
  }

  const uint8_t* PayloadData() const {
    if (!owned_payload_.empty()) {
      return owned_payload_.data();
    }
    return borrowed_data_;
  }

  void RefreshMessagePointer() {
    const uint8_t* data = PayloadData();
    if (data == nullptr || size_ == 0) {
      message_ = nullptr;
      return;
    }
    message_ = TypedSubscribeTraits<T>::GetRoot(data);
  }

  const uint8_t* borrowed_data_ = nullptr;
  std::size_t size_ = 0;
  MessageContext context_;
  std::vector<uint8_t> owned_payload_;
  const T* message_ = nullptr;
};

template <typename T>
using TypedRetainedSamplePtr = std::shared_ptr<const TypedRetainedSample<T>>;

}  // namespace aorta
