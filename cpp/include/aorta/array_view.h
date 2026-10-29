// array_view.h — Non-owning view over a contiguous array of T.
//
// Designed for use in generated Msg structs so that PublishTyped can
// serialize scalar-vector fields without copying the caller's data
// into an intermediate std::vector.
//
// Key properties:
//   - Implicit conversion from const std::vector<T>& (backward compat).
//   - Deleted rvalue-vector constructor to prevent dangling references
//     to temporaries.
//   - Trivially copyable (pointer + size), zero overhead.
//
// Requires C++17.  Intentionally avoids std::span (C++20) and
// absl::Span (not available in this project).

#pragma once

#include <cstddef>
#include <initializer_list>
#include <type_traits>
#include <vector>

namespace aorta {

/// Non-owning, read-only view over a contiguous array of `T`.
template <typename T>
class ArrayView {
  static_assert(std::is_trivially_copyable_v<T>,
                "ArrayView<T> requires a trivially-copyable element type.");

 public:
  // -- constructors ----------------------------------------------------------

  /// Default: empty view.
  constexpr ArrayView() noexcept = default;

  /// View over a raw pointer range.
  constexpr ArrayView(const T* data, std::size_t size) noexcept
      : data_(data), size_(size) {}

  /// Implicit conversion from a const lvalue std::vector<T>&.
  /// This allows callers that already hold a vector to keep using the same
  /// assignment syntax:  msg.bytes = my_vector;
  // NOLINTNEXTLINE(google-explicit-constructor)
  ArrayView(const std::vector<T>& v) noexcept
      : data_(v.data()), size_(v.size()) {}

  /// **Deleted** rvalue overload.  Prevents binding to a temporary vector,
  /// which would leave the view dangling once the temporary is destroyed.
  ///   msg.bytes = std::vector<uint8_t>{1,2,3};  // compile error
  ArrayView(std::vector<T>&& v) = delete;

  // -- element access --------------------------------------------------------

  constexpr const T* data() const noexcept { return data_; }
  constexpr std::size_t size() const noexcept { return size_; }
  constexpr bool empty() const noexcept { return size_ == 0; }
  constexpr const T& operator[](std::size_t i) const { return data_[i]; }

  // -- iterators (for range-for) ---------------------------------------------

  constexpr const T* begin() const noexcept { return data_; }
  constexpr const T* end() const noexcept { return data_ + size_; }

 private:
  const T* data_ = nullptr;
  std::size_t size_ = 0;
};

}  // namespace aorta
