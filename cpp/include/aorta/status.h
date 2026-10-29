#pragma once

#include <aorta.h>

#include <cstdio>
#include <cstdlib>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

namespace aorta {

// [[nodiscard]]: silently dropping a Status discards the error outcome of the
// call that produced it (~14 sibling leaf types in this
// tree already carry the attribute; these two most load-bearing return types
// were the gap).
class [[nodiscard]] Status {
 public:
  Status() : code_(AORTA_OK) {}
  Status(aorta_result_t code, std::string message)
      : code_(code), message_(std::move(message)) {}

  bool ok() const { return code_ == AORTA_OK; }
  aorta_result_t code() const { return code_; }
  const std::string& message() const { return message_; }

  static Status Ok() { return Status(); }
  static Status FromResult(aorta_result_t code) {
    if (code == AORTA_OK) {
      return Status();
    }
    const char* msg = aorta_last_error();
    return Status(code, msg ? std::string(msg) : std::string());
  }

 private:
  aorta_result_t code_;
  std::string message_;
};

namespace detail {

[[noreturn]] inline void FatalStatusOrAccess(const Status& status) {
  if (status.message().empty()) {
    std::fprintf(stderr,
                 "Aorta fatal: StatusOr::value() called on error status "
                 "(code=%d)\n",
                 static_cast<int>(status.code()));
  } else {
    std::fprintf(stderr,
                 "Aorta fatal: StatusOr::value() called on error status "
                 "(code=%d, message=%s)\n",
                 static_cast<int>(status.code()), status.message().c_str());
  }
  std::fflush(stderr);
  std::abort();
}

}  // namespace detail

template <typename T>
class [[nodiscard]] StatusOr {
 public:
  StatusOr(Status status) : status_(std::move(status)) {}
  StatusOr(T value) : status_(Status::Ok()), value_(std::move(value)) {}

  bool ok() const { return status_.ok(); }
  explicit operator bool() const { return ok(); }
  const Status& status() const { return status_; }

  T& value() & {
    if (!ok()) {
      detail::FatalStatusOrAccess(status_);
    }
    return *value_;
  }
  const T& value() const& {
    if (!ok()) {
      detail::FatalStatusOrAccess(status_);
    }
    return *value_;
  }
  T&& value() && {
    if (!ok()) {
      detail::FatalStatusOrAccess(status_);
    }
    return std::move(*value_);
  }

  T& operator*() & { return value(); }
  const T& operator*() const& { return value(); }
  T&& operator*() && { return std::move(*this).value(); }

  T* operator->() { return &value(); }
  const T* operator->() const { return &value(); }

  template <typename U>
  T value_or(U&& default_value) const& {
    if (ok()) {
      return *value_;
    }
    return static_cast<T>(std::forward<U>(default_value));
  }

  template <typename U>
  T value_or(U&& default_value) && {
    if (ok()) {
      return std::move(*value_);
    }
    return static_cast<T>(std::forward<U>(default_value));
  }

 private:
  Status status_;
  std::optional<T> value_;
};

}  // namespace aorta
