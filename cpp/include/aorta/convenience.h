#pragma once

#include <cstdio>
#include <cstdlib>
#include <exception>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

#include "aorta/status.h"

namespace aorta {

// Exception thrown by `Must` / `CheckOk` / `detail::FatalStatus` when an
// Aorta operation fails. Carries the underlying Status so callers that
// wrap with `RunOrFatal` (or their own try/catch) can route the failure
// without losing the error code. Throwing (instead of
// the old `std::abort()` path) lets stack RAII unwind through live
// `aorta::Node` / `Publisher` / `Subscriber` / `Service` handles so they
// get a clean `aorta_*_free` call on the way out — peers see a graceful
// undeclare instead of a TCP RST.
class FatalError : public std::exception {
 public:
  FatalError(Status status, std::string_view what = {})
      : status_(std::move(status)), what_msg_(FormatWhat(status_, what)) {}

  const char* what() const noexcept override { return what_msg_.c_str(); }
  const Status& status() const noexcept { return status_; }

 private:
  static std::string FormatWhat(const Status& s, std::string_view w) {
    std::string out("Aorta fatal: ");
    if (w.empty()) {
      out += "operation failed";
    } else {
      out.append(w.data(), w.size());
      out += " failed";
    }
    out += " (code=";
    out += std::to_string(static_cast<int>(s.code()));
    if (!s.message().empty()) {
      out += ", message=";
      out += s.message();
    }
    out += ")";
    return out;
  }

  Status status_;
  std::string what_msg_;
};

namespace detail {

// Print the formatted diagnostic to stderr THEN throw a FatalError carrying
// the same message. The stderr print preserves the old `Must`/`CheckOk`
// surface for callers that have not adopted `RunOrFatal` (the terminate
// handler that fires on an uncaught FatalError emits less-readable text).
[[noreturn]] inline void FatalStatus(const Status& status,
                                     std::string_view what = {}) {
  FatalError error(status, what);
  std::fprintf(stderr, "%s\n", error.what());
  std::fflush(stderr);
  // ODR prohibition (one exception mode per program, see cpp/README.md
  // D1): this
  // inline function's body differs by `__cpp_exceptions`, so mixing
  // -fno-exceptions and exceptions-enabled TUs that both include this
  // header in one program is unsupported — the linker may keep either
  // TU's definition. Pick ONE exception mode for the whole binary.
#if defined(__cpp_exceptions)
  throw error;
#else
  // Baseline C++17 SDK under -fno-exceptions (D1 inclusion sweep): the
  // diagnostic already printed; a fatal is a fatal.
  std::abort();
#endif
}

}  // namespace detail

inline void CheckOk(const Status& status, std::string_view what = {}) {
  if (!status.ok()) {
    detail::FatalStatus(status, what);
  }
}

template <typename T>
T Must(StatusOr<T>&& result, std::string_view what = {}) {
  if (!result.ok()) {
    detail::FatalStatus(result.status(), what);
  }
  return std::move(result).value();
}

// Wrap an `int main()` body (or any nullary callable) so a thrown
// `FatalError` unwinds the stack — running every RAII destructor for live
// Aorta handles — before the process exits. Returns the callable's result
// for void/int returns; on caught exception returns `EXIT_FAILURE`. Marked
// `noexcept` so the wrapper itself never escapes a throw past `main`.
//
// Usage:
//   int main(int argc, char* argv[]) {
//     return aorta::RunOrFatal([&]() -> int {
//       auto node = aorta::Must(aorta::Node::Create("listener"));
//       /* … */
//       return 0;
//     });
//   }
template <typename F>
int RunOrFatal(F&& fn) noexcept {
  // ODR prohibition (one exception mode per program, see cpp/README.md
  // D1): this
  // inline function's body (this #if/#else pair AND its matching #if
  // below around the catch clauses) differs by `__cpp_exceptions`, so
  // mixing -fno-exceptions and exceptions-enabled TUs that both include
  // this header in one program is unsupported — the linker may keep
  // either TU's definition. Pick ONE exception mode for the whole
  // binary.
#if defined(__cpp_exceptions)
  try {
#endif
    using R = std::invoke_result_t<F>;
    if constexpr (std::is_void_v<R>) {
      std::forward<F>(fn)();
      return 0;
    } else {
      return static_cast<int>(std::forward<F>(fn)());
    }
#if defined(__cpp_exceptions)
  } catch (const FatalError&) {
    // Diagnostic already printed by detail::FatalStatus; stack has unwound
    // through RAII so Aorta handles got their `aorta_*_free` call.
    return EXIT_FAILURE;
  } catch (const std::exception& e) {
    std::fprintf(stderr, "Aorta fatal: uncaught std::exception: %s\n",
                 e.what());
    std::fflush(stderr);
    return EXIT_FAILURE;
  } catch (...) {
    std::fprintf(stderr, "Aorta fatal: uncaught non-std exception\n");
    std::fflush(stderr);
    return EXIT_FAILURE;
  }
#endif  // -fno-exceptions (D1 inclusion sweep): FatalStatus aborts after
        // printing, so there is nothing to catch — the body runs bare.
}

}  // namespace aorta
