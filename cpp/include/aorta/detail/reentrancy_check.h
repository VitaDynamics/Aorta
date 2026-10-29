#pragma once

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include <aorta.h>

namespace aorta {
namespace detail {

enum class ReentrantMode { kOff, kWarn, kAbort };

inline ReentrantMode CurrentReentrantMode() {
  static const ReentrantMode mode = [] {
    const char* value = std::getenv("AORTA_REENTRANT_ASSERT");
    if (value != nullptr && std::strcmp(value, "1") == 0) {
      return ReentrantMode::kWarn;
    }
    if (value != nullptr && std::strcmp(value, "2") == 0) {
      return ReentrantMode::kAbort;
    }
    return ReentrantMode::kOff;
  }();
  return mode;
}

inline void HandleReentrantHit(const char* operation,
                               std::atomic<bool>& warned_once) {
  switch (CurrentReentrantMode()) {
    case ReentrantMode::kOff:
      return;
    case ReentrantMode::kWarn:
      if (!warned_once.exchange(true, std::memory_order_relaxed)) {
        std::fprintf(stderr, "[aorta] reentrancy guard: %s\n", operation);
        std::fflush(stderr);
      }
      return;
    case ReentrantMode::kAbort:
      std::fprintf(stderr, "Aorta fatal: reentrancy guard: %s\n", operation);
      std::fflush(stderr);
      std::abort();
  }
}

}  // namespace detail
}  // namespace aorta

#ifdef NDEBUG
#define AORTA_CHECK_NOT_REENTRANT(op) \
  do {                                \
  } while (0)
#else
#define AORTA_CHECK_NOT_REENTRANT(op)                               \
  do {                                                              \
    /* reentrancy: N (the detector call is the guard mechanism) */  \
    if (aorta_reentrant_context()) {                                \
      static std::atomic<bool> aorta_warned_once{false};            \
      ::aorta::detail::HandleReentrantHit(op, aorta_warned_once);    \
    }                                                               \
  } while (0)
#endif
