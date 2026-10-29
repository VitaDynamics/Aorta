#pragma once

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <string>
#include <utility>

#include <unistd.h>

#include "aorta/aorta.h"

namespace aorta {
namespace examples {
namespace detail {

constexpr int kActionExamplePort = 17447;

inline bool HasNonEmptyEnv(const char* name) {
  const char* value = std::getenv(name);
  return value != nullptr && value[0] != '\0';
}

inline Status InternalError(const std::string& message) {
  return Status(AORTA_ERR_INTERNAL, message);
}

inline Status ErrnoError(const std::string& message) {
  return InternalError(message + ": " + std::strerror(errno));
}

}  // namespace detail

enum class LocalZenohConfigMode {
  kActionServer,
  kActionClient,
};

inline const char* ConfigForMode(LocalZenohConfigMode mode) {
  switch (mode) {
    case LocalZenohConfigMode::kActionServer:
      return R"({
  mode: "peer",
  listen: {
    endpoints: ["tcp/127.0.0.1:17447"]
  },
  scouting: {
    multicast: { enabled: false },
    gossip: { enabled: false }
  },
  transport: {
    shared_memory: { enabled: false }
  }
}
)";
    case LocalZenohConfigMode::kActionClient:
      return R"({
  mode: "peer",
  connect: {
    endpoints: ["tcp/127.0.0.1:17447"]
  },
  listen: {
    endpoints: ["tcp/127.0.0.1:0"]
  },
  scouting: {
    multicast: { enabled: false },
    gossip: { enabled: false }
  },
  transport: {
    shared_memory: { enabled: false }
  }
}
)";
  }
  return "";
}

class ScopedLocalZenohConfig {
 public:
  ScopedLocalZenohConfig() = default;
  ScopedLocalZenohConfig(const ScopedLocalZenohConfig&) = delete;
  ScopedLocalZenohConfig& operator=(const ScopedLocalZenohConfig&) = delete;

  ScopedLocalZenohConfig(ScopedLocalZenohConfig&& other) noexcept
      : installed_(other.installed_), path_(std::move(other.path_)) {
    other.installed_ = false;
    other.path_.clear();
  }

  ScopedLocalZenohConfig& operator=(ScopedLocalZenohConfig&& other) noexcept {
    if (this == &other) {
      return *this;
    }
    Reset();
    installed_ = other.installed_;
    path_ = std::move(other.path_);
    other.installed_ = false;
    other.path_.clear();
    return *this;
  }

  ~ScopedLocalZenohConfig() { Reset(); }

  static StatusOr<ScopedLocalZenohConfig> InstallIfNeeded(
      LocalZenohConfigMode mode) {
    if (detail::HasNonEmptyEnv("ZENOH_SESSION_CONFIG_URI") ||
        detail::HasNonEmptyEnv("ZENOH_CONFIG")) {
      return ScopedLocalZenohConfig();
    }

    char path[] = "/tmp/aorta_cpp_example_zenoh_XXXXXX.json5";
    const int fd = ::mkstemps(path, 6);
    if (fd < 0) {
      return detail::ErrnoError("create local zenoh config temp file failed");
    }
    if (::close(fd) != 0) {
      const int saved_errno = errno;
      (void)::unlink(path);
      errno = saved_errno;
      return detail::ErrnoError("close local zenoh config temp file failed");
    }

    {
      std::ofstream out(path, std::ios::out | std::ios::trunc);
      if (!out.is_open()) {
        (void)::unlink(path);
        return detail::InternalError(
            "open local zenoh config temp file for writing failed");
      }
      out << ConfigForMode(mode);
      out.flush();
      if (!out.good()) {
        (void)::unlink(path);
        return detail::InternalError("write local zenoh config temp file failed");
      }
    }

    if (::setenv("ZENOH_SESSION_CONFIG_URI", path, 1) != 0) {
      const int saved_errno = errno;
      (void)::unlink(path);
      errno = saved_errno;
      return detail::ErrnoError("set ZENOH_SESSION_CONFIG_URI failed");
    }

    return ScopedLocalZenohConfig(std::string(path));
  }

  bool installed() const { return installed_; }
  const std::string& path() const { return path_; }

 private:
  explicit ScopedLocalZenohConfig(std::string path)
      : installed_(true), path_(std::move(path)) {}

  void Reset() {
    if (!installed_) {
      return;
    }
    (void)::unsetenv("ZENOH_SESSION_CONFIG_URI");
    (void)::unlink(path_.c_str());
    installed_ = false;
    path_.clear();
  }

  bool installed_ = false;
  std::string path_;
};

}  // namespace examples
}  // namespace aorta
