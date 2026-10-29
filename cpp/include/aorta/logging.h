#pragma once

#include <atomic>
#include <cstdint>
#include <sstream>
#include <string>

#include "aorta/status.h"

namespace aorta {

enum class LogFormat {
  kText,
  kJson,
};

enum class LogLevel {
  kTrace,
  kDebug,
  kInfo,
  kWarn,
  kError,
};

struct LogConfig {
  std::string filter;
  std::string file_path;
  LogFormat format = LogFormat::kText;
  bool stderr_enabled = true;
};

Status InitLoggingFromEnv();
Status ConfigureLogging(const LogConfig& config);
Status FlushLogs();
Status SetDefaultLogContext(const std::string& node_name,
                            const std::string& group);
bool IsLogEnabled(LogLevel level, const std::string& scope = "aorta_sdk");
Status WriteLogAt(LogLevel level, const std::string& scope, const char* file,
                  uint32_t line, const std::string& message);

namespace detail {

bool ShouldLogEveryN(std::atomic<uint64_t>& counter, uint64_t n);

class LogMessageBuilder {
 public:
  LogMessageBuilder(LogLevel level, const char* file, uint32_t line,
                    const char* scope);
  ~LogMessageBuilder();

  LogMessageBuilder(const LogMessageBuilder&) = delete;
  LogMessageBuilder& operator=(const LogMessageBuilder&) = delete;
  LogMessageBuilder(LogMessageBuilder&&) = delete;
  LogMessageBuilder& operator=(LogMessageBuilder&&) = delete;

  template <typename T>
  LogMessageBuilder& operator<<(const T& value) {
    if (enabled_) {
      stream_ << value;
    }
    return *this;
  }

  LogMessageBuilder& operator<<(std::ostream& (*manipulator)(std::ostream&)) {
    if (enabled_) {
      manipulator(stream_);
    }
    return *this;
  }

 private:
  LogLevel level_;
  bool enabled_{false};
  const char* file_;
  uint32_t line_{0};
  std::string scope_;
  std::ostringstream stream_;
};

}  // namespace detail

}  // namespace aorta

#define AORTA_INTERNAL_LOG_LEVEL_TRACE ::aorta::LogLevel::kTrace
#define AORTA_INTERNAL_LOG_LEVEL_DEBUG ::aorta::LogLevel::kDebug
#define AORTA_INTERNAL_LOG_LEVEL_INFO ::aorta::LogLevel::kInfo
#define AORTA_INTERNAL_LOG_LEVEL_WARN ::aorta::LogLevel::kWarn
#define AORTA_INTERNAL_LOG_LEVEL_ERROR ::aorta::LogLevel::kError

#define AORTA_INTERNAL_CONCAT_INNER(a, b) a##b
#define AORTA_INTERNAL_CONCAT(a, b) AORTA_INTERNAL_CONCAT_INNER(a, b)

#define AORTA_LOG_SCOPE(level, scope)                                           \
  ::aorta::detail::LogMessageBuilder(                                           \
      AORTA_INTERNAL_LOG_LEVEL_##level, __FILE__,                               \
      static_cast<uint32_t>(__LINE__), (scope))

#define AORTA_LOG(level)                                                       \
  AORTA_LOG_SCOPE(level, "aorta_sdk")

#define AORTA_LOG_SCOPE_IF(level, scope, condition)                             \
  if (!(condition))                                                            \
  ;                                                                            \
  else                                                                         \
  AORTA_LOG_SCOPE(level, scope)

#define AORTA_LOG_IF(level, condition)                                         \
  AORTA_LOG_SCOPE_IF(level, "aorta_sdk", condition)

#define AORTA_LOG_SCOPE_EVERY_N(level, scope, n)                               \
  AORTA_LOG_SCOPE_IF(level, scope,                                             \
                     ([=]() {                                                   \
                       static std::atomic<uint64_t> counter{0};                \
                       return ::aorta::detail::ShouldLogEveryN(                \
                           counter, static_cast<uint64_t>(n));                 \
                     }()))

#define AORTA_LOG_EVERY_N(level, n)                                            \
  AORTA_LOG_SCOPE_EVERY_N(level, "aorta_sdk", n)
