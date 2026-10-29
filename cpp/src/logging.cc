#include "aorta/logging.h"

#include <cstdio>

#include "internal/ffi.h"

namespace aorta {
namespace {

aorta_log_level_t ToFfiLogLevel(LogLevel level) {
  switch (level) {
    case LogLevel::kTrace:
      return AORTA_LOG_LEVEL_TRACE;
    case LogLevel::kDebug:
      return AORTA_LOG_LEVEL_DEBUG;
    case LogLevel::kInfo:
      return AORTA_LOG_LEVEL_INFO;
    case LogLevel::kWarn:
      return AORTA_LOG_LEVEL_WARN;
    case LogLevel::kError:
      return AORTA_LOG_LEVEL_ERROR;
  }
  return AORTA_LOG_LEVEL_WARN;
}

aorta_log_format_t ToFfiLogFormat(LogFormat format) {
  switch (format) {
    case LogFormat::kJson:
      return AORTA_LOG_FORMAT_JSON;
    case LogFormat::kText:
      return AORTA_LOG_FORMAT_TEXT;
  }
  return AORTA_LOG_FORMAT_TEXT;
}

const char* ResolveScope(const std::string& scope) {
  if (scope.empty()) {
    return "aorta_sdk";
  }
  return scope.c_str();
}

}  // namespace

Status InitLoggingFromEnv() {
  return Status::FromResult(aorta_log_init_from_env());
}

Status ConfigureLogging(const LogConfig& config) {
  aorta_log_config_t ffi{};
  ffi.filter = config.filter.empty() ? nullptr : config.filter.c_str();
  ffi.file_path = config.file_path.empty() ? nullptr : config.file_path.c_str();
  ffi.format = ToFfiLogFormat(config.format);
  ffi.stderr_enabled = config.stderr_enabled ? 1 : 0;
  return Status::FromResult(aorta_log_configure(&ffi));
}

Status FlushLogs() { return Status::FromResult(aorta_log_flush()); }

Status SetDefaultLogContext(const std::string& node_name,
                            const std::string& group) {
  return Status::FromResult(
      aorta_log_set_context(node_name.c_str(), group.c_str()));
}

bool IsLogEnabled(LogLevel level, const std::string& scope) {
  return aorta_log_enabled(ToFfiLogLevel(level), ResolveScope(scope)) != 0;
}

Status WriteLogAt(LogLevel level, const std::string& scope, const char* file,
                  uint32_t line, const std::string& message) {
  return Status::FromResult(aorta_log_write_at(ToFfiLogLevel(level),
                                               ResolveScope(scope), file, line,
                                               message.c_str()));
}

namespace detail {

bool ShouldLogEveryN(std::atomic<uint64_t>& counter, uint64_t n) {
  if (n == 0) {
    return true;
  }
  uint64_t current = counter.fetch_add(1, std::memory_order_relaxed) + 1;
  return ((current - 1) % n) == 0;
}

LogMessageBuilder::LogMessageBuilder(LogLevel level, const char* file,
                                     uint32_t line, const char* scope)
    : level_(level),
      file_(file == nullptr ? "" : file),
      line_(line),
      scope_(scope == nullptr || *scope == '\0' ? "aorta_sdk" : scope) {
  enabled_ = IsLogEnabled(level_, scope_);
}

LogMessageBuilder::~LogMessageBuilder() {
  if (!enabled_) {
    return;
  }
  const std::string message = stream_.str();
  Status status = WriteLogAt(level_, scope_, file_, line_, message);
  if (!status.ok()) {
    std::fprintf(stderr, "%s:%u %s\n", file_, line_, message.c_str());
  }
}

}  // namespace detail

}  // namespace aorta
