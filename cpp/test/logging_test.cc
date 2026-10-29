// Copyright (c) 2025-2026 VitaDynamics
// Unit tests for C++ logging API wrappers

#include "aorta/logging.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <string>

#include "mock_aorta.h"

namespace aorta {
namespace {

using ::testing::_;
using ::testing::Ge;
using ::testing::HasSubstr;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::StrEq;

TEST(LoggingTest, InitFromEnvDelegatesToFfi) {
  MockAortaFFI mock;
  EXPECT_CALL(mock, aorta_log_init_from_env()).WillOnce(Return(AORTA_OK));

  Status status = InitLoggingFromEnv();
  EXPECT_TRUE(status.ok());
}

TEST(LoggingTest, ConfigureMapsFieldsToFfiConfig) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_log_configure(_))
      .WillOnce(Invoke([](const aorta_log_config_t* cfg) {
        EXPECT_NE(cfg, nullptr);
        EXPECT_NE(cfg->filter, nullptr);
        EXPECT_NE(cfg->file_path, nullptr);
        EXPECT_STREQ(cfg->filter, "aorta_core=info,zenoh=warn");
        EXPECT_STREQ(cfg->file_path, "/tmp/aorta.log");
        EXPECT_EQ(cfg->format, AORTA_LOG_FORMAT_JSON);
        EXPECT_EQ(cfg->stderr_enabled, 0);
        return AORTA_OK;
      }));

  LogConfig cfg;
  cfg.filter = "aorta_core=info,zenoh=warn";
  cfg.file_path = "/tmp/aorta.log";
  cfg.format = LogFormat::kJson;
  cfg.stderr_enabled = false;

  Status status = ConfigureLogging(cfg);
  EXPECT_TRUE(status.ok());
}

TEST(LoggingTest, ConfigureUsesNullForEmptyStrings) {
  MockAortaFFI mock;

  EXPECT_CALL(mock, aorta_log_configure(_))
      .WillOnce(Invoke([](const aorta_log_config_t* cfg) {
        EXPECT_NE(cfg, nullptr);
        EXPECT_EQ(cfg->filter, nullptr);
        EXPECT_EQ(cfg->file_path, nullptr);
        EXPECT_EQ(cfg->format, AORTA_LOG_FORMAT_TEXT);
        EXPECT_EQ(cfg->stderr_enabled, 1);
        return AORTA_OK;
      }));

  LogConfig cfg;
  Status status = ConfigureLogging(cfg);
  EXPECT_TRUE(status.ok());
}

TEST(LoggingTest, SetDefaultLogContextDelegatesToFfi) {
  MockAortaFFI mock;
  EXPECT_CALL(mock, aorta_log_set_context(StrEq("node_a"), StrEq("group_a")))
      .WillOnce(Return(AORTA_OK));

  Status status = SetDefaultLogContext("node_a", "group_a");
  EXPECT_TRUE(status.ok());
}

TEST(LoggingTest, IsLogEnabledDelegatesToFfi) {
  MockAortaFFI mock;
  EXPECT_CALL(mock, aorta_log_enabled(AORTA_LOG_LEVEL_INFO, StrEq("aorta_sdk")))
      .WillOnce(Return(1));

  EXPECT_TRUE(IsLogEnabled(LogLevel::kInfo, "aorta_sdk"));
}

TEST(LoggingTest, WriteLogAtDelegatesToFfi) {
  MockAortaFFI mock;
  EXPECT_CALL(mock, aorta_log_write_at(AORTA_LOG_LEVEL_WARN, StrEq("aorta_sdk"),
                                       StrEq("x.cc"), 19, StrEq("warn msg")))
      .WillOnce(Return(AORTA_OK));

  Status status = WriteLogAt(LogLevel::kWarn, "aorta_sdk", "x.cc", 19, "warn msg");
  EXPECT_TRUE(status.ok());
}

TEST(LoggingTest, LogMacroWritesAtCallsite) {
  MockAortaFFI mock;
  EXPECT_CALL(mock, aorta_log_enabled(AORTA_LOG_LEVEL_INFO, StrEq("aorta_sdk")))
      .WillOnce(Return(1));
  EXPECT_CALL(mock, aorta_log_write_at(AORTA_LOG_LEVEL_INFO, StrEq("aorta_sdk"),
                                       HasSubstr("logging_test.cc"), Ge(1u),
                                       StrEq("hello-42")))
      .WillOnce(Return(AORTA_OK));

  AORTA_LOG(INFO) << "hello-" << 42;
}

TEST(LoggingTest, LogScopeMacroUsesProvidedScope) {
  MockAortaFFI mock;
  EXPECT_CALL(mock, aorta_log_enabled(AORTA_LOG_LEVEL_WARN, StrEq("demo.scope")))
      .WillOnce(Return(1));
  EXPECT_CALL(mock, aorta_log_write_at(AORTA_LOG_LEVEL_WARN, StrEq("demo.scope"),
                                       HasSubstr("logging_test.cc"), Ge(1u),
                                       StrEq("scoped-warn")))
      .WillOnce(Return(AORTA_OK));

  AORTA_LOG_SCOPE(WARN, "demo.scope") << "scoped-warn";
}

TEST(LoggingTest, LogIfSkipsWhenConditionFalse) {
  MockAortaFFI mock;
  EXPECT_CALL(mock, aorta_log_enabled(_, _)).Times(0);
  EXPECT_CALL(mock, aorta_log_write_at(_, _, _, _, _)).Times(0);

  AORTA_LOG_IF(INFO, false) << "should-not-log";
}

TEST(LoggingTest, LogEveryNUsesCounter) {
  MockAortaFFI mock;
  EXPECT_CALL(mock, aorta_log_enabled(AORTA_LOG_LEVEL_INFO, StrEq("aorta_sdk")))
      .Times(2)
      .WillRepeatedly(Return(1));
  EXPECT_CALL(mock, aorta_log_write_at(AORTA_LOG_LEVEL_INFO, StrEq("aorta_sdk"),
                                       HasSubstr("logging_test.cc"), Ge(1u), _))
      .Times(2)
      .WillRepeatedly(Return(AORTA_OK));

  for (int i = 0; i < 3; ++i) {
    AORTA_LOG_EVERY_N(INFO, 2) << "n=2 at i=" << i;
  }
}

TEST(LoggingTest, LogEveryNCanBeUsedInIfElseControlFlow) {
  MockAortaFFI mock;
  EXPECT_CALL(mock, aorta_log_enabled(_, _)).Times(0);
  EXPECT_CALL(mock, aorta_log_write_at(_, _, _, _, _)).Times(0);

  const bool should_log = false;
  if (should_log)
    AORTA_LOG_EVERY_N(INFO, 2) << "unreachable";
  else
    SUCCEED();
}

TEST(LoggingTest, FlushDelegatesToFfi) {
  MockAortaFFI mock;
  EXPECT_CALL(mock, aorta_log_flush()).WillOnce(Return(AORTA_OK));

  Status status = FlushLogs();
  EXPECT_TRUE(status.ok());
}

}  // namespace
}  // namespace aorta
