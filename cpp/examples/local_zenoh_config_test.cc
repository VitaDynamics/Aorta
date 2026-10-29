#include "local_zenoh_config.h"

#include <cstdlib>
#include <fstream>
#include <iterator>
#include <string>

#include <gtest/gtest.h>

#include <unistd.h>

namespace aorta::examples {
namespace {

class ScopedEnvVar {
 public:
  explicit ScopedEnvVar(const char* name) : name_(name) {
    const char* value = std::getenv(name_);
    if (value != nullptr) {
      had_value_ = true;
      value_ = value;
    }
  }

  ~ScopedEnvVar() {
    if (had_value_) {
      (void)::setenv(name_, value_.c_str(), 1);
    } else {
      (void)::unsetenv(name_);
    }
  }

 private:
  const char* name_;
  bool had_value_ = false;
  std::string value_;
};

bool FileExists(const std::string& path) {
  return ::access(path.c_str(), F_OK) == 0;
}

TEST(LocalZenohConfigTest, InstallIfNeededCreatesServerConfigWhenEnvUnset) {
  ScopedEnvVar session_config("ZENOH_SESSION_CONFIG_URI");
  ScopedEnvVar zenoh_config("ZENOH_CONFIG");
  (void)::unsetenv("ZENOH_SESSION_CONFIG_URI");
  (void)::unsetenv("ZENOH_CONFIG");

  std::string installed_path;
  {
    auto config_or = ScopedLocalZenohConfig::InstallIfNeeded(
        LocalZenohConfigMode::kActionServer);
    ASSERT_TRUE(config_or.ok()) << config_or.status().message();
    auto config = std::move(config_or).value();
    ASSERT_TRUE(config.installed());
    installed_path = config.path();
    EXPECT_FALSE(installed_path.empty());
    EXPECT_TRUE(FileExists(installed_path));

    const char* session_config_env = std::getenv("ZENOH_SESSION_CONFIG_URI");
    ASSERT_NE(session_config_env, nullptr);
    EXPECT_EQ(std::string(session_config_env), installed_path);

    std::ifstream input(installed_path);
    ASSERT_TRUE(input.is_open());
    const std::string content((std::istreambuf_iterator<char>(input)),
                              std::istreambuf_iterator<char>());
    EXPECT_NE(content.find("mode: \"peer\""), std::string::npos);
    EXPECT_NE(content.find("tcp/127.0.0.1:17447"), std::string::npos);
    EXPECT_NE(content.find("multicast: { enabled: false }"),
              std::string::npos);
    EXPECT_NE(content.find("gossip: { enabled: false }"), std::string::npos);
    EXPECT_NE(content.find("shared_memory: { enabled: false }"),
              std::string::npos);
  }

  EXPECT_FALSE(FileExists(installed_path));
  EXPECT_EQ(std::getenv("ZENOH_SESSION_CONFIG_URI"), nullptr);
}

TEST(LocalZenohConfigTest, InstallIfNeededCreatesClientConfigWhenEnvUnset) {
  ScopedEnvVar session_config("ZENOH_SESSION_CONFIG_URI");
  ScopedEnvVar zenoh_config("ZENOH_CONFIG");
  (void)::unsetenv("ZENOH_SESSION_CONFIG_URI");
  (void)::unsetenv("ZENOH_CONFIG");

  std::string installed_path;
  {
    auto config_or = ScopedLocalZenohConfig::InstallIfNeeded(
        LocalZenohConfigMode::kActionClient);
    ASSERT_TRUE(config_or.ok()) << config_or.status().message();
    auto config = std::move(config_or).value();
    ASSERT_TRUE(config.installed());
    installed_path = config.path();
    EXPECT_TRUE(FileExists(installed_path));

    std::ifstream input(installed_path);
    ASSERT_TRUE(input.is_open());
    const std::string content((std::istreambuf_iterator<char>(input)),
                              std::istreambuf_iterator<char>());
    EXPECT_NE(content.find("connect"), std::string::npos);
    EXPECT_NE(content.find("tcp/127.0.0.1:17447"), std::string::npos);
    EXPECT_NE(content.find("tcp/127.0.0.1:0"), std::string::npos);
    EXPECT_NE(content.find("multicast: { enabled: false }"),
              std::string::npos);
    EXPECT_NE(content.find("gossip: { enabled: false }"), std::string::npos);
    EXPECT_NE(content.find("shared_memory: { enabled: false }"),
              std::string::npos);
  }

  EXPECT_FALSE(FileExists(installed_path));
  EXPECT_EQ(std::getenv("ZENOH_SESSION_CONFIG_URI"), nullptr);
}

TEST(LocalZenohConfigTest, InstallIfNeededDoesNotOverrideExplicitConfigEnv) {
  ScopedEnvVar session_config("ZENOH_SESSION_CONFIG_URI");
  ScopedEnvVar zenoh_config("ZENOH_CONFIG");
  (void)::unsetenv("ZENOH_CONFIG");
  ASSERT_EQ(::setenv("ZENOH_SESSION_CONFIG_URI", "/tmp/custom-zenoh.json5", 1),
            0);

  auto config_or = ScopedLocalZenohConfig::InstallIfNeeded(
      LocalZenohConfigMode::kActionServer);
  ASSERT_TRUE(config_or.ok()) << config_or.status().message();
  auto config = std::move(config_or).value();
  EXPECT_FALSE(config.installed());
  const char* session_config_env = std::getenv("ZENOH_SESSION_CONFIG_URI");
  ASSERT_NE(session_config_env, nullptr);
  EXPECT_EQ(std::string(session_config_env), "/tmp/custom-zenoh.json5");
}

}  // namespace
}  // namespace aorta::examples
