// C++ UUIDv7 integration test — real Core, no mock.
//
// `cpp/test/uuid_test.cc` (unit tier) only exercises `NewUuidV7()`'s error
// propagation against `MockAortaFFI`, so it never generates a real UUIDv7 and
// cannot confirm the RFC 9562 version/variant bits the Rust
// (`core/src/uuid_v7.rs`) and Python (`python/tests/test_uuid_v7.py`) sides
// already verify against their real implementations. This closes that gap by
// linking `//core:aorta_core_ffi` directly, matching the
// `qos_preset_integration_test` pattern: a pure function, no node/network
// fixture required.
//
// Lives in cpp_integration_tests (not cpp_unit_tests) for the same reason as
// qos_preset_integration_test: it pulls //core:aorta_core_ffi, which the
// sanitizer unit tier cannot splice without the cargo-bazel timeout.

#include "aorta/uuid.h"

#include <cctype>
#include <string>

#include <gtest/gtest.h>

namespace aorta {
namespace {

// Manual RFC 9562 structural check (canonical 8-4-4-4-12 lowercase hex, '-'
// at the fixed positions, version nibble '7', variant nibble in {8,9,a,b}) —
// mirrors the assertions `core/src/uuid_v7.rs` and `test_uuid_v7.py` already
// make against the real generator on their respective sides.
void ExpectCanonicalUuidV7(const std::string &text) {
  ASSERT_EQ(text.size(), 36u);
  for (size_t i : {8u, 13u, 18u, 23u}) {
    ASSERT_EQ(text[i], '-') << "text=" << text;
  }
  for (size_t i = 0; i < text.size(); ++i) {
    if (i == 8 || i == 13 || i == 18 || i == 23) {
      continue;
    }
    const unsigned char c = static_cast<unsigned char>(text[i]);
    ASSERT_TRUE(std::isxdigit(c) && (!std::isalpha(c) || std::islower(c)))
        << "non-canonical hex digit '" << text[i] << "' in " << text;
  }
  EXPECT_EQ(text[14], '7') << "version nibble; text=" << text;
  EXPECT_TRUE(text[19] == '8' || text[19] == '9' || text[19] == 'a' ||
              text[19] == 'b')
      << "variant nibble; text=" << text;
}

TEST(UuidIntegrationTest, GeneratesCanonicalRfc9562UuidV7) {
  auto first = NewUuidV7();
  ASSERT_TRUE(first.ok()) << first.status().message();
  ExpectCanonicalUuidV7(*first);
}

TEST(UuidIntegrationTest, ConsecutiveCallsAreDistinct) {
  auto first = NewUuidV7();
  auto second = NewUuidV7();
  ASSERT_TRUE(first.ok()) << first.status().message();
  ASSERT_TRUE(second.ok()) << second.status().message();
  EXPECT_NE(*first, *second);
}

} // namespace
} // namespace aorta
