// Codegen golden: compile + round-trip proof for the C++ [bool] typed-build
// fix.
//
// Pre-fix, `all_types_ext`'s `flags:[bool]` builder emitted
// `builder.CreateVector(msg.flags.value().data(), ...)`, and
// `std::vector<bool>` is the bit-packed standard-library specialization with
// NO `.data()` member -> a hard compile bomb. The fix copies each element
// through an uninitialized `uint8_t` buffer. This test builds a distinguishable
// bool pattern (plus the union-member-alias `Chosen` and qualified-root_type
// schema) and decodes it via flatc's own raw accessors.
#include "all_types_ext_msg.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <gtest/gtest.h>

namespace {

using ::aorta::detail::typed_demo_ext_all_types_ext::BuildAllTypesExtTyped;
using ::demo::ext::AllTypesExtMsg;
using ::demo::ext::AltLeafMsg;

TEST(AllTypesExtRoundTrip, BoolVectorAndUnionAliasSurviveTypedBuild) {
  AllTypesExtMsg msg;
  // Distinguishable, non-uniform [bool] so a dropped bit or wrong element
  // width reads back wrong -- this is the field whose builder was a compile
  // bomb pre-fix.
  msg.flags = std::vector<bool>{true, false, true, true, false};
  msg.toggle = true;

  auto chosen = std::make_shared<AltLeafMsg>();
  chosen->id = 77u;
  msg.payload = chosen;  // union member alias `Chosen: AltLeaf`

  msg.levels = std::vector<::demo::ext::Level>{::demo::ext::Level_ONE, ::demo::ext::Level_TWO};
  msg.names = std::vector<std::string>{"alice", "bob"};

  flatbuffers::FlatBufferBuilder builder;
  auto offset = BuildAllTypesExtTyped(builder, msg, {});
  builder.Finish(offset);

  const auto* root = ::demo::ext::GetAllTypesExt(builder.GetBufferPointer());
  ASSERT_NE(root, nullptr);

  const auto* flags = root->flags();
  ASSERT_NE(flags, nullptr);
  ASSERT_EQ(flags->size(), 5u);
  EXPECT_EQ(flags->Get(0), 1);
  EXPECT_EQ(flags->Get(1), 0);
  EXPECT_EQ(flags->Get(2), 1);
  EXPECT_EQ(flags->Get(3), 1);
  EXPECT_EQ(flags->Get(4), 0);

  EXPECT_TRUE(root->toggle());

  // union alias: flatc keys the discriminant + `_as_Chosen()` accessor by the
  // ALIAS "Chosen" (verbatim, no snake_case), not the target bare name.
  ASSERT_EQ(root->payload_type(), ::demo::ext::AltPayload_Chosen);
  const auto* leaf = root->payload_as_Chosen();
  ASSERT_NE(leaf, nullptr);
  EXPECT_EQ(leaf->id(), 77u);

  ASSERT_NE(root->levels(), nullptr);
  ASSERT_EQ(root->levels()->size(), 2u);
  EXPECT_EQ(root->levels()->Get(0), ::demo::ext::Level_ONE);
  EXPECT_EQ(root->levels()->Get(1), ::demo::ext::Level_TWO);

  ASSERT_NE(root->names(), nullptr);
  ASSERT_EQ(root->names()->size(), 2u);
  EXPECT_EQ(root->names()->Get(0)->str(), "alice");
  EXPECT_EQ(root->names()->Get(1)->str(), "bob");
}

}  // namespace
