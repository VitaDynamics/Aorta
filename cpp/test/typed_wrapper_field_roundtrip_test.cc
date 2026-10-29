#include "all_types_msg.h"

#include <string>

#include <gtest/gtest.h>

namespace {

const demo::AllTypes* RoundTrip(const demo::AllTypesMsg& msg,
                                flatbuffers::FlatBufferBuilder* builder) {
  const auto node = builder->CreateString("typed_wrapper_field_roundtrip_test");
  const auto header =
      aorta::sys::CreateAortaHeader(*builder, 1U, node, 1U);
  const auto root = aorta::TypedPublishTraits<demo::AllTypes>::Build(
      *builder, header, msg);
  builder->Finish(root);
  if (!aorta::TypedSubscribeTraits<demo::AllTypes>::Verify(
          builder->GetBufferPointer(), builder->GetSize())) {
    return nullptr;
  }
  return aorta::TypedSubscribeTraits<demo::AllTypes>::GetRoot(
      builder->GetBufferPointer());
}

TEST(TypedWrapperFieldRoundTrip, ValuesSurviveTypedBuild) {
  demo::AllTypesMsg msg;
  msg.text = "fixture-text";
  msg.color = demo::Color_BLUE;
  msg.val_int32 = -42;
  msg.flag = true;

  flatbuffers::FlatBufferBuilder builder;
  const auto* root = RoundTrip(msg, &builder);
  ASSERT_NE(root, nullptr);
  ASSERT_NE(root->text(), nullptr);
  EXPECT_EQ(root->text()->str(), "fixture-text");
  EXPECT_EQ(root->color(), demo::Color_BLUE);
  EXPECT_EQ(root->val_int32(), -42);
  EXPECT_TRUE(root->flag());
  EXPECT_NE(root->aorta_header(), nullptr);
}

TEST(TypedWrapperFieldRoundTrip, UnsetStringStaysAbsent) {
  demo::AllTypesMsg msg;
  flatbuffers::FlatBufferBuilder builder;
  const auto* root = RoundTrip(msg, &builder);
  ASSERT_NE(root, nullptr);
  EXPECT_EQ(root->text(), nullptr);
  EXPECT_EQ(root->color(), demo::Color_RED);
}

TEST(TypedWrapperFieldRoundTrip, EmptyStringIsPresentButEmpty) {
  demo::AllTypesMsg msg;
  msg.text = "";
  flatbuffers::FlatBufferBuilder builder;
  const auto* root = RoundTrip(msg, &builder);
  ASSERT_NE(root, nullptr);
  ASSERT_NE(root->text(), nullptr);
  EXPECT_TRUE(root->text()->str().empty());
}

}  // namespace
