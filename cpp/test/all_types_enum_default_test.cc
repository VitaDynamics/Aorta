// Copyright (c) 2025-2026 VitaDynamics
// Behavior tests for typed enum/default/string handling (all_types fixture).

#include "all_types_msg.h"

#include <gtest/gtest.h>

namespace aorta {
namespace {

TEST(AllTypesEnumDefaultTest, MissingColorDecodesAsRed) {
  flatbuffers::FlatBufferBuilder builder;
  demo::AllTypesBuilder root_builder(builder);
  const auto root = root_builder.Finish();
  builder.Finish(root);

  const auto* decoded = demo::GetAllTypes(builder.GetBufferPointer());
  EXPECT_EQ(decoded->color(), demo::Color_RED);
}

TEST(AllTypesEnumDefaultTest, ExplicitColorRoundTrips) {
  flatbuffers::FlatBufferBuilder builder;
  demo::AllTypesBuilder root_builder(builder);
  root_builder.add_color(demo::Color_BLUE);
  const auto root = root_builder.Finish();
  builder.Finish(root);

  const auto* decoded = demo::GetAllTypes(builder.GetBufferPointer());
  EXPECT_EQ(decoded->color(), demo::Color_BLUE);
}

}  // namespace
}  // namespace aorta
