// Copyright (c) 2025-2026 VitaDynamics
// Unit tests for Header building functions

#include "aorta/header.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <string>

namespace aorta {
namespace {

// Test HeaderValues default construction
TEST(HeaderValuesTest, DefaultConstruction) {
  HeaderValues values;
  EXPECT_EQ(values.publish_stamp_ns, 0u);
  EXPECT_TRUE(values.publisher_node.empty());
  EXPECT_EQ(values.sequence, 0u);
}

// Test HeaderValues construction with values
TEST(HeaderValuesTest, ConstructionWithValues) {
  HeaderValues values;
  values.publish_stamp_ns = 1234567890;
  values.publisher_node = "test_publisher";
  values.sequence = 42;

  EXPECT_EQ(values.publish_stamp_ns, 1234567890u);
  EXPECT_EQ(values.publisher_node, "test_publisher");
  EXPECT_EQ(values.sequence, 42u);
}

// Test HeaderValues copy semantics
TEST(HeaderValuesTest, CopySemantics) {
  HeaderValues original;
  original.publish_stamp_ns = 1000;
  original.publisher_node = "original_node";
  original.sequence = 1;

  HeaderValues copied = original;
  EXPECT_EQ(copied.publish_stamp_ns, 1000u);
  EXPECT_EQ(copied.publisher_node, "original_node");
  EXPECT_EQ(copied.sequence, 1u);

  // Modify original scalar should not affect copy
  original.publish_stamp_ns = 2000;
  EXPECT_EQ(copied.publish_stamp_ns, 1000u);
}

TEST(HeaderValuesTest, PublisherNodeOwnsAssignedString) {
  std::string source = "owned_node";
  HeaderValues values;
  values.publisher_node = source;

  EXPECT_EQ(values.publisher_node, "owned_node");
  EXPECT_NE(values.publisher_node.data(), source.data());

  source.assign("changed");
  EXPECT_EQ(values.publisher_node, "owned_node");
}

// Test HeaderValues move semantics.
// publisher_node is std::string; moved-from state is valid but unspecified
// per the standard, so we only assert the destination.
TEST(HeaderValuesTest, MoveSemantics) {
  HeaderValues original;
  original.publish_stamp_ns = 1000;
  original.publisher_node = "move_test";
  original.sequence = 1;

  HeaderValues moved = std::move(original);
  EXPECT_EQ(moved.publish_stamp_ns, 1000u);
  EXPECT_EQ(moved.publisher_node, "move_test");
  EXPECT_EQ(moved.sequence, 1u);
}

// Test HeaderValues with empty node name
TEST(HeaderValuesTest, EmptyNodeName) {
  HeaderValues values;
  values.publish_stamp_ns = 1234567890;
  values.publisher_node = "";
  values.sequence = 1;

  EXPECT_EQ(values.publish_stamp_ns, 1234567890u);
  EXPECT_TRUE(values.publisher_node.empty());
  EXPECT_EQ(values.sequence, 1u);
}

// Test HeaderValues with long node name
TEST(HeaderValuesTest, LongNodeName) {
  std::string long_name(256, 'a');
  HeaderValues values;
  values.publish_stamp_ns = 1234567890;
  values.publisher_node = long_name;
  values.sequence = 1;

  EXPECT_EQ(values.publisher_node.length(), 256u);
}

// Test HeaderValues with special characters in node name
TEST(HeaderValuesTest, SpecialCharactersInNodeName) {
  HeaderValues values;
  values.publisher_node = "node_name/with/slashes.and.dots";

  EXPECT_EQ(values.publisher_node, "node_name/with/slashes.and.dots");
}

// Test HeaderValues with max values
TEST(HeaderValuesTest, MaxValues) {
  HeaderValues values;
  values.publish_stamp_ns = UINT64_MAX;
  values.sequence = UINT64_MAX;

  EXPECT_EQ(values.publish_stamp_ns, UINT64_MAX);
  EXPECT_EQ(values.sequence, UINT64_MAX);
}

// Test HeaderValues with zero values
TEST(HeaderValuesTest, ZeroValues) {
  HeaderValues values;
  values.publish_stamp_ns = 0;
  values.publisher_node = "zero_test";
  values.sequence = 0;

  EXPECT_EQ(values.publish_stamp_ns, 0u);
  EXPECT_EQ(values.sequence, 0u);
}

// ==================== BuildAortaHeader Tests ====================

// Test building header with basic values
TEST(BuildAortaHeaderTest, BasicValues) {
  flatbuffers::FlatBufferBuilder builder(1024);

  HeaderValues values;
  values.publish_stamp_ns = 1234567890;
  values.publisher_node = "test_node";
  values.sequence = 42;

  auto header_offset = BuildAortaHeader(builder, values);
  builder.Finish(header_offset);

  // Verify we got a valid offset
  EXPECT_NE(header_offset.o, 0u);
}

// Test building header with empty node name
TEST(BuildAortaHeaderTest, EmptyNodeName) {
  flatbuffers::FlatBufferBuilder builder(1024);

  HeaderValues values;
  values.publish_stamp_ns = 1000;
  values.publisher_node = "";
  values.sequence = 1;

  auto header_offset = BuildAortaHeader(builder, values);
  builder.Finish(header_offset);

  EXPECT_NE(header_offset.o, 0u);
}

// Test building header with long node name
TEST(BuildAortaHeaderTest, LongNodeName) {
  flatbuffers::FlatBufferBuilder builder(1024);

  std::string long_name(100, 'x');
  HeaderValues values;
  values.publish_stamp_ns = 1000;
  values.publisher_node = long_name;
  values.sequence = 1;

  auto header_offset = BuildAortaHeader(builder, values);
  builder.Finish(header_offset);

  EXPECT_NE(header_offset.o, 0u);
}

// Test building header with zero timestamp
TEST(BuildAortaHeaderTest, ZeroTimestamp) {
  flatbuffers::FlatBufferBuilder builder(1024);

  HeaderValues values;
  values.publish_stamp_ns = 0;
  values.publisher_node = "zero_time";
  values.sequence = 1;

  auto header_offset = BuildAortaHeader(builder, values);
  builder.Finish(header_offset);

  EXPECT_NE(header_offset.o, 0u);
}

// Test building header with max timestamp
TEST(BuildAortaHeaderTest, MaxTimestamp) {
  flatbuffers::FlatBufferBuilder builder(1024);

  HeaderValues values;
  values.publish_stamp_ns = UINT64_MAX;
  values.publisher_node = "max_time";
  values.sequence = 1;

  auto header_offset = BuildAortaHeader(builder, values);
  builder.Finish(header_offset);

  EXPECT_NE(header_offset.o, 0u);
}

// Test building header with zero sequence
TEST(BuildAortaHeaderTest, ZeroSequence) {
  flatbuffers::FlatBufferBuilder builder(1024);

  HeaderValues values;
  values.publish_stamp_ns = 1000;
  values.publisher_node = "zero_seq";
  values.sequence = 0;

  auto header_offset = BuildAortaHeader(builder, values);
  builder.Finish(header_offset);

  EXPECT_NE(header_offset.o, 0u);
}

// Test building header with max sequence
TEST(BuildAortaHeaderTest, MaxSequence) {
  flatbuffers::FlatBufferBuilder builder(1024);

  HeaderValues values;
  values.publish_stamp_ns = 1000;
  values.publisher_node = "max_seq";
  values.sequence = UINT64_MAX;

  auto header_offset = BuildAortaHeader(builder, values);
  builder.Finish(header_offset);

  EXPECT_NE(header_offset.o, 0u);
}

// Test multiple header builds with same builder
TEST(BuildAortaHeaderTest, MultipleBuilds) {
  flatbuffers::FlatBufferBuilder builder(1024);

  HeaderValues values1;
  values1.publish_stamp_ns = 1000;
  values1.publisher_node = "node1";
  values1.sequence = 1;

  HeaderValues values2;
  values2.publish_stamp_ns = 2000;
  values2.publisher_node = "node2";
  values2.sequence = 2;

  auto offset1 = BuildAortaHeader(builder, values1);
  auto offset2 = BuildAortaHeader(builder, values2);

  // Both should succeed
  EXPECT_NE(offset1.o, 0u);
  EXPECT_NE(offset2.o, 0u);
}

// Test building header with special characters
TEST(BuildAortaHeaderTest, SpecialCharacters) {
  flatbuffers::FlatBufferBuilder builder(1024);

  HeaderValues values;
  values.publish_stamp_ns = 1000;
  values.publisher_node = "node/with/slashes:and:colons";
  values.sequence = 1;

  auto header_offset = BuildAortaHeader(builder, values);
  builder.Finish(header_offset);

  EXPECT_NE(header_offset.o, 0u);
}

// Test building header with unicode characters
TEST(BuildAortaHeaderTest, UnicodeCharacters) {
  flatbuffers::FlatBufferBuilder builder(1024);

  HeaderValues values;
  values.publish_stamp_ns = 1000;
  values.publisher_node = "节点_테스트_🤖";
  values.sequence = 1;

  auto header_offset = BuildAortaHeader(builder, values);
  builder.Finish(header_offset);

  EXPECT_NE(header_offset.o, 0u);
}

}  // namespace
}  // namespace aorta
