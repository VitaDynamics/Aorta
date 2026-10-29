// Codegen golden action: compile + round-trip proof, C++ side, for the
// shapes_probe golden action's struct / [struct] / union-with-alias / table /
// [table] / enum / [enum] rows. The Rust owned-root clone compile bombs are
// Rust-only; this C++ half proves the SAME golden schema compiles and
// round-trips in C++ (the "both languages" acceptance), through the
// typed_build helper and the action copy helper.
#include "shapes_probe_msg.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <gtest/gtest.h>

namespace {

using ::aorta::detail::typed_aorta_action_example_shapes_probe::BuildShapesProbeResultTyped;
using ::aorta::detail::typed_aorta_action_example_shapes_probe::CopyShapesProbeResultMsg;
using ::aorta::action::example::Grade_HIGH;
using ::aorta::action::example::Grade_LOW;
using ::aorta::action::example::Grade_UNKNOWN;
using ::aorta::action::example::LeafMsg;
using ::aorta::action::example::Point;
using ::aorta::action::example::ShapesProbeResult;
using ::aorta::action::example::ShapesProbeResultMsg;

ShapesProbeResultMsg MakeSentinelResult() {
  ShapesProbeResultMsg msg;
  msg.grade = Grade_HIGH;
  msg.origin = Point(1.0f, 2.0f, 3.0f);                                   // struct
  msg.path = std::vector<Point>{Point(4.0f, 5.0f, 6.0f), Point(7.0f, 8.0f, 9.0f)};  // [struct]
  msg.note = "done";
  msg.labels = std::vector<std::string>{"l1", "l2"};

  auto best = std::make_shared<LeafMsg>();
  best->id = 11u;
  best->label = "best-leaf";
  msg.best = best;                                                        // table

  auto leaf_a = std::make_shared<LeafMsg>();
  leaf_a->id = 12u;
  leaf_a->label = "leaf-a";
  auto leaf_b = std::make_shared<LeafMsg>();
  leaf_b->id = 13u;
  leaf_b->label = "leaf-b";
  msg.leaves = std::vector<std::shared_ptr<LeafMsg>>{leaf_a, leaf_b};     // [table]

  auto chosen = std::make_shared<LeafMsg>();
  chosen->id = 99u;
  chosen->label = "chosen";
  msg.choice = chosen;                                                    // union member alias

  msg.grades = std::vector<::aorta::action::example::Grade>{Grade_LOW, Grade_HIGH, Grade_UNKNOWN};
  msg.score = -7;                                                         // optional scalar (= null)
  msg.flags = std::vector<bool>{true, false, true};                       // [bool] via copy helper

  // `required` fields -- must be present (flatc rejects a missing required
  // field at buffer finish); the C++ copy helper reads them through the same
  // non-null-pointer path as optional reference fields.
  auto req_best = std::make_shared<LeafMsg>();
  req_best->id = 21u;
  req_best->label = "req-best";
  msg.req_point = Point(30.0f, 31.0f, 32.0f);                            // required struct
  msg.req_note = "req";                                                   // required string
  msg.req_best = req_best;                                                // required table
  msg.req_scores = std::vector<std::int32_t>{100, 200};                  // required [scalar]
  msg.req_tags = std::vector<std::string>{"t1", "t2"};                   // required [string]
  msg.req_marks = std::vector<Point>{Point(20.0f, 21.0f, 22.0f)};        // required [struct]
  msg.req_grades = std::vector<::aorta::action::example::Grade>{Grade_HIGH, Grade_LOW};  // required [enum]
  auto req_leaf = std::make_shared<LeafMsg>();
  req_leaf->id = 22u;
  req_leaf->label = "req-leaf";
  msg.req_leaves = std::vector<std::shared_ptr<LeafMsg>>{req_leaf};       // required [table]
  return msg;
}

TEST(ShapesProbeActionRoundTrip, StructAndStructVectorSurviveBuildAndCopy) {
  flatbuffers::FlatBufferBuilder builder;
  auto offset = BuildShapesProbeResultTyped(builder, MakeSentinelResult(), {});
  builder.Finish(offset);

  const auto* root =
      flatbuffers::GetRoot<ShapesProbeResult>(builder.GetBufferPointer());
  ASSERT_NE(root, nullptr);

  const ShapesProbeResultMsg copied = CopyShapesProbeResultMsg(*root);

  ASSERT_TRUE(copied.grade.has_value());
  EXPECT_EQ(*copied.grade, Grade_HIGH);

  // struct
  ASSERT_TRUE(copied.origin.has_value());
  EXPECT_FLOAT_EQ(copied.origin->x(), 1.0f);
  EXPECT_FLOAT_EQ(copied.origin->y(), 2.0f);
  EXPECT_FLOAT_EQ(copied.origin->z(), 3.0f);

  // [struct]
  ASSERT_TRUE(copied.path.has_value());
  ASSERT_EQ(copied.path->size(), 2u);
  EXPECT_FLOAT_EQ((*copied.path)[0].x(), 4.0f);
  EXPECT_FLOAT_EQ((*copied.path)[1].z(), 9.0f);

  ASSERT_TRUE(copied.note.has_value());
  EXPECT_EQ(*copied.note, "done");

  ASSERT_TRUE(copied.labels.has_value());
  ASSERT_EQ(copied.labels->size(), 2u);
  EXPECT_EQ((*copied.labels)[1], "l2");

  // table
  ASSERT_TRUE(copied.best.has_value());
  ASSERT_NE(*copied.best, nullptr);
  EXPECT_EQ(*(*copied.best)->id, 11u);
  EXPECT_EQ(*(*copied.best)->label, "best-leaf");

  // [table]
  ASSERT_TRUE(copied.leaves.has_value());
  ASSERT_EQ(copied.leaves->size(), 2u);
  EXPECT_EQ(*(*copied.leaves)[0]->id, 12u);
  EXPECT_EQ(*(*copied.leaves)[1]->label, "leaf-b");

  // union member alias (Chosen -> LeafMsg)
  ASSERT_TRUE(copied.choice.has_value());
  ASSERT_TRUE(std::holds_alternative<std::shared_ptr<LeafMsg>>(*copied.choice));
  const auto& chosen_leaf = std::get<std::shared_ptr<LeafMsg>>(*copied.choice);
  ASSERT_NE(chosen_leaf, nullptr);
  EXPECT_EQ(*chosen_leaf->id, 99u);
  EXPECT_EQ(*chosen_leaf->label, "chosen");

  // [enum]
  ASSERT_TRUE(copied.grades.has_value());
  ASSERT_EQ(copied.grades->size(), 3u);
  EXPECT_EQ((*copied.grades)[0], Grade_LOW);
  EXPECT_EQ((*copied.grades)[2], Grade_UNKNOWN);

  // optional scalar (= null): the copy helper reads flatc's
  // `flatbuffers::Optional<int32>` (== std::optional under C++17) straight into
  // the Msg's std::optional, so a present value survives.
  ASSERT_TRUE(copied.score.has_value());
  EXPECT_EQ(*copied.score, -7);

  // [bool] through the action copy helper: std::vector<bool> is the bit-packed
  // specialization, exercised end-to-end here (build + copy), not just topic
  // typed-build.
  ASSERT_TRUE(copied.flags.has_value());
  ASSERT_EQ(copied.flags->size(), 3u);
  EXPECT_TRUE((*copied.flags)[0]);
  EXPECT_FALSE((*copied.flags)[1]);
  EXPECT_TRUE((*copied.flags)[2]);

  // required fields survive the copy helper.
  ASSERT_TRUE(copied.req_point.has_value());
  EXPECT_FLOAT_EQ(copied.req_point->x(), 30.0f);
  EXPECT_FLOAT_EQ(copied.req_point->z(), 32.0f);
  ASSERT_TRUE(copied.req_note.has_value());
  EXPECT_EQ(*copied.req_note, "req");
  ASSERT_TRUE(copied.req_best.has_value());
  ASSERT_NE(*copied.req_best, nullptr);
  EXPECT_EQ(*(*copied.req_best)->id, 21u);
  ASSERT_TRUE(copied.req_scores.has_value());
  EXPECT_EQ(copied.req_scores->size(), 2u);
  EXPECT_EQ((*copied.req_scores)[1], 200);
  ASSERT_TRUE(copied.req_tags.has_value());
  EXPECT_EQ((*copied.req_tags)[0], "t1");
  ASSERT_TRUE(copied.req_marks.has_value());
  EXPECT_FLOAT_EQ((*copied.req_marks)[0].x(), 20.0f);
  ASSERT_TRUE(copied.req_grades.has_value());
  EXPECT_EQ((*copied.req_grades)[0], Grade_HIGH);
  ASSERT_TRUE(copied.req_leaves.has_value());
  ASSERT_EQ(copied.req_leaves->size(), 1u);
  EXPECT_EQ(*(*copied.req_leaves)[0]->label, "req-leaf");
}

TEST(ShapesProbeActionRoundTrip, OptionalScalarAbsenceStaysNullopt) {
  // A `= null` scalar left unset must round-trip as absent (nullopt), NOT as a
  // coerced default -- the copy helper preserves flatc's Optional emptiness.
  ShapesProbeResultMsg msg = MakeSentinelResult();
  msg.score = std::nullopt;
  msg.flags = std::nullopt;

  flatbuffers::FlatBufferBuilder builder;
  auto offset = BuildShapesProbeResultTyped(builder, msg, {});
  builder.Finish(offset);
  const auto* root =
      flatbuffers::GetRoot<ShapesProbeResult>(builder.GetBufferPointer());
  ASSERT_NE(root, nullptr);

  const ShapesProbeResultMsg copied = CopyShapesProbeResultMsg(*root);
  EXPECT_FALSE(copied.score.has_value());
  EXPECT_FALSE(copied.flags.has_value());
  // A non-optional field on the same table is unaffected.
  ASSERT_TRUE(copied.grade.has_value());
  EXPECT_EQ(*copied.grade, Grade_HIGH);
}

}  // namespace
