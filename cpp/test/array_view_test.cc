// array_view_test.cc — Unit tests for aorta::ArrayView<T>.

#include "aorta/array_view.h"

#include <cstdint>
#include <optional>
#include <type_traits>
#include <vector>

#include "gtest/gtest.h"

namespace {

// ---------- basic construction / access ----------

TEST(ArrayViewTest, DefaultIsEmpty) {
  aorta::ArrayView<int> v;
  EXPECT_TRUE(v.empty());
  EXPECT_EQ(v.size(), 0u);
  EXPECT_EQ(v.data(), nullptr);
}

TEST(ArrayViewTest, FromPointerAndSize) {
  const uint8_t buf[] = {10, 20, 30};
  aorta::ArrayView<uint8_t> v(buf, 3);
  EXPECT_EQ(v.size(), 3u);
  EXPECT_EQ(v.data(), buf);
  EXPECT_EQ(v[0], 10);
  EXPECT_EQ(v[2], 30);
}

TEST(ArrayViewTest, FromLvalueVector) {
  std::vector<float> vec{1.0f, 2.0f, 3.0f};
  aorta::ArrayView<float> v = vec;  // implicit ctor
  EXPECT_EQ(v.size(), 3u);
  EXPECT_EQ(v.data(), vec.data());
  EXPECT_EQ(v[1], 2.0f);
}

TEST(ArrayViewTest, EmptyVector) {
  std::vector<int32_t> vec;
  aorta::ArrayView<int32_t> v(vec);
  EXPECT_TRUE(v.empty());
  EXPECT_EQ(v.size(), 0u);
}

// ---------- rvalue vector is deleted ----------

// This is a compile-time property; we verify the type trait.
TEST(ArrayViewTest, RvalueVectorCtorIsDeleted) {
  EXPECT_FALSE(
      (std::is_constructible_v<aorta::ArrayView<uint8_t>, std::vector<uint8_t>&&>));
}

// ---------- range-for ----------

TEST(ArrayViewTest, RangeFor) {
  const int arr[] = {1, 2, 3, 4};
  aorta::ArrayView<int> v(arr, 4);
  int sum = 0;
  for (int x : v) {
    sum += x;
  }
  EXPECT_EQ(sum, 10);
}

// ---------- optional<ArrayView> (the Msg field pattern) ----------

TEST(ArrayViewTest, OptionalAssignFromLvalueVector) {
  std::optional<aorta::ArrayView<uint8_t>> field;
  EXPECT_FALSE(field.has_value());

  std::vector<uint8_t> data{1, 2, 3, 4};
  field = data;  // implicit ctor through optional
  ASSERT_TRUE(field.has_value());
  EXPECT_EQ(field->size(), 4u);
  EXPECT_EQ(field->data(), data.data());
}

TEST(ArrayViewTest, OptionalAssignFromPointerSize) {
  const float buf[] = {0.5f, 1.5f};
  std::optional<aorta::ArrayView<float>> field;
  field = aorta::ArrayView<float>(buf, 2);
  ASSERT_TRUE(field.has_value());
  EXPECT_EQ(field->size(), 2u);
  EXPECT_EQ((*field)[0], 0.5f);
}

// Verify that optional<ArrayView<T>> cannot be assigned from a temporary
// vector.  We can't directly EXPECT a compile error, but we can verify
// the assignment operator would be ill-formed via type traits.
TEST(ArrayViewTest, OptionalCannotAssignFromRvalueVector) {
  // std::optional<ArrayView<T>>::operator=(U&&) requires
  // constructibility from U.  Since ArrayView(vector&&) is deleted,
  // is_constructible<ArrayView<uint8_t>, vector<uint8_t>&&> is false.
  // This means the implicit conversion in optional's assignment template
  // does not participate in overload resolution.
  EXPECT_FALSE(
      (std::is_constructible_v<aorta::ArrayView<uint8_t>, std::vector<uint8_t>&&>));
  EXPECT_TRUE(
      (std::is_constructible_v<aorta::ArrayView<uint8_t>, const std::vector<uint8_t>&>));
}

// ---------- trivially copyable view ----------

TEST(ArrayViewTest, IsCopyable) {
  const int arr[] = {42};
  aorta::ArrayView<int> a(arr, 1);
  aorta::ArrayView<int> b = a;
  EXPECT_EQ(b.data(), a.data());
  EXPECT_EQ(b.size(), a.size());
}

}  // namespace
