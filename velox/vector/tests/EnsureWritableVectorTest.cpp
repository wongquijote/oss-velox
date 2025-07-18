/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <gtest/gtest.h>
#include "velox/vector/ComplexVector.h"
#include "velox/vector/tests/VectorTestUtils.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::test;

class EnsureWritableVectorTest : public testing::Test, public VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }
};

TEST_F(EnsureWritableVectorTest, flat) {
  SelectivityVector rows(1'000);

  VectorPtr result;
  BaseVector::ensureWritable(rows, BIGINT(), pool(), result);
  ASSERT_TRUE(result);
  ASSERT_EQ(rows.size(), result->size());
  ASSERT_EQ(TypeKind::BIGINT, result->typeKind());
  ASSERT_EQ(VectorEncoding::Simple::FLAT, result->encoding());

  auto* flatResult = result->asFlatVector<int64_t>();
  for (vector_size_t i = 0; i < rows.size(); i++) {
    if (i % 5 == 0) {
      flatResult->setNull(i, true);
    } else {
      flatResult->set(i, i);
    }
  }

  // Singly referenced vector with singly referenced buffers should be reused
  // as-is
  auto* rawNulls = flatResult->nulls().get();
  auto* rawValues = flatResult->values().get();
  BaseVector::ensureWritable(rows, BIGINT(), pool(), result);
  ASSERT_EQ(flatResult, result.get());
  ASSERT_EQ(rawNulls, flatResult->nulls().get());
  ASSERT_EQ(rawValues, flatResult->values().get());

  // Resize upwards singly-referenced vector with singly referenced buffers
  rows.resize(2'000);
  BaseVector::ensureWritable(rows, BIGINT(), pool(), result);
  ASSERT_EQ(rows.size(), result->size());
  ASSERT_EQ(flatResult, result.get());
  rawNulls = flatResult->nulls().get();
  rawValues = flatResult->values().get();

  for (vector_size_t i = 0; i < rows.size(); i++) {
    if (i % 5 == 0) {
      flatResult->setNull(i, true);
    } else {
      flatResult->set(i, i);
    }
  }

  // Resize downwards
  rows.resize(1'024);
  BaseVector::ensureWritable(rows, BIGINT(), pool(), result);
  ASSERT_EQ(2'000, result->size());
  ASSERT_EQ(rawNulls, flatResult->nulls().get());
  ASSERT_EQ(rawValues, flatResult->values().get());

  // Add second reference to the vector -> new vector should be allocated
  auto resultCopy = result;

  BaseVector::ensureWritable(rows, BIGINT(), pool(), result);
  ASSERT_NE(flatResult, result.get());
  flatResult = result->asFlatVector<int64_t>();
  ASSERT_NE(rawNulls, flatResult->nulls().get());
  ASSERT_NE(rawValues, flatResult->values().get());
  rawNulls = flatResult->nulls().get();
  rawValues = flatResult->values().get();
  for (vector_size_t i = 0; i < rows.size(); i++) {
    if (i % 7 == 0) {
      flatResult->setNull(i, true);
    } else {
      flatResult->set(i, i * 2);
    }
  }

  // Make sure resultCopy hasn't changed
  auto* flatResultCopy = resultCopy->asFlatVector<int64_t>();
  for (vector_size_t i = 0; i < rows.size(); i++) {
    if (i % 5 == 0) {
      ASSERT_TRUE(flatResultCopy->isNullAt(i));
    } else {
      ASSERT_FALSE(flatResultCopy->isNullAt(i));
      ASSERT_EQ(i, flatResultCopy->valueAt(i));
    }
  }

  // Remove second reference to the vector. Add a reference to nulls buffer.
  // Verify that vector is reused, but new nulls buffer is allocated.
  resultCopy.reset();
  auto nullsCopy = result->nulls();

  BaseVector::ensureWritable(rows, BIGINT(), pool(), result);
  ASSERT_EQ(flatResult, result.get());
  ASSERT_NE(rawNulls, flatResult->nulls().get());
  rawNulls = flatResult->nulls().get();
  ASSERT_EQ(rawValues, flatResult->values().get());
  for (vector_size_t i = 0; i < rows.size(); i++) {
    if (i % 11 == 0) {
      flatResult->setNull(i, true);
    } else {
      flatResult->set(i, i * 3);
    }
  }

  // Make sure nullsCopy hasn't changed
  auto* rawNullsCopy = nullsCopy->as<uint64_t>();
  for (vector_size_t i = 0; i < rows.size(); i++) {
    ASSERT_EQ(i % 7 == 0, bits::isBitNull(rawNullsCopy, i));
  }

  // Add a reference to values buffer
  auto valuesCopy = result->values();

  BaseVector::ensureWritable(rows, BIGINT(), pool(), result);
  ASSERT_EQ(flatResult, result.get());
  ASSERT_EQ(rawNulls, flatResult->nulls().get());
  ASSERT_NE(rawValues, flatResult->values().get());
  for (vector_size_t i = 0; i < rows.size(); i++) {
    if (i % 13 == 0) {
      flatResult->setNull(i, true);
    } else {
      flatResult->set(i, i * 4);
    }
  }

  // Make sure valuesCopy hasn't changed
  auto* rawValuesCopy = valuesCopy->as<int64_t>();
  for (vector_size_t i = 0; i < rows.size(); i++) {
    if (i % 11 != 0) {
      ASSERT_EQ(i * 3, rawValuesCopy[i]);
    }
  }
}

TEST_F(EnsureWritableVectorTest, flatStrings) {
  SelectivityVector rows(1'000);

  VectorPtr result;
  BaseVector::ensureWritable(rows, VARCHAR(), pool(), result);
  ASSERT_TRUE(result);
  ASSERT_EQ(rows.size(), result->size());
  ASSERT_EQ(TypeKind::VARCHAR, result->typeKind());
  ASSERT_EQ(VectorEncoding::Simple::FLAT, result->encoding());

  for (auto i = 0; i < result->size(); ++i) {
    ASSERT_EQ("", result->asFlatVector<StringView>()->valueAt(i).str());
  }

  // Add a reference to values buffer. Expect ensureWritable to make a new
  // buffer.
  auto valuesCopy = result->values();

  BaseVector::ensureWritable(rows, BIGINT(), pool(), result);
  ASSERT_TRUE(result);
  ASSERT_EQ(rows.size(), result->size());
  ASSERT_EQ(TypeKind::VARCHAR, result->typeKind());
  ASSERT_EQ(VectorEncoding::Simple::FLAT, result->encoding());

  ASSERT_NE(valuesCopy->as<StringView>(), result->values()->as<StringView>());
  for (auto i = 0; i < result->size(); ++i) {
    ASSERT_EQ("", result->asFlatVector<StringView>()->valueAt(i).str());
  }
}

namespace {
SelectivityVector selectOddRows(vector_size_t size) {
  SelectivityVector oddRows(size);
  for (vector_size_t i = 0; i < size; i += 2) {
    oddRows.setValid(i, false);
  }
  oddRows.updateBounds();
  return oddRows;
}

template <typename IsNullAt>
void assertEqualOffsetsOrSizes(
    const BufferPtr& expected,
    const BufferPtr& actual,
    vector_size_t size,
    IsNullAt isNullAt) {
  auto rawExpected = expected->as<vector_size_t>();
  auto rawActual = actual->as<vector_size_t>();
  for (vector_size_t i = 0; i < size; i++) {
    if (!isNullAt(i)) {
      EXPECT_EQ(rawExpected[i], rawActual[i]) << "at " << i;
    }
  }
}

std::function<bool(vector_size_t /*row*/)> isNullAt(const VectorPtr& v) {
  return [&](vector_size_t row) { return v->isNullAt(row); };
}

template <typename T>
struct VectorPointersBase {
  void initialize(const VectorPtr& vector) {
    rawVector = vector->as<T>();
    rawNulls = rawVector->nulls().get();
    rawOffsets = rawVector->offsets().get();
    rawSizes = rawVector->sizes().get();

    nullsUnique = true;
    offsetsUnique = true;
    sizesUnique = true;
  }

  void setNullsUnique(bool unique) {
    nullsUnique = unique;
  }

  void setOffsetsUnique(bool unique) {
    offsetsUnique = unique;
  }

  void setSizesUnique(bool unique) {
    sizesUnique = unique;
  }

  void assertMutable(const VectorPtr& vector) {
    ASSERT_EQ(vector.use_count(), 1);

    auto* typedVector = vector->as<T>();
    ASSERT_EQ(nullsUnique, typedVector->nulls()->isMutable());
    ASSERT_EQ(offsetsUnique, typedVector->offsets()->isMutable());
    ASSERT_EQ(sizesUnique, typedVector->sizes()->isMutable());
  }

  void assertPointers(const VectorPtr& vector) {
    ASSERT_EQ(rawVector, vector.get());

    auto* typedVector = vector->as<T>();

    if (nullsUnique) {
      ASSERT_EQ(rawNulls, typedVector->nulls().get());
    } else {
      ASSERT_NE(rawNulls, typedVector->nulls().get());
    }

    if (offsetsUnique) {
      ASSERT_EQ(rawOffsets, typedVector->offsets().get());
    } else {
      ASSERT_NE(rawOffsets, typedVector->offsets().get());
    }

    if (sizesUnique) {
      ASSERT_EQ(rawSizes, typedVector->sizes().get());
    } else {
      ASSERT_NE(rawSizes, typedVector->sizes().get());
    }
  }

  T* rawVector;
  Buffer* rawNulls;
  Buffer* rawOffsets;
  Buffer* rawSizes;

  bool arrayUnique;
  bool nullsUnique;
  bool offsetsUnique;
  bool sizesUnique;
};

struct ArrayVectorPointers : public VectorPointersBase<ArrayVector> {
  void initialize(const VectorPtr& vector) {
    VectorPointersBase<ArrayVector>::initialize(vector);
    rawElements = rawVector->elements().get();
    elementsUnique = true;
  }

  void setElementsUnique(bool unique) {
    elementsUnique = unique;
  }

  void assertMutable(const VectorPtr& vector) {
    VectorPointersBase<ArrayVector>::assertMutable(vector);

    auto* arrayVector = vector->as<ArrayVector>();
    ASSERT_EQ(
        elementsUnique, BaseVector::isVectorWritable(arrayVector->elements()));
  }

  void assertPointers(const VectorPtr& vector) {
    VectorPointersBase<ArrayVector>::assertPointers(vector);

    auto* arrayVector = vector->as<ArrayVector>();
    if (elementsUnique) {
      ASSERT_EQ(rawElements, arrayVector->elements().get());
    } else {
      ASSERT_NE(rawElements, arrayVector->elements().get());
    }
  }

  BaseVector* rawElements;
  bool elementsUnique;
};

struct MapVectorPointers : public VectorPointersBase<MapVector> {
  void initialize(const VectorPtr& vector) {
    VectorPointersBase<MapVector>::initialize(vector);
    rawKeys = rawVector->mapKeys().get();
    rawValues = rawVector->mapValues().get();
    keysUnique = true;
    valuesUnique = true;
  }

  void setKeysUnique(bool unique) {
    keysUnique = unique;
  }

  void setValuesUnique(bool unique) {
    valuesUnique = unique;
  }

  void assertMutable(const VectorPtr& vector) {
    VectorPointersBase<MapVector>::assertMutable(vector);

    auto* mapVector = vector->as<MapVector>();
    ASSERT_EQ(keysUnique, BaseVector::isVectorWritable(mapVector->mapKeys()));
    ASSERT_EQ(
        valuesUnique, BaseVector::isVectorWritable(mapVector->mapValues()));
  }

  void assertPointers(const VectorPtr& vector) {
    VectorPointersBase<MapVector>::assertPointers(vector);

    auto* mapVector = vector->as<MapVector>();
    if (keysUnique) {
      ASSERT_EQ(rawKeys, mapVector->mapKeys().get());
    } else {
      ASSERT_NE(rawKeys, mapVector->mapKeys().get());
    }

    if (valuesUnique) {
      ASSERT_EQ(rawValues, mapVector->mapValues().get());
    } else {
      ASSERT_NE(rawValues, mapVector->mapValues().get());
    }
  }

  BaseVector* rawKeys;
  BaseVector* rawValues;
  bool keysUnique;
  bool valuesUnique;
};
} // namespace

TEST_F(EnsureWritableVectorTest, dictionary) {
  vector_size_t dictionarySize = 100;
  vector_size_t size = 1'000;
  auto dictionary = BaseVector::create(BIGINT(), dictionarySize, pool());
  for (vector_size_t i = 0; i < dictionarySize; i++) {
    if (i % 5 == 0) {
      dictionary->setNull(i, true);
    } else {
      dictionary->asFlatVector<int64_t>()->set(i, i * 2);
    }
  }

  BufferPtr indices = AlignedBuffer::allocate<vector_size_t>(size, pool());
  auto* rawIndices = indices->asMutable<vector_size_t>();
  for (vector_size_t i = 0; i < size; i++) {
    rawIndices[i] = i % dictionarySize;
  }

  auto result = BaseVector::wrapInDictionary(
      BufferPtr(nullptr), indices, size, dictionary);

  auto oddRows = selectOddRows(size);
  BaseVector::ensureWritable(oddRows, BIGINT(), pool(), result);
  ASSERT_EQ(size, result->size());
  ASSERT_EQ(TypeKind::BIGINT, result->typeKind());
  ASSERT_EQ(VectorEncoding::Simple::FLAT, result->encoding());

  // Verify that values in even rows were copied over
  auto* flatResult = result->asFlatVector<int64_t>();
  for (vector_size_t i = 0; i < size; i += 2) {
    auto index = i % dictionarySize;
    if (index % 5 == 0) {
      ASSERT_TRUE(result->isNullAt(i)) << "at " << i;
    } else {
      ASSERT_EQ(index * 2, flatResult->valueAt(i)) << "at " << i;
    }
  }
}

TEST_F(EnsureWritableVectorTest, constant) {
  // Check that the flattened vector has the correct size.
  {
    const vector_size_t size = 100;
    auto constant = BaseVector::createConstant(
        BIGINT(), variant::create<TypeKind::BIGINT>(123), size, pool());
    BaseVector::ensureWritable(
        SelectivityVector::empty(), BIGINT(), pool(), constant);
    EXPECT_EQ(VectorEncoding::Simple::FLAT, constant->encoding());
    EXPECT_EQ(size, constant->size());
  }

  // If constant has smaller size, check that we follow the selectivity vector
  // max seleced row size.
  {
    const vector_size_t selectivityVectorSize = 100;
    auto constant = BaseVector::createConstant(
        BIGINT(), variant::create<TypeKind::BIGINT>(123), 1, pool());
    SelectivityVector rows(selectivityVectorSize);
    rows.setValid(99, false);
    rows.updateBounds();
    BaseVector::ensureWritable(rows, BIGINT(), pool(), constant);
    EXPECT_EQ(VectorEncoding::Simple::FLAT, constant->encoding());
    EXPECT_EQ(99, constant->size());
  }

  // If constant has larger size, check that we follow the constant vector
  // size.
  {
    const vector_size_t selectivityVectorSize = 100;
    const vector_size_t constantVectorSize = 200;
    auto constant = BaseVector::createConstant(
        BIGINT(),
        variant::create<TypeKind::BIGINT>(123),
        constantVectorSize,
        pool());
    BaseVector::ensureWritable(
        SelectivityVector::empty(selectivityVectorSize),
        BIGINT(),
        pool(),
        constant);
    EXPECT_EQ(VectorEncoding::Simple::FLAT, constant->encoding());
    EXPECT_EQ(constantVectorSize, constant->size());
  }
}

struct MockBufferViewReleaser {
  void addRef() {}
  void release() {}
};
using MockBufferView = BufferView<MockBufferViewReleaser&>;
MockBufferViewReleaser releaser;

TEST_F(EnsureWritableVectorTest, array) {
  vector_size_t size = 1'000;
  auto a = makeArrayVector<int32_t>(
      size,
      /*sizeAt*/ [](vector_size_t row) { return row % 5; },
      /*valueAt*/ [](vector_size_t row) { return row; },
      /*isNullAt*/ test::VectorMaker::nullEvery(7));

  auto b = makeArrayVector<int32_t>(
      size,
      /*sizeAt*/ [](vector_size_t row) { return row % 7; },
      /*valueAt*/ [](vector_size_t row) { return row * 2; },
      /*isNullAt*/ test::VectorMaker::nullEvery(11));

  SelectivityVector rows(size);
  VectorPtr result;
  BaseVector::ensureWritable(rows, ARRAY(INTEGER()), pool(), result);
  ASSERT_EQ(size, result->size());
  ASSERT_TRUE(ARRAY(INTEGER())->kindEquals(result->type()));
  ASSERT_EQ(VectorEncoding::Simple::ARRAY, result->encoding());

  result->copy(a.get(), rows, nullptr);

  // Multiply-referenced vector.
  auto resultCopy = result;
  ASSERT_FALSE(result.use_count() == 1);

  auto oddRows = selectOddRows(size);
  BaseVector::ensureWritable(oddRows, result->type(), pool(), result);
  ASSERT_TRUE(result.use_count() == 1);
  ASSERT_NE(resultCopy.get(), result.get());

  // Verify that even rows were copied over.
  for (vector_size_t i = 0; i < size; i += 2) {
    ASSERT_TRUE(a->equalValueAt(result.get(), i, i));
  }

  // Modify odd rows and verify that resultCopy is not affected.
  result->copy(b.get(), oddRows, nullptr);

  for (vector_size_t i = 0; i < size; i++) {
    auto expected = i % 2 == 0 ? a : b;
    ASSERT_TRUE(expected->equalValueAt(result.get(), i, i));

    ASSERT_TRUE(a->equalValueAt(resultCopy.get(), i, i));
  }

  // Singly referenced array vector; multiply-referenced elements vector.
  result = BaseVector::create(result->type(), rows.size(), pool());
  result->copy(a.get(), rows, nullptr);

  ArrayVectorPointers pointers;
  pointers.initialize(result);
  auto elementsCopy = result->as<ArrayVector>()->elements();
  pointers.setElementsUnique(false);
  pointers.assertMutable(result);

  BaseVector::ensureWritable(oddRows, ARRAY(INTEGER()), pool(), result);
  pointers.assertPointers(result);

  // Verify that even rows were copied over.
  for (vector_size_t i = 0; i < size; i += 2) {
    ASSERT_TRUE(a->equalValueAt(result.get(), i, i)) << "at " << i;
  }

  result->copy(b.get(), oddRows, nullptr);

  // Verify that elementsCopy is not modified.
  for (vector_size_t i = 0; i < size; i++) {
    ArrayVectorPtr expected = (i % 2 == 0) ? a : b;
    ASSERT_TRUE(expected->equalValueAt(result.get(), i, i)) << "at " << i;
  }

  for (vector_size_t i = 0; i < a->elements()->size(); i++) {
    ASSERT_TRUE(a->elements()->equalValueAt(elementsCopy.get(), i, i))
        << "at " << i;
  }

  // Singly referenced array vector; multiply-referenced offsets buffer.
  result = BaseVector::create(result->type(), rows.size(), pool());
  result->copy(a.get(), rows, nullptr);

  pointers.initialize(result);
  auto offsetsCopy = result->as<ArrayVector>()->offsets();
  pointers.setOffsetsUnique(false);
  pointers.assertMutable(result);

  BaseVector::ensureWritable(oddRows, result->type(), pool(), result);
  pointers.assertPointers(result);

  result->copy(b.get(), oddRows, nullptr);

  // Verify offsetsCopy is unmodified.
  for (vector_size_t i = 0; i < size; i++) {
    ArrayVectorPtr expected = (i % 2 == 0) ? a : b;
    ASSERT_TRUE(expected->equalValueAt(result.get(), i, i)) << "at " << i;
  }

  assertEqualOffsetsOrSizes(a->offsets(), offsetsCopy, a->size(), isNullAt(a));

  // Singly referenced array vector; multiply-referenced sizes buffer.
  result = BaseVector::create(result->type(), rows.size(), pool());
  result->copy(a.get(), rows, nullptr);

  pointers.initialize(result);
  auto sizesCopy = result->as<ArrayVector>()->sizes();
  pointers.setSizesUnique(false);
  pointers.assertMutable(result);

  BaseVector::ensureWritable(oddRows, result->type(), pool(), result);
  pointers.assertPointers(result);

  result->copy(b.get(), oddRows, nullptr);

  // Verify sizesCopy is unmodified.
  for (vector_size_t i = 0; i < size; i++) {
    ArrayVectorPtr expected = (i % 2 == 0) ? a : b;
    ASSERT_TRUE(expected->equalValueAt(result.get(), i, i)) << "at " << i;
  }

  assertEqualOffsetsOrSizes(a->sizes(), sizesCopy, a->size(), isNullAt(a));

  // Test arrays containing buffer views. Buffer views are not mutable even if
  // they are unique, and must always be copied on write.
  auto copyOfA = BaseVector::copy(*a);

  result = std::make_shared<ArrayVector>(
      pool(),
      a->type(),
      MockBufferView::create(a->nulls(), releaser),
      a->size(),
      MockBufferView::create(a->offsets(), releaser),
      MockBufferView::create(a->sizes(), releaser),
      a->elements());

  pointers.initialize(result);
  pointers.setNullsUnique(false);

  pointers.setOffsetsUnique(false);
  pointers.setSizesUnique(false);

  pointers.setElementsUnique(false);
  pointers.assertMutable(result);

  BaseVector::ensureWritable(oddRows, result->type(), pool(), result);
  pointers.assertPointers(result);

  result->copy(b.get(), oddRows, nullptr);

  // Verify the result is as expected.
  for (vector_size_t i = 0; i < size; i++) {
    ArrayVectorPtr expected = (i % 2 == 0) ? a : b;
    ASSERT_TRUE(expected->equalValueAt(result.get(), i, i)) << "at " << i;
  }

  // Verify the initial "a" map where we created buffer views has not being
  // overwritten.
  test::assertEqualVectors(a, copyOfA);
}

TEST_F(EnsureWritableVectorTest, map) {
  vector_size_t size = 1'000;
  auto a = makeMapVector<int32_t, int32_t>(
      size,
      /*sizeAt*/ [](vector_size_t row) { return row % 5; },
      /*keyAt*/ [](vector_size_t row) { return row; },
      /*valueAt*/ [](vector_size_t row) { return row + 10; },
      /*isNullAt*/ test::VectorMaker::nullEvery(7));

  auto b = makeMapVector<int32_t, int32_t>(
      size,
      /*sizeAt*/ [](vector_size_t row) { return row % 7; },
      /*keyAt*/ [](vector_size_t row) { return row * 2; },
      /*valueAt*/ [](vector_size_t row) { return row * 2 - 10; },
      /*isNullAt*/ test::VectorMaker::nullEvery(11));

  SelectivityVector rows(size);
  VectorPtr result;
  BaseVector::ensureWritable(rows, MAP(INTEGER(), INTEGER()), pool(), result);
  ASSERT_EQ(size, result->size());
  ASSERT_TRUE(MAP(INTEGER(), INTEGER())->kindEquals(result->type()));
  ASSERT_EQ(VectorEncoding::Simple::MAP, result->encoding());

  result->copy(a.get(), rows, nullptr);

  // Multiply-referenced vector.
  auto resultCopy = result;
  ASSERT_FALSE(result.use_count() == 1);

  auto oddRows = selectOddRows(size);
  BaseVector::ensureWritable(oddRows, result->type(), pool(), result);
  ASSERT_TRUE(result.use_count() == 1);
  ASSERT_NE(resultCopy.get(), result.get());

  // Verify that even rows were copied over.
  for (vector_size_t i = 0; i < size; i += 2) {
    ASSERT_TRUE(a->equalValueAt(result.get(), i, i));
  }

  // Modify odd rows and verify that resultCopy is not affected.
  result->copy(b.get(), oddRows, nullptr);

  for (vector_size_t i = 0; i < size; i++) {
    auto expected = i % 2 == 0 ? a : b;
    ASSERT_TRUE(expected->equalValueAt(result.get(), i, i));

    ASSERT_TRUE(a->equalValueAt(resultCopy.get(), i, i));
  }

  // Singly referenced map vector; multiply-referenced keys vector.
  result = BaseVector::create(result->type(), rows.size(), pool());
  result->copy(a.get(), rows, nullptr);

  MapVectorPointers pointers;
  pointers.initialize(result);
  auto keysCopy = result->as<MapVector>()->mapKeys();
  pointers.setKeysUnique(false);
  pointers.assertMutable(result);

  BaseVector::ensureWritable(oddRows, ARRAY(INTEGER()), pool(), result);
  pointers.assertPointers(result);

  // Verify that even rows were copied over.
  for (vector_size_t i = 0; i < size; i += 2) {
    ASSERT_TRUE(a->equalValueAt(result.get(), i, i)) << "at " << i;
  }

  result->copy(b.get(), oddRows, nullptr);

  // Verify that keysCopy is not modified.
  for (vector_size_t i = 0; i < size; i++) {
    MapVectorPtr expected = (i % 2 == 0) ? a : b;
    ASSERT_TRUE(expected->equalValueAt(result.get(), i, i)) << "at " << i;
  }

  for (vector_size_t i = 0; i < a->mapKeys()->size(); i++) {
    ASSERT_TRUE(a->mapKeys()->equalValueAt(keysCopy.get(), i, i)) << "at " << i;
  }

  // Singly referenced map vector; multiply-referenced values vector.
  result = BaseVector::create(result->type(), rows.size(), pool());
  result->copy(a.get(), rows, nullptr);

  pointers.initialize(result);
  auto valuesCopy = result->as<MapVector>()->mapValues();
  pointers.setValuesUnique(false);
  pointers.assertMutable(result);

  BaseVector::ensureWritable(oddRows, ARRAY(INTEGER()), pool(), result);
  pointers.assertPointers(result);

  // Verify that even rows were copied over.
  for (vector_size_t i = 0; i < size; i += 2) {
    ASSERT_TRUE(a->equalValueAt(result.get(), i, i)) << "at " << i;
  }

  result->copy(b.get(), oddRows, nullptr);

  // Verify that valuesCopy is not modified.
  for (vector_size_t i = 0; i < size; i++) {
    MapVectorPtr expected = (i % 2 == 0) ? a : b;
    ASSERT_TRUE(expected->equalValueAt(result.get(), i, i)) << "at " << i;
  }

  for (vector_size_t i = 0; i < a->mapKeys()->size(); i++) {
    ASSERT_TRUE(a->mapValues()->equalValueAt(valuesCopy.get(), i, i))
        << "at " << i;
  }

  // Singly referenced map vector; multiply-referenced offsets buffer.
  result = BaseVector::create(result->type(), rows.size(), pool());
  result->copy(a.get(), rows, nullptr);

  pointers.initialize(result);
  auto offsetsCopy = result->as<MapVector>()->offsets();
  pointers.setOffsetsUnique(false);
  pointers.assertMutable(result);

  BaseVector::ensureWritable(oddRows, result->type(), pool(), result);
  pointers.assertPointers(result);

  result->copy(b.get(), oddRows, nullptr);

  // Verify offsetsCopy is unmodified
  for (vector_size_t i = 0; i < size; i++) {
    MapVectorPtr expected = (i % 2 == 0) ? a : b;
    ASSERT_TRUE(expected->equalValueAt(result.get(), i, i)) << "at " << i;
  }

  assertEqualOffsetsOrSizes(a->offsets(), offsetsCopy, a->size(), isNullAt(a));

  // Singly referenced map vector; multiply-referenced sizes buffer.
  result = BaseVector::create(result->type(), rows.size(), pool());
  result->copy(a.get(), rows, nullptr);

  pointers.initialize(result);
  auto sizesCopy = result->as<MapVector>()->sizes();
  pointers.setSizesUnique(false);
  pointers.assertMutable(result);

  BaseVector::ensureWritable(oddRows, result->type(), pool(), result);
  pointers.assertPointers(result);

  result->copy(b.get(), oddRows, nullptr);

  // Verify sizesCopy is unmodified.
  for (vector_size_t i = 0; i < size; i++) {
    MapVectorPtr expected = (i % 2 == 0) ? a : b;
    ASSERT_TRUE(expected->equalValueAt(result.get(), i, i)) << "at " << i;
  }

  assertEqualOffsetsOrSizes(a->sizes(), sizesCopy, a->size(), isNullAt(a));

  // Test maps containing buffer views. Buffer views are not mutable even if
  // they are unique, and must always be copied on write.
  auto copyOfA = BaseVector::copy(*a);

  result = std::make_shared<MapVector>(
      pool(),
      a->type(),
      MockBufferView::create(a->nulls(), releaser),
      a->size(),
      MockBufferView::create(a->offsets(), releaser),
      MockBufferView::create(a->sizes(), releaser),
      a->mapKeys(),
      a->mapValues());

  pointers.initialize(result);
  pointers.setNullsUnique(false);

  pointers.setOffsetsUnique(false);
  pointers.setSizesUnique(false);

  pointers.setKeysUnique(false);
  pointers.setValuesUnique(false);
  pointers.assertMutable(result);

  BaseVector::ensureWritable(oddRows, result->type(), pool(), result);
  pointers.assertPointers(result);

  result->copy(b.get(), oddRows, nullptr);

  // Verify the result is as expected.
  for (vector_size_t i = 0; i < size; i++) {
    MapVectorPtr expected = (i % 2 == 0) ? a : b;
    ASSERT_TRUE(expected->equalValueAt(result.get(), i, i)) << "at " << i;
  }

  // Verify the initial "a" map where we created buffer views has not being
  // overwritten.
  test::assertEqualVectors(a, copyOfA);
}

TEST_F(EnsureWritableVectorTest, flatMap) {
  std::vector<std::string> jsonData = {
      "{1:10, 2:20, 3:null}",
      "{7:60}",
      "{}",
      "{4:40, 5:null, 6:null}",
  };
  std::vector<std::string> otherJsonData = {
      "null",
      "{6:10, 2:20, 7:null}",
      "{3:60}",
      "{}",
  };
  vector_size_t size = jsonData.size();

  auto flatMap = makeFlatMapVectorFromJson<int64_t, int32_t>(jsonData);
  auto otherFlatMap =
      makeFlatMapVectorFromJson<int64_t, int32_t>(otherJsonData);

  // Make a shallow copy that points to all the same buffers as `flatMap`.
  VectorPtr shallowCopy = std::make_shared<FlatMapVector>(
      pool(),
      flatMap->type(),
      flatMap->nulls(),
      flatMap->size(),
      flatMap->distinctKeys(),
      flatMap->mapValues(),
      flatMap->inMaps());
  test::assertEqualVectors(shallowCopy, flatMap);

  auto oddRows = selectOddRows(size);
  SelectivityVector evenRows(size);
  evenRows.deselect(oddRows);

  // Copy the odd rows from otherFlatMap into shallowCopy. We want to make sure
  // ensure writable will detect and copy on write the buffers as expected.
  BaseVector::ensureWritable(oddRows, flatMap->type(), pool(), shallowCopy);

  // Ensure that the even rows (the ones that won't be overwritten) were
  // properly copied.
  test::assertEqualVectors(shallowCopy, flatMap, evenRows);

  shallowCopy->copy(otherFlatMap.get(), oddRows, nullptr);

  // Ensure that the original buffers from flatMap have not been modified.
  test::assertEqualVectors(
      flatMap, makeFlatMapVectorFromJson<int64_t, int32_t>(jsonData));

  // Ensure the written vector has the correct even and odd rows.
  test::assertEqualVectors(shallowCopy, otherFlatMap, oddRows);
  test::assertEqualVectors(shallowCopy, flatMap, evenRows);
}

TEST_F(EnsureWritableVectorTest, allNullArray) {
  vector_size_t size = 1'000;
  auto a = makeArrayVector<int64_t>(
      size,
      /*sizeAt*/ [](vector_size_t row) { return row % 5; },
      /*valueAt*/ [](vector_size_t row) { return row; },
      /*isNullAt*/ test::VectorMaker::nullEvery(7));

  VectorPtr result = makeAllNullArrayVector(size, BIGINT());

  SelectivityVector oddRows = selectOddRows(size);
  BaseVector::ensureWritable(oddRows, result->type(), pool(), result);

  result->copy(a.get(), oddRows, nullptr);

  for (vector_size_t i = 0; i < size; i++) {
    if (i % 2 == 0) {
      ASSERT_TRUE(result->isNullAt(i));
    } else {
      ASSERT_TRUE(a->equalValueAt(result.get(), i, i));
    }
  }

  // Multiply-references array vector; should be copied
  result = makeAllNullArrayVector(size, BIGINT());
  auto resultCopy = result;

  BaseVector::ensureWritable(oddRows, result->type(), pool(), result);
  ASSERT_NE(resultCopy.get(), result.get());

  result->copy(a.get(), oddRows, nullptr);
  for (vector_size_t i = 0; i < size; i++) {
    if (i % 2 == 0) {
      ASSERT_TRUE(result->isNullAt(i)) << "at " << i;
    } else {
      ASSERT_TRUE(a->equalValueAt(result.get(), i, i)) << "at " << i;
    }

    ASSERT_TRUE(resultCopy->isNullAt(i)) << "at " << i;
  }
}

TEST_F(EnsureWritableVectorTest, allNullMap) {
  vector_size_t size = 1'000;
  auto a = makeMapVector<int64_t, StringView>(
      size,
      /*sizeAt*/ [](vector_size_t row) { return row % 5; },
      /*keyAt*/ [](vector_size_t row) { return row; },
      /*valueAt*/
      [](vector_size_t row) {
        return StringView::makeInline("s-" + std::to_string(row));
      },
      /*isNullAt*/ test::VectorMaker::nullEvery(7));

  VectorPtr result = makeAllNullMapVector(size, BIGINT(), VARCHAR());

  SelectivityVector oddRows = selectOddRows(size);
  BaseVector::ensureWritable(oddRows, result->type(), pool(), result);

  result->copy(a.get(), oddRows, nullptr);

  for (vector_size_t i = 0; i < size; i++) {
    if (i % 2 == 0) {
      ASSERT_TRUE(result->isNullAt(i));
    } else {
      ASSERT_TRUE(a->equalValueAt(result.get(), i, i));
    }
  }

  // Multiply-referenced vector; should be copied
  result = makeAllNullMapVector(size, BIGINT(), VARCHAR());
  auto resultCopy = result;

  BaseVector::ensureWritable(oddRows, result->type(), pool(), result);
  ASSERT_NE(resultCopy.get(), result.get());

  result->copy(a.get(), oddRows, nullptr);
  for (vector_size_t i = 0; i < size; i++) {
    if (i % 2 == 0) {
      ASSERT_TRUE(result->isNullAt(i)) << "at " << i;
    } else {
      ASSERT_TRUE(a->equalValueAt(result.get(), i, i)) << "at " << i;
    }

    ASSERT_TRUE(resultCopy->isNullAt(i)) << "at " << i;
  }
}

TEST_F(EnsureWritableVectorTest, booleanFlatVector) {
  auto testEnsureWritable = [this](VectorPtr& vector, SelectivityVector& rows) {
    // Make sure vector::values_ buffer is not uniquely referenced so that
    // the branch in the FlatVector::ensureWritable() that copy old values
    // to new buffer is executed.
    auto another = vector->asFlatVector<bool>()->values();

    auto vectorPtr = vector.get();
    ASSERT_NO_THROW(
        BaseVector::ensureWritable(rows, BOOLEAN(), pool(), vector));
    ASSERT_EQ(vectorPtr, vector.get());
    ASSERT_NE(another->as<void>(), vector->valuesAsVoid());
  };

  {
    VectorPtr vector =
        makeFlatVector<bool>(100, [](auto /*row*/) { return true; });

    SelectivityVector rows{200, false};
    rows.setValidRange(16, 32, true);
    rows.updateBounds();

    testEnsureWritable(vector, rows);
  }

  {
    auto value = AlignedBuffer::allocate<bool>(1000, pool(), true);
    // Create a FlatVector with a length smaller than the value buffer.
    VectorPtr vector = std::make_shared<FlatVector<bool>>(
        pool(),
        BOOLEAN(),
        nullptr,
        100,
        std::move(value),
        std::vector<BufferPtr>());

    // Create rows with a length smaller than the value buffer to ensure that
    // the newly created vector is also smaller.
    SelectivityVector rows{100, true};

    testEnsureWritable(vector, rows);
  }
}

TEST_F(EnsureWritableVectorTest, dataDependentFlags) {
  auto size = 10;

  auto ensureWritableStatic = [](VectorPtr& vector) {
    BaseVector::ensureWritable(
        SelectivityVector{1}, vector->type(), vector->pool(), vector);
  };
  auto ensureWritableInstance = [](VectorPtr& vector) {
    vector->ensureWritable(SelectivityVector{1});
  };

  // Primitive flat vector.
  {
    SCOPED_TRACE("Flat");
    auto createVector = [&]() {
      return test::makeFlatVectorWithFlags<TypeKind::VARCHAR>(size, pool());
    };

    test::checkVectorFlagsReset(
        createVector, ensureWritableInstance, SelectivityVector{1});
    test::checkVectorFlagsReset(
        createVector, ensureWritableStatic, SelectivityVector{1});
  }

  // Constant vector.
  {
    SCOPED_TRACE("Constant");
    auto createVector = [&]() {
      return test::makeConstantVectorWithFlags<TypeKind::VARCHAR>(size, pool());
    };

    test::checkVectorFlagsReset(
        createVector, ensureWritableStatic, SelectivityVector{1});
  }

  // Dictionary vector.
  {
    SCOPED_TRACE("Dictionary");
    auto createVector = [&]() {
      return test::makeDictionaryVectorWithFlags<TypeKind::VARCHAR>(
          size, pool());
    };

    test::checkVectorFlagsReset(
        createVector, ensureWritableStatic, SelectivityVector{1});
  }

  // Map vector.
  {
    SCOPED_TRACE("Map");
    auto createVector = [&]() {
      return test::makeMapVectorWithFlags<TypeKind::VARCHAR, TypeKind::VARCHAR>(
          size, pool());
    };

    test::checkVectorFlagsReset(
        createVector, ensureWritableInstance, SelectivityVector{1});
    test::checkVectorFlagsReset(
        createVector, ensureWritableStatic, SelectivityVector{1});
  }
}

TEST_F(EnsureWritableVectorTest, lazyMap) {
  // Check that the flattened vector has the correct size.
  {
    const vector_size_t size = 100;
    VectorPtr lazy = std::make_shared<LazyVector>(
        pool(),
        BIGINT(),
        size,
        std::make_unique<test::SimpleVectorLoader>([&](auto) {
          return BaseVector::createConstant(
              BIGINT(), variant::create<TypeKind::BIGINT>(123), size, pool());
        }));
    BaseVector::ensureWritable(
        SelectivityVector::empty(), BIGINT(), pool(), lazy);
    EXPECT_EQ(VectorEncoding::Simple::FLAT, lazy->encoding());
    EXPECT_EQ(size, lazy->size());
  }
}
