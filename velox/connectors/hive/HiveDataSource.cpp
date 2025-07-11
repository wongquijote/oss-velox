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

#include "velox/connectors/hive/HiveDataSource.h"

#include <fmt/ranges.h>
#include <string>
#include <unordered_map>

#include "velox/common/testutil/TestValue.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/dwio/common/ReaderFactory.h"
#include "velox/expression/FieldReference.h"

using facebook::velox::common::testutil::TestValue;

namespace facebook::velox::connector::hive {

class HiveTableHandle;
class HiveColumnHandle;

namespace {

bool isMember(
    const std::vector<exec::FieldReference*>& fields,
    const exec::FieldReference& field) {
  return std::find(fields.begin(), fields.end(), &field) != fields.end();
}

bool shouldEagerlyMaterialize(
    const exec::Expr& remainingFilter,
    const exec::FieldReference& field) {
  if (!remainingFilter.evaluatesArgumentsOnNonIncreasingSelection()) {
    return true;
  }
  for (auto& input : remainingFilter.inputs()) {
    if (isMember(input->distinctFields(), field) && input->hasConditionals()) {
      return true;
    }
  }
  return false;
}

} // namespace

HiveDataSource::HiveDataSource(
    const RowTypePtr& outputType,
    const connector::ConnectorTableHandlePtr& tableHandle,
    const connector::ColumnHandleMap& columnHandles,
    FileHandleFactory* fileHandleFactory,
    folly::Executor* executor,
    const ConnectorQueryCtx* connectorQueryCtx,
    const std::shared_ptr<HiveConfig>& hiveConfig)
    : fileHandleFactory_(fileHandleFactory),
      executor_(executor),
      connectorQueryCtx_(connectorQueryCtx),
      hiveConfig_(hiveConfig),
      pool_(connectorQueryCtx->memoryPool()),
      outputType_(outputType),
      expressionEvaluator_(connectorQueryCtx->expressionEvaluator()) {
  // Column handled keyed on the column alias, the name used in the query.
  for (const auto& [canonicalizedName, columnHandle] : columnHandles) {
    auto handle =
        std::dynamic_pointer_cast<const HiveColumnHandle>(columnHandle);
    VELOX_CHECK_NOT_NULL(
        handle,
        "ColumnHandle must be an instance of HiveColumnHandle for {}",
        canonicalizedName);
    switch (handle->columnType()) {
      case HiveColumnHandle::ColumnType::kRegular:
        break;
      case HiveColumnHandle::ColumnType::kPartitionKey:
        partitionKeys_.emplace(handle->name(), handle);
        break;
      case HiveColumnHandle::ColumnType::kSynthesized:
        infoColumns_.emplace(handle->name(), handle);
        break;
      case HiveColumnHandle::ColumnType::kRowIndex:
        specialColumns_.rowIndex = handle->name();
        break;
      case HiveColumnHandle::ColumnType::kRowId:
        specialColumns_.rowId = handle->name();
        break;
    }
  }

  std::vector<std::string> readColumnNames;
  auto readColumnTypes = outputType_->children();
  for (const auto& outputName : outputType_->names()) {
    auto it = columnHandles.find(outputName);
    VELOX_CHECK(
        it != columnHandles.end(),
        "ColumnHandle is missing for output column: {}",
        outputName);

    auto* handle = static_cast<const HiveColumnHandle*>(it->second.get());
    readColumnNames.push_back(handle->name());
    for (auto& subfield : handle->requiredSubfields()) {
      VELOX_USER_CHECK_EQ(
          getColumnName(subfield),
          handle->name(),
          "Required subfield does not match column name");
      subfields_[handle->name()].push_back(&subfield);
    }
  }

  hiveTableHandle_ =
      std::dynamic_pointer_cast<const HiveTableHandle>(tableHandle);
  VELOX_CHECK_NOT_NULL(
      hiveTableHandle_, "TableHandle must be an instance of HiveTableHandle");
  if (hiveConfig_->isFileColumnNamesReadAsLowerCase(
          connectorQueryCtx->sessionProperties())) {
    checkColumnNameLowerCase(outputType_);
    checkColumnNameLowerCase(hiveTableHandle_->subfieldFilters(), infoColumns_);
    checkColumnNameLowerCase(hiveTableHandle_->remainingFilter());
  }

  for (const auto& [k, v] : hiveTableHandle_->subfieldFilters()) {
    filters_.emplace(k.clone(), v);
  }
  double sampleRate = 1;
  auto remainingFilter = extractFiltersFromRemainingFilter(
      hiveTableHandle_->remainingFilter(),
      expressionEvaluator_,
      false,
      filters_,
      sampleRate);
  if (sampleRate != 1) {
    randomSkip_ = std::make_shared<random::RandomSkipTracker>(sampleRate);
  }

  if (remainingFilter) {
    remainingFilterExprSet_ = expressionEvaluator_->compile(remainingFilter);
    auto& remainingFilterExpr = remainingFilterExprSet_->expr(0);
    folly::F14FastMap<std::string, column_index_t> columnNames;
    for (int i = 0; i < readColumnNames.size(); ++i) {
      columnNames[readColumnNames[i]] = i;
    }
    for (auto& input : remainingFilterExpr->distinctFields()) {
      auto it = columnNames.find(input->field());
      if (it != columnNames.end()) {
        if (shouldEagerlyMaterialize(*remainingFilterExpr, *input)) {
          multiReferencedFields_.push_back(it->second);
        }
        continue;
      }
      // Remaining filter may reference columns that are not used otherwise,
      // e.g. are not being projected out and are not used in range filters.
      // Make sure to add these columns to readerOutputType_.
      readColumnNames.push_back(input->field());
      readColumnTypes.push_back(input->type());
    }
    remainingFilterSubfields_ = remainingFilterExpr->extractSubfields();
    if (VLOG_IS_ON(1)) {
      VLOG(1) << fmt::format(
          "Extracted subfields from remaining filter: [{}]",
          fmt::join(remainingFilterSubfields_, ", "));
    }
    for (auto& subfield : remainingFilterSubfields_) {
      const auto& name = getColumnName(subfield);
      auto it = subfields_.find(name);
      if (it != subfields_.end()) {
        // Some subfields of the column are already projected out, we append the
        // remainingFilter subfield
        it->second.push_back(&subfield);
      } else if (columnNames.count(name) == 0) {
        // remainingFilter subfield's column is not projected out, we add the
        // column and append the subfield
        subfields_[name].push_back(&subfield);
      }
    }
  }

  readerOutputType_ =
      ROW(std::move(readColumnNames), std::move(readColumnTypes));
  scanSpec_ = makeScanSpec(
      readerOutputType_,
      subfields_,
      filters_,
      hiveTableHandle_->dataColumns(),
      partitionKeys_,
      infoColumns_,
      specialColumns_,
      hiveConfig_->readStatsBasedFilterReorderDisabled(
          connectorQueryCtx_->sessionProperties()),
      pool_);
  if (remainingFilter) {
    metadataFilter_ = std::make_shared<common::MetadataFilter>(
        *scanSpec_, *remainingFilter, expressionEvaluator_);
  }

  ioStats_ = std::make_shared<io::IoStatistics>();
  fsStats_ = std::make_shared<filesystems::File::IoStats>();
}

std::unique_ptr<SplitReader> HiveDataSource::createSplitReader() {
  return SplitReader::create(
      split_,
      hiveTableHandle_,
      &partitionKeys_,
      connectorQueryCtx_,
      hiveConfig_,
      readerOutputType_,
      ioStats_,
      fsStats_,
      fileHandleFactory_,
      executor_,
      scanSpec_);
}

std::vector<column_index_t> HiveDataSource::setupBucketConversion() {
  VELOX_CHECK_NE(
      split_->bucketConversion->tableBucketCount,
      split_->bucketConversion->partitionBucketCount);
  VELOX_CHECK(split_->tableBucketNumber.has_value());
  VELOX_CHECK_NOT_NULL(hiveTableHandle_->dataColumns());
  ++numBucketConversion_;
  bool rebuildScanSpec = false;
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  std::vector<column_index_t> bucketChannels;
  for (auto& handle : split_->bucketConversion->bucketColumnHandles) {
    VELOX_CHECK(handle->columnType() == HiveColumnHandle::ColumnType::kRegular);
    if (subfields_.erase(handle->name()) > 0) {
      rebuildScanSpec = true;
    }
    auto index = readerOutputType_->getChildIdxIfExists(handle->name());
    if (!index.has_value()) {
      if (names.empty()) {
        names = readerOutputType_->names();
        types = readerOutputType_->children();
      }
      index = names.size();
      names.push_back(handle->name());
      types.push_back(
          hiveTableHandle_->dataColumns()->findChild(handle->name()));
      rebuildScanSpec = true;
    }
    bucketChannels.push_back(*index);
  }
  if (!names.empty()) {
    readerOutputType_ = ROW(std::move(names), std::move(types));
  }
  if (rebuildScanSpec) {
    auto newScanSpec = makeScanSpec(
        readerOutputType_,
        subfields_,
        filters_,
        hiveTableHandle_->dataColumns(),
        partitionKeys_,
        infoColumns_,
        specialColumns_,
        hiveConfig_->readStatsBasedFilterReorderDisabled(
            connectorQueryCtx_->sessionProperties()),
        pool_);
    newScanSpec->moveAdaptationFrom(*scanSpec_);
    scanSpec_ = std::move(newScanSpec);
  }
  return bucketChannels;
}

void HiveDataSource::setupRowIdColumn() {
  VELOX_CHECK(split_->rowIdProperties.has_value());
  const auto& props = *split_->rowIdProperties;
  auto* rowId = scanSpec_->childByName(*specialColumns_.rowId);
  VELOX_CHECK_NOT_NULL(rowId);
  auto& rowIdType =
      readerOutputType_->findChild(*specialColumns_.rowId)->asRow();
  auto rowGroupId = split_->getFileName();
  rowId->childByName(rowIdType.nameOf(1))
      ->setConstantValue<StringView>(
          StringView(rowGroupId), VARCHAR(), connectorQueryCtx_->memoryPool());
  rowId->childByName(rowIdType.nameOf(2))
      ->setConstantValue<int64_t>(
          props.metadataVersion, BIGINT(), connectorQueryCtx_->memoryPool());
  rowId->childByName(rowIdType.nameOf(3))
      ->setConstantValue<int64_t>(
          props.partitionId, BIGINT(), connectorQueryCtx_->memoryPool());
  rowId->childByName(rowIdType.nameOf(4))
      ->setConstantValue<StringView>(
          StringView(props.tableGuid),
          VARCHAR(),
          connectorQueryCtx_->memoryPool());
}

void HiveDataSource::addSplit(std::shared_ptr<ConnectorSplit> split) {
  VELOX_CHECK_NULL(
      split_,
      "Previous split has not been processed yet. Call next to process the split.");
  split_ = std::dynamic_pointer_cast<HiveConnectorSplit>(split);
  VELOX_CHECK_NOT_NULL(split_, "Wrong type of split");

  VLOG(1) << "Adding split " << split_->toString();

  if (splitReader_) {
    splitReader_.reset();
  }

  std::vector<column_index_t> bucketChannels;
  if (split_->bucketConversion.has_value()) {
    bucketChannels = setupBucketConversion();
  }
  if (specialColumns_.rowId.has_value()) {
    setupRowIdColumn();
  }

  splitReader_ = createSplitReader();
  if (!bucketChannels.empty()) {
    splitReader_->setBucketConversion(std::move(bucketChannels));
  }
  // Split reader subclasses may need to use the reader options in prepareSplit
  // so we initialize it beforehand.
  splitReader_->configureReaderOptions(randomSkip_);
  splitReader_->prepareSplit(metadataFilter_, runtimeStats_);
  readerOutputType_ = splitReader_->readerOutputType();
}

std::optional<RowVectorPtr> HiveDataSource::next(
    uint64_t size,
    velox::ContinueFuture& /*future*/) {
  VELOX_CHECK(split_ != nullptr, "No split to process. Call addSplit first.");
  VELOX_CHECK_NOT_NULL(splitReader_, "No split reader present");

  TestValue::adjust(
      "facebook::velox::connector::hive::HiveDataSource::next", this);

  if (splitReader_->emptySplit()) {
    resetSplit();
    return nullptr;
  }

  // Bucket conversion or delta update could add extra column to reader output.
  auto needsExtraColumn = [&] {
    return output_->asUnchecked<RowVector>()->childrenSize() <
        readerOutputType_->size();
  };
  if (!output_ || needsExtraColumn()) {
    output_ = BaseVector::create(readerOutputType_, 0, pool_);
  }

  const auto rowsScanned = splitReader_->next(size, output_);
  completedRows_ += rowsScanned;
  if (rowsScanned == 0) {
    splitReader_->updateRuntimeStats(runtimeStats_);
    resetSplit();
    return nullptr;
  }

  VELOX_CHECK(
      !output_->mayHaveNulls(), "Top-level row vector cannot have nulls");
  auto rowsRemaining = output_->size();
  if (rowsRemaining == 0) {
    // no rows passed the pushed down filters.
    return getEmptyOutput();
  }

  auto rowVector = std::dynamic_pointer_cast<RowVector>(output_);

  // In case there is a remaining filter that excludes some but not all
  // rows, collect the indices of the passing rows. If there is no filter,
  // or it passes on all rows, leave this as null and let exec::wrap skip
  // wrapping the results.
  BufferPtr remainingIndices;
  filterRows_.resize(rowVector->size());

  if (remainingFilterExprSet_) {
    rowsRemaining = evaluateRemainingFilter(rowVector);
    VELOX_CHECK_LE(rowsRemaining, rowsScanned);
    if (rowsRemaining == 0) {
      // No rows passed the remaining filter.
      return getEmptyOutput();
    }

    if (rowsRemaining < rowVector->size()) {
      // Some, but not all rows passed the remaining filter.
      remainingIndices = filterEvalCtx_.selectedIndices;
    }
  }

  if (outputType_->size() == 0) {
    return exec::wrap(rowsRemaining, remainingIndices, rowVector);
  }

  std::vector<VectorPtr> outputColumns;
  outputColumns.reserve(outputType_->size());
  for (int i = 0; i < outputType_->size(); ++i) {
    auto& child = rowVector->childAt(i);
    if (remainingIndices) {
      // Disable dictionary values caching in expression eval so that we
      // don't need to reallocate the result for every batch.
      child->disableMemo();
    }
    outputColumns.emplace_back(
        exec::wrapChild(rowsRemaining, remainingIndices, child));
  }

  return std::make_shared<RowVector>(
      pool_, outputType_, BufferPtr(nullptr), rowsRemaining, outputColumns);
}

void HiveDataSource::addDynamicFilter(
    column_index_t outputChannel,
    const std::shared_ptr<common::Filter>& filter) {
  auto& fieldSpec = scanSpec_->getChildByChannel(outputChannel);
  fieldSpec.setFilter(filter);
  scanSpec_->resetCachedValues(true);
  if (splitReader_) {
    splitReader_->resetFilterCaches();
  }
}

std::unordered_map<std::string, RuntimeCounter> HiveDataSource::runtimeStats() {
  auto res = runtimeStats_.toMap();
  res.insert(
      {{"numPrefetch", RuntimeCounter(ioStats_->prefetch().count())},
       {"prefetchBytes",
        RuntimeCounter(
            ioStats_->prefetch().sum(), RuntimeCounter::Unit::kBytes)},
       {"totalScanTime",
        RuntimeCounter(
            ioStats_->totalScanTime(), RuntimeCounter::Unit::kNanos)},
       {"totalRemainingFilterTime",
        RuntimeCounter(
            totalRemainingFilterTime_.load(std::memory_order_relaxed),
            RuntimeCounter::Unit::kNanos)},
       {"ioWaitWallNanos",
        RuntimeCounter(
            ioStats_->queryThreadIoLatency().sum() * 1000,
            RuntimeCounter::Unit::kNanos)},
       {"maxSingleIoWaitWallNanos",
        RuntimeCounter(
            ioStats_->queryThreadIoLatency().max() * 1000,
            RuntimeCounter::Unit::kNanos)},
       {"overreadBytes",
        RuntimeCounter(
            ioStats_->rawOverreadBytes(), RuntimeCounter::Unit::kBytes)}});
  if (ioStats_->read().count() > 0) {
    res.insert({"numStorageRead", RuntimeCounter(ioStats_->read().count())});
    res.insert(
        {"storageReadBytes",
         RuntimeCounter(ioStats_->read().sum(), RuntimeCounter::Unit::kBytes)});
  }
  if (ioStats_->ssdRead().count() > 0) {
    res.insert({"numLocalRead", RuntimeCounter(ioStats_->ssdRead().count())});
    res.insert(
        {"localReadBytes",
         RuntimeCounter(
             ioStats_->ssdRead().sum(), RuntimeCounter::Unit::kBytes)});
  }
  if (ioStats_->ramHit().count() > 0) {
    res.insert({"numRamRead", RuntimeCounter(ioStats_->ramHit().count())});
    res.insert(
        {"ramReadBytes",
         RuntimeCounter(
             ioStats_->ramHit().sum(), RuntimeCounter::Unit::kBytes)});
  }
  if (numBucketConversion_ > 0) {
    res.insert({"numBucketConversion", RuntimeCounter(numBucketConversion_)});
  }

  const auto fsStats = fsStats_->stats();
  for (const auto& storageStats : fsStats) {
    res.emplace(
        storageStats.first,
        RuntimeCounter(storageStats.second.sum, storageStats.second.unit));
  }
  return res;
}

void HiveDataSource::setFromDataSource(
    std::unique_ptr<DataSource> sourceUnique) {
  auto source = dynamic_cast<HiveDataSource*>(sourceUnique.get());
  VELOX_CHECK_NOT_NULL(source, "Bad DataSource type");

  split_ = std::move(source->split_);
  runtimeStats_.skippedSplits += source->runtimeStats_.skippedSplits;
  runtimeStats_.processedSplits += source->runtimeStats_.processedSplits;
  runtimeStats_.skippedSplitBytes += source->runtimeStats_.skippedSplitBytes;
  readerOutputType_ = std::move(source->readerOutputType_);
  source->scanSpec_->moveAdaptationFrom(*scanSpec_);
  scanSpec_ = std::move(source->scanSpec_);
  splitReader_ = std::move(source->splitReader_);
  splitReader_->setConnectorQueryCtx(connectorQueryCtx_);
  // New io will be accounted on the stats of 'source'. Add the existing
  // balance to that.
  source->ioStats_->merge(*ioStats_);
  ioStats_ = std::move(source->ioStats_);
  source->fsStats_->merge(*fsStats_);
  fsStats_ = std::move(source->fsStats_);

  numBucketConversion_ += source->numBucketConversion_;
}

int64_t HiveDataSource::estimatedRowSize() {
  if (!splitReader_) {
    return kUnknownRowSize;
  }
  return splitReader_->estimatedRowSize();
}

vector_size_t HiveDataSource::evaluateRemainingFilter(RowVectorPtr& rowVector) {
  for (auto fieldIndex : multiReferencedFields_) {
    LazyVector::ensureLoadedRows(
        rowVector->childAt(fieldIndex),
        filterRows_,
        filterLazyDecoded_,
        filterLazyBaseRows_);
  }
  uint64_t filterTimeUs{0};
  vector_size_t rowsRemaining{0};
  {
    MicrosecondTimer timer(&filterTimeUs);
    expressionEvaluator_->evaluate(
        remainingFilterExprSet_.get(), filterRows_, *rowVector, filterResult_);
    rowsRemaining = exec::processFilterResults(
        filterResult_, filterRows_, filterEvalCtx_, pool_);
  }
  totalRemainingFilterTime_.fetch_add(
      filterTimeUs * 1000, std::memory_order_relaxed);
  return rowsRemaining;
}

void HiveDataSource::resetSplit() {
  split_.reset();
  splitReader_->resetSplit();
  // Keep readers around to hold adaptation.
}

HiveDataSource::WaveDelegateHookFunction HiveDataSource::waveDelegateHook_;

std::shared_ptr<wave::WaveDataSource> HiveDataSource::toWaveDataSource() {
  VELOX_CHECK_NOT_NULL(waveDelegateHook_);
  if (!waveDataSource_) {
    waveDataSource_ = waveDelegateHook_(
        hiveTableHandle_,
        scanSpec_,
        readerOutputType_,
        &partitionKeys_,
        fileHandleFactory_,
        executor_,
        connectorQueryCtx_,
        hiveConfig_,
        ioStats_,
        remainingFilterExprSet_.get(),
        metadataFilter_);
  }
  return waveDataSource_;
}

//  static
void HiveDataSource::registerWaveDelegateHook(WaveDelegateHookFunction hook) {
  waveDelegateHook_ = hook;
}
std::shared_ptr<wave::WaveDataSource> toWaveDataSource();

} // namespace facebook::velox::connector::hive
