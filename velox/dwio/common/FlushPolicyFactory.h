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

#pragma once

#include <memory>

// #include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/Options.h"
// #include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/dwio/common/FlushPolicy.h"
// #include "velox/dwio/dwrf/writer/Writer.h"

#ifdef VELOX_ENABLE_PARQUET
// #include "velox/dwio/parquet/RegisterParquetReader.h"
// #include "velox/dwio/parquet/RegisterParquetWriter.h"
// #include "velox/dwio/parquet/reader/ParquetReader.h"
// #include "velox/dwio/parquet/writer/Writer.h"
#endif

#include "velox/dwio/common/FlushPolicy.h"
#include "velox/dwio/dwrf/writer/FlushPolicy.h"

namespace facebook::velox::dwio::common {

/**
 * Flush factory interface.
 *
 * Implement this interface to provide a factory of Flushs
 * for a particular file format. Factory objects should be
 * registered using registerFlushPolicyFactory method to become
 * available for connectors. Only a single Flush factory
 * per file format is allowed.
 */
class FlushPolicyFactory {
 public:
  using PolicyFactory =
    std::function<std::unique_ptr<dwio::common::FlushPolicy>()>;

  ~FlushPolicyFactory() = default;
};

/**
 * Register a Default Flush Policy factory. Only a single factory can be registered
 * for each file format. An attempt to register multiple factories for
 * a single file format would cause a filure.
 * @return true
 */
bool registerDefaultFactory(FileFormat format, uint64_t stripeSizeThreshold,
    uint64_t dictionarySizeThresold);

/**
 * Register a Lambda Flush Policy factory. Only a single factory can be registered
 * for each file format. An attempt to register multiple factories for
 * a single file format would cause a filure.
 * @return true
 */
bool registerLambdaFactory(FileFormat format, std::function<bool()> lambda);

/**
 * Unregister a Default Flush Policy factory for a specified file format.
 * @return true for unregistered factory and false for a
 * missing factory for the specfified format.
 */
bool unregisterDefaultFactory(FileFormat format);

/**
 * Unregister a Lambda Flush Policy factory for a specified file format.
 * @return true for unregistered factory and false for a
 * missing factory for the specfified format.
 */
bool unregisterLambdaFactory(FileFormat format);

/**
 * Get a function that returns a Default Flush Policy object for a specified file format. Results in
 * a failure if there is no default policy for this format.
 * @return FlushPolicyFactory object
 */
FlushPolicyFactory::PolicyFactory getDefaultFactory(FileFormat format);

/**
 * Get a function that returns a Lambda Flush Policy object for a specified file format. Results in
 * a failure if there is no lambda policy for this format.
 * @return FlushPolicyFactory object
 */
FlushPolicyFactory::PolicyFactory getLambdaFactory(FileFormat format);
} // namespace facebook::velox::dwio::common
