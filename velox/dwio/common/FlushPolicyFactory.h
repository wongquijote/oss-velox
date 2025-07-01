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

#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/Options.h"
// #include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/dwio/dwrf/writer/FlushPolicy.h"
#include "velox/dwio/dwrf/writer/Writer.h"

#ifdef VELOX_ENABLE_PARQUET
// #include "velox/dwio/parquet/RegisterParquetReader.h"
// #include "velox/dwio/parquet/RegisterParquetWriter.h"
// #include "velox/dwio/parquet/reader/ParquetReader.h"
#include "velox/dwio/parquet/writer/Writer.h"
#endif

#include "velox/dwio/common/FlushPolicy.h"

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

  /**
   *  Base class for flush policies.
   */
  class FlushPolicy {
  public:
    virtual ~FlushPolicy() = default;
    virtual bool shouldFlush() = 0;
  };

  /**
   * Default flush policy: flushes based on strip size threshold 
   * and dictionarySizeThresold.
   */
  class DefaultFlushPolicy : public FlushPolicy {
  public:
    DefaultFlushPolicy(uint64_t stripeSizeThreshold, uint64_t dictionarySizeThresold);
    ~DefaultFlushPolicy() = default;
    bool shouldFlush() override;
  private:
    uint64_t stripeSizeThreshold_;
    uint64_t dictionarySizeThresold_;
  };

  /**
   * Lambda-based flush policy: uses a user-supplied lambda for flush logic.
   */
  class LambdaFlushPolicy : public FlushPolicy {
  public:
    LambdaFlushPolicy(
      std::function<bool()> lambda);
    ~LambdaFlushPolicy() = default;
    bool shouldFlush() override;
  private:
      std::function<bool()> lambda_;
  };

   using DefaultFlushPolicyFactory =
    std::function<std::unique_ptr<DefaultFlushPolicy>(uint64_t, uint64_t)>;

  /**
   * Factory function type for creating LambdaFlushPolicy objects.
   * This is a generic lambda policy that doesn't take in any arguments.
   */
  using LambdaFlushPolicyFactory =
    std::function<std::unique_ptr<LambdaFlushPolicy>(std::function<bool()>)>;

  struct FlushPolicyFactories {
    DefaultFlushPolicyFactory = function<std::unique_ptr<facebook::velox::dwio::common::FlushPolicy>();
    LambdaFlushPolicyFactory = function<std::unique_ptr<facebook::velox::dwio::common::FlushPolicy>();
  }

  /**
   * Factory function type for creating DefaultFlushPolicy objects.
   */
  // using DefaultFlushPolicyFactory =
  //   std::function<std::unique_ptr<DefaultFlushPolicy>(uint64_t, uint64_t)>;

  /**
   * Factory function type for creating LambdaFlushPolicy objects.
   * This is a generic lambda policy that doesn't take in any arguments.
   */
  // using LambdaFlushPolicyFactory =
  //   std::function<std::unique_ptr<LambdaFlushPolicy>(std::function<bool()>)>;

  // struct FlushPolicyFactories {
  //   FileFormat format;
  //   std::unordered_map<FileFormat, DefaultFlushPolicyFactory> defaultFactoryMap;
  //   std::unordered_map<FileFormat, LambdaFlushPolicyFactory> lambdaFactoryMap;
  // };

  /**
   * Constructor.
   * @param format File format this factory is designated to.
   * @param factory FlushPolicyFactories struct
   */
  explicit FlushPolicyFactory(FileFormat format) : format_(format) {}

  virtual ~FlushPolicyFactory() = default;

  /**
   * Get the file format ths factory is designated to.
   */
  FileFormat fileFormat() const {
    return format_;
  }

  /**
   * Get the default flush policy map for the file format.
   */
  // std::unordered_map<FileFormat, DefaultFlushPolicyFactory> defaultFlushPolicyMap() {
  //   return defaultFactoryMap_;
  // }

  /**
   * Get the lambda flush policy map for the file format.
   */
  // std::unordered_map<FileFormat, LambdaFlushPolicyFactory> lambdaFlushPolicyMap() {
  //   return lambdaFactoryMap_;
  // }

  /**
   * Get the flush policy factories.
   */
  FlushPolicyFactories flushPolicyFactories() {
    return flushPolicyFactories_;
  }

  using FlushPolicyFactoriesMap = 
    std::unordered_map<FileFormat, std::shared_ptr<FlushPolicyFactories>>;

  /** 
   * 
   */
  FlushPolicyFactoriesMap& flushPolicyFactories();

  
 private:
  const FileFormat format_;
  /**
   * Empty maps that store the flush policies by their file format
   */
  // std::unordered_map<FileFormat, DefaultFlushPolicyFactory> defaultFactoryMap_;
  // std::unordered_map<FileFormat, LambdaFlushPolicyFactory> lambdaFactoryMap_;
  function<std::unique_ptr<facebook::velox::dwio::common::FlushPolicy> flushPolicyFactories_;
};

/**
 * Register a Default Flush Policy factory. Only a single factory can be registered
 * for each file format. An attempt to register multiple factories for
 * a single file format would cause a filure.
 * @return true
 */
bool registerDefaultFactory(FileFormat format);

/**
 * Register a Lambda Flush Policy factory. Only a single factory can be registered
 * for each file format. An attempt to register multiple factories for
 * a single file format would cause a filure.
 * @return true
 */
bool registerLambdaFactory(FileFormat format, std::unique_ptr<std::function<bool()>> lambda);

/**
 * Unregister a Flush factory for a specified file format.
 * @return true for unregistered factory and false for a
 * missing factory for the specfified format.
 */
bool unregisterDefaultFactory(FileFormat format);

/**
 * Unregister a Flush factory for a specified file format.
 * @return true for unregistered factory and false for a
 * missing factory for the specfified format.
 */
bool unregisterLambdaFactory(FileFormat format);

/**
 * Get Flush factory object for a specified file format. Results in
 * a failure if there is no registered factory for this format.
 * @return FlushPolicyFactory object
 */
FlushPolicyFactory::DefaultFlushPolicyFactory getDefaultFactory(FileFormat format);

/**
 * Get Flush factory object for a specified file format. Results in
 * a failure if there is no registered factory for this format.
 * @return FlushPolicyFactory object
 */
FlushPolicyFactory::LambdaFlushPolicyFactory getLambdaFactory(FileFormat format);

} // namespace facebook::velox::dwio::common
