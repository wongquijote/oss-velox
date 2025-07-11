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

#include "velox/dwio/dwrf/writer/FlushPolicy.h"

 #ifdef VELOX_ENABLE_PARQUET
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/dwio/parquet/RegisterParquetWriter.h"
#include "velox/dwio/parquet/reader/ParquetReader.h"
#include "velox/dwio/parquet/writer/Writer.h"
#endif

#include "velox/dwio/common/FlushPolicyFactory.h"

using namespace facebook::velox;
using namespace facebook::velox::dwrf;
using namespace facebook::velox::parquet;

namespace facebook::velox::dwio::common {

namespace {
using DefaultFlushPolicyFactoriesMap = 
    std::unordered_map<FileFormat, std::unique_ptr<dwio::common::FlushPolicy>>;
using LambdaFlushPolicyFactoriesMap = 
  std::unordered_map<FileFormat, std::unique_ptr<dwio::common::FlushPolicy>>;

DefaultFlushPolicyFactoriesMap& defaultFlushPolicyFactories() {
  static DefaultFlushPolicyFactoriesMap factories;
  return factories;
}

LambdaFlushPolicyFactoriesMap& lambdaFlushPolicyFactories() {
  static LambdaFlushPolicyFactoriesMap factories;
  return factories;
}

} // namespace

const uint64_t STRIPE_SIZE_THRESOLD = 1234;
const uint64_t DICTIONARY_SIZE_THRESOLD = 0;
// const auto lambda = []() {
//   return std::make_unique<LambdaFlushPolicy>([]() {
//     return true; // Flushes every batch.
//   });
// };

// This could be changed to take in user's arguments
// Parquet FlushPolicy Class not implemented yet 
const std::unordered_map<FileFormat, std::function<std::unique_ptr<FlushPolicy>>()> DefaultFactoryMap = {
  {FileFormat::DWRF, []() {
    return std::make_unique<dwrf::DefaultFlushPolicy>(STRIPE_SIZE_THRESOLD, DICTIONARY_SIZE_THRESOLD);
    }
  },
  // {FileFormat::PARQUET, []() {
  //   return std::make_unique<parquet::DefaultFlushPolicy>(STRIPE_SIZE_THRESOLD, DICTIONARY_SIZE_THRESOLD);
  //   }
  // }
};

// const std::unordered_map<
//   FileFormat, 
//   std::function<std::unique_ptr<FlushPolicy>(std::function<bool()>)>
// > LambdaFactoryMap = {
//   {FileFormat::DWRF, [](lambda) {
//     return std::make_unique<dwrf::LambdaFlushPolicy(lambda)>;
//     }
//   },
  // {FileFormat::PARQUET, [](lambda) {
  //   return std::make_unique<parquet::LambdaFlushPolicy(lambda)>;
  //   }
  // }
// };
const std::unordered_map<
  FileFormat, 
  std::function<std::unique_ptr<FlushPolicy>(std::function<bool()>)>
> LambdaFactoryMap;


bool registerDefaultFactory(FileFormat format) {
  if (DefaultFactoryMap.contains(format)) {
    auto factory = std::move(DefaultFactoryMap[format]());
    [[maybe_unused]] const bool ok = defaultFlushPolicyFactories().insert({format, std::move(factory)}).second;
    // NOTE: re-enable this check after Prestissimo has updated dwrf registration.
    #if 0
      VELOX_CHECK(
          ok,
          "FlushFactory is already registered for format {}",
          toString(format));
    #endif
      return true;
  } else {
    return true;
  }
}

bool registerLambdaFactory(FileFormat format, std::function<bool()> lambda) {
  auto factory = std::move(LambdaFactoryMap[format](lambda));
  [[maybe_unused]] const bool ok = lambdaFlushPolicyFactories().insert({format, std::move(factory)}).second;
  // NOTE: re-enable this check after Prestissimo has updated dwrf registration.
  #if 0
    VELOX_CHECK(
        ok,
        "FlushFactory is already registered for format {}",
        toString(format));
  #endif
    return true;
}

bool unregisterDefaultFactory(FileFormat format) {
  auto count = defaultFlushPolicyFactories().erase(format);
  return count == 1;
}

bool unregisterLambdaFactory(FileFormat format) {
  auto count = lambdaFlushPolicyFactories().erase(format);
  return count == 1;
}

std::unique_ptr<FlushPolicy> getDefaultFactory(FileFormat format) {
  auto it = defaultFlushPolicyFactories().find(format);
  VELOX_CHECK(
      it != defaultFlushPolicyFactories().end(),
      "DefaultFlushPolicyFactory is not registered for format {}",
      toString(format));
  return std::move(it->second);
}

std::unique_ptr<FlushPolicy> getLambdaFactory(FileFormat format) {
  auto it = lambdaFlushPolicyFactories().find(format);
  VELOX_CHECK(
      it != lambdaFlushPolicyFactories().end(),
      "LambdaFlushPolicyFactory is not registered for format {}",
      toString(format));
  return std::move(it->second);
}

}
