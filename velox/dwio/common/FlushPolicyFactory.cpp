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

#include "velox/dwio/common/FlushPolicyFactory.h"

namespace facebook::velox::dwio::common {

namespace {

// // Returns a factory that creates DefaultFlushPolicy objects
// template <typename T>
// DefaultFlushPolicyFactory makeDefaultFlushPolicyFactory(
//     T format,
//     uint64_t stripeSizeThreshold,
//     uint64_t dictionarySizeThresold) {
//     return [stripeSizeThreshold, dictionarySizeThresold]() {std::make_unique<T>(stripeSizeThreshold, dictionarySizeThresold)};
// }

// // Returns a factory that creates LambdaFlushPolicy objects with the lambda function.
// LambdaFlushPolicyFactory makeLambdaFlushPolicyFactory(
//     std::function<bool()> lambda) {
//     return [lambda]() {std::make_unique<LambdaFlushPolicy>(lambda)};   
// }

using FlushPolicyFactoriesMap = 
    std::unordered_map<FileFormat, std::shared_ptr<FlushPolicyFactory>;

FlushPolicyFactoriesMap& flushPolicyFactories() {
  static FlushPolicyFactoriesMap factories;
  return factories;
}
} // namespace

const uint64_t DefaultStripeSizeThreshold = 1234;
const uint64_t DefaultDictionarySizeThresold = 0;
const std::unordered_map<FileFormat, std::function<dwio::common::FlushPolicy()>> DefaultPolicies = {
  {FileFormat::DWRF, [DefaultStripeSizeThreshold, DefaultDictionarySizeThresold]() { return dwrf::DefaultFlushPolicy>(DefaultStripeSizeThreshold, DefaultDictionarySizeThresold); }},
#ifdef VELOX_ENABLE_PARQUET
  {FileFormat::PARQUET, [DefaultStripeSizeThreshold, DefaultDictionarySizeThresold]() { return parquet::DefaultFlushPolicy>(DefaultStripeSizeThreshold, DefaultDictionarySizeThresold); }}
#endif
};
const std::unordered_map<FileFormat, std::function<bool()>> LambdaPolicies = {
  {FileFormat::DWRF, []() { return true; } },
#ifdef VELOX_ENBALE_PARQUET
  {FileFormat::PARQUET, []() { return true; } }
#endif
};

bool registerDefaultFactory(FileFormat format) {
  if (format == FileFormat::DWRF || format == FileFormat::PARQUET) {
    // auto fmt = dwrf::DefaultFlushPolicy>(rowsInRowGroup, bytesInRowGroup);
    auto factory = DefaultPolicies[format];
  } else {
    return false;
  }
  // auto factory = makeDefaultFlushPolicyFactory(fmt);
  FlushPolicyFactory flushPolicyFactory = {
    .format = format,
  };
  flushPolicyFactory[format].defaultFlushPolicyMap().insert({format, factory});
  [[maybe_unused]] const bool ok =
      flushFactories().insert({factory->fileFormat(), flushPolicyFactory}).second;
  // NOTE: re-enable this check after Prestissimo has updated dwrf registration.
#if 0
  VELOX_CHECK(
      ok,
      "FlushFactory is already registered for format {}",
      toString(factory->fileFormat()));
#endif
  return true;
}

/// Create some default lambda policies
bool registerLambdaFactory(FileFormat format, std::unique_ptr<std::function<bool()>> lambda = 
    []() {
      return std::make_unique<LambdaFlushPolicy>([]() {
        return true; // Flushes every batch.
      })
    }
) {
  if (format == FileFormat::DWRF || format == FileFormat::Parquet) {
    auto factory = LambdaPolicies[format];
  } else {
    return false;
  }
  // auto factory = makeLambdaFlushPolicyFactory(lambda)
  FlushPolicyFactory flushPolicyFactory = {
    .format = format,
  };
  flushPolicyFactory[format].lambdaFlushPolicyMap().insert({format, factory});
  [[maybe_unused]] const bool ok =
      flushFactories().insert({factory->fileFormat(), flushPolicyFactory}).second;
  // NOTE: re-enable this check after Prestissimo has updated dwrf registration.
#if 0
  VELOX_CHECK(
      ok,
      "FlushFactory is already registered for format {}",
      toString(factory->fileFormat()));
#endif
  return true;
}

bool unregisterDefaultFactory(FileFormat format) {
  auto count = flushFactories().defaultFlushPolicyMap().erase(format);
  return count == 1;
}

bool unregisterLambdaFactory(FileFormat format) {
  auto count = flushFactories().lamdaFlushPolicyMap().erase(format);
  return count == 1;
}

std::optional<std::shared_ptr<DefaultFlushPolicyFactory>> getDefaultFactory(FileFormat format) {
  auto it = flushFactories().defaultFlushPolicyMap().find(format);
  VELOX_CHECK(
      it != flushFactories().defaultFlushPolicyMap().end(),
      "FlushFactory is not registered for format {}",
      toString(format));
  return it->second.defaultFactory;
}

std::optional<std::shared_ptr<LambdaFlushPolicyFactory>> getLambdaFactory(FileFormat format) {
  auto it = flushFactories().lambdaFlushPolicyMap().find(format);
  VELOX_CHECK(
      it != flushFactories().lambdaFlushPolicyMap().end(),
      "FlushFactory is not registered for format {}",
      toString(format));
  return it->second.lambdaFactory;
}

} // namespace facebook::velox::dwio::common

// // Base class for flush policies.
// class FlushPolicy {
// public:
//     virtual ~FlushPolicy() = default;
//     virtual bool shouldFlush() = 0;
// };

// // Default flush policy: flushes based on row and byte limits.
// class DefaultFlushPolicy : public FlushPolicy {
// public:
//     DefaultFlushPolicy(uint64_t stripeSizeThreshold, int64_t dictionarySizeThresold);
//     ~DefaultFlushPolicy() = default;
//     bool shouldFlush() override;
// private:
//     uint64_t stripeSizeThreshold_;
//     int64_t dictionarySizeThresold_;
// };

// // Lambda-based flush policy: uses a user-supplied lambda for flush logic.
// class LambdaFlushPolicy : public FlushPolicy {
// public:
//     LambdaFlushPolicy(
//         std::function<bool()> lambda);
//     ~LambdaFlushPolicy = default;
//     bool shouldFlush() override;
// private:
//     std::function<bool()> lambda_;
// };

// /// Factory function type for creating DefaultFlushPolicy objects.
// using DefaultFlushPolicyFactory =
//     std::function<std::unique_ptr<DefaultFlushPolicy>(uint64_t, uint64_t)>;

// /// Factory function type for creating LambdaFlushPolicy objects.
// /// This is a generic lambda policy that doesn't take in any arguments.
// using LambdaFlushPolicyFactory =
//     std::function<std::unique_ptr<LambdaFlushPolicy>(std::function<bool()>)>;

// struct FlushPolicyFactories {
//     std::optional<DefaultFlushPolicyFactory> defaultFactory;
//     std::optional<LambdaFlushPolicyFactory> lambdaFactory;
// }

/// Returns a factory that creates DefaultFlushPolicy objects with the specified parameters.
// template <typename T>
// DefaultFlushPolicyFactory makeDefaultFlushPolicyFactory(
//     T format,
//     uint64_t stripeSizeThreshold,
//     uint64_t dictionarySizeThresold) {
//     return [stripeSizeThreshold, dictionarySizeThresold]() {std::make_unique<T>(stripeSizeThreshold, dictionarySizeThresold)};
// }

// /// Returns a factory that creates LambdaFlushPolicy objects with the specified parameters and lambda.
// LambdaFlushPolicyFactory makeLambdaFlushPolicyFactory(
//     std::function<bool()> lambda) {
//     return [lambda]() {std::make_unique<LambdaFlushPolicy>(lambda)};   
// }

// using FlushPolicyFactoriesMap = 
//     std::unordered_map<FileFormat, std::shared_ptr<FlushPolicyFactory>;

// FlushPolicyFactoriesMap& flushPolicyFactories() {
//   static FlushPolicyFactoriesMap factories;
//   return factories;
// }

// } // namespace
