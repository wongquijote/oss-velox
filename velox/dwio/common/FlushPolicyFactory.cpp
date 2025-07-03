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
struct PairHash {
  template <class T1, class T2>
  std::size_t operator()(const std::pair<T1, T2>& p) const {
    auto h1 = std::hash<T1>{}(p.first);
    auto h2 = std::hash<T2>{}(p.second);
    return h1 ^ (h2 + 0x9e3779b9 + (h1 << 6) + (h1 >> 2));
  }
};

using FlushPolicyFactoriesMap = 
    std::unordered_map<std::pair<FileFormat, std::string>, std::unique_ptr<dwio::common::FlushPolicy>, PairHash>;

FlushPolicyFactoriesMap& flushPolicyFactories() {
  static FlushPolicyFactoriesMap factories;
  return factories;
}

} // namespace

bool registerDefaultFactory(FileFormat format, uint64_t stripeSizeThreshold = 1234,
    uint64_t dictionarySizeThresold = 0) {
  switch (format) {
    case FileFormat::DWRF: {
      auto factory = std::make_unique<dwrf::DefaultFlushPolicy>(stripeSizeThreshold, dictionarySizeThresold);
      [[maybe_unused]] const bool ok =
          flushPolicyFactories().insert(std::make_pair(std::make_pair(format, "Default"), std::move(factory))).second;
      // NOTE: re-enable this check after Prestissimo has updated dwrf registration.
      #if 0
        VELOX_CHECK(
            ok,
            "FlushFactory is already registered for format {}",
            toString(factory->fileFormat()));
      #endif
        return true;
    }
    case FileFormat::PARQUET: {
      auto factory = std::make_unique<parquet::DefaultFlushPolicy>(stripeSizeThreshold, dictionarySizeThresold);
      [[maybe_unused]] const bool ok =
          flushPolicyFactories().insert(std::make_pair(std::make_pair(format, "Default"), std::move(factory))).second;
      // NOTE: re-enable this check after Prestissimo has updated dwrf registration.
      #if 0
        VELOX_CHECK(
            ok,
            "FlushFactory is already registered for format {}",
            toString(factory->fileFormat()));
      #endif
        return true;
    }
    default:
        return false;
  }
}

bool registerLambdaFactory(FileFormat format, std::function<bool()> lambda)
{
  switch (format) {
    case FileFormat::DWRF: {
      auto factory = std::make_unique<dwrf::LambdaFlushPolicy>(lambda);
      [[maybe_unused]] const bool ok =
          flushPolicyFactories().insert(std::make_pair(std::make_pair(format, "Lambda"), std::move(factory))).second;
      // NOTE: re-enable this check after Prestissimo has updated dwrf registration.
      #if 0
        VELOX_CHECK(
            ok,
            "FlushFactory is already registered for format {}",
            toString(factory->fileFormat()));
      #endif
        return true;
    }
    // case FileFormat::PARQUET: {
    //   auto factory = std::make_unique<parquet::LambdaFlushPolicy>(lambda);
    //   [[maybe_unused]] const bool ok =
    //       flushPolicyFactories().insert(std::make_pair(std::make_pair(format, "Lambda"), std::move(factory))).second;
    //   // NOTE: re-enable this check after Prestissimo has updated dwrf registration.
    //   #if 0
    //     VELOX_CHECK(
    //         ok,
    //         "FlushFactory is already registered for format {}",
    //         toString(factory->fileFormat()));
    //   #endif
    //     return true;
    // }
    default:
      return false;
  }
}

bool unregisterDefaultFactory(FileFormat format) {
  auto count = flushPolicyFactories().erase(std::pair(format, "Default"));
  return count == 1;
}

bool unregisterLambdaFactory(FileFormat format) {
  auto count = flushPolicyFactories().erase(std::pair(format, "Lambda"));
  return count == 1;
}

FlushPolicyFactory::PolicyFactory getDefaultFactory(FileFormat format) {
  auto it = flushPolicyFactories().find(std::pair(format, "Default"));
  VELOX_CHECK(
      it != flushPolicyFactories().end(),
      "DefaultFlushPolicyFactory is not registered for format {}",
      toString(format));
  auto flushPolicyFactory = [&it]() {
    return std::move(it->second);
  };
  return flushPolicyFactory;
}

FlushPolicyFactory::PolicyFactory getLambdaFactory(FileFormat format) {
  auto it = flushPolicyFactories().find(std::pair(format, "Lambda"));
  VELOX_CHECK(
      it != flushPolicyFactories().end(),
      "LambdaFlushPolicyFactory is not registered for format {}",
      toString(format));
  auto flushPolicyFactory = [&it]() {
    return std::move(it->second);
  };
  return flushPolicyFactory;
}

}
