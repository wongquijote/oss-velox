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

using FlushPolicyFactoriesMap = 
    std::unordered_map<FileFormat, std::unique_ptr<FlushPolicy>>;

FlushPolicyFactoriesMap& flushPolicyFactories() {
  static FlushPolicyFactoriesMap factories;
  return factories;
}

} // namespace

template <typename T>
bool registerDefaultFactory(FileFormat format, uint64_t stripeSizeThreshold = 1234,
    uint64_t dictionarySizeThresold = 0) {
  auto factory = std::make_unique<T>(stripeSizeThreshold, dictionarySizeThresold);
  [[maybe_unused]] const bool ok =
      flushPolicyFactories().insert({std::make_pair(format, FlushPolicyType::Default), factory}).second;
  // NOTE: re-enable this check after Prestissimo has updated dwrf registration.
#if 0
  VELOX_CHECK(
      ok,
      "FlushFactory is already registered for format {}",
      toString(factory->fileFormat()));
#endif
  return true;
}

template <typename T>
bool registerLambdaFactory(FileFormat format, std::function<bool()> lambda = 
    []() {
      return std::make_unique<T>([]() {
        return true; // Flushes every batch.
      })
    }
) {
 auto factory = std::make_unique<T>(lambda);
  [[maybe_unused]] const bool ok =
      flushPolicyFactories().insert({std::make_pair(format, FlushPolicyType::Lambda), factory}).second;
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
  auto count = flushPolicyFactories().erase(std::pair(format, FlushPolicyType::Default));
  return count == 1;
}

bool unregisterLambdaFactory(FileFormat format) {
  auto count = flushPolicyFactories().erase(std::pair(format, FlushPolicyType::Lambda));
  return count == 1;
}

FlushPolicyFactory::DefaultFlushPolicyFactory getDefaultFactory(FileFormat format) {
  auto it = flushPolicyFactories().find(std::pair(format, FlushPolicyType::Default));
  VELOX_CHECK(
      it != flushPolicyFactories().end(),
      "DefaultFlushPolicyFactory is not registered for format {}",
      toString(format));
  auto flushPolicyFactory = []() {
    return it->second;
  };
  return flushPolicyFactory;
}

FlushPolicyFactory::LambdaFlushPolicyFactory getLambdaFactory(FileFormat format) {
  auto it = flushPolicyFactories().find(std::pair(format, FlushPolicyType::Lambda));
  VELOX_CHECK(
      it != flushPolicyFactories().end(),
      "LambdaFlushPolicyFactory is not registered for format {}",
      toString(format));
  auto flushPolicyFactory = []() {
    return it->second;
  };
  return flushPolicyFactory;
}

}
