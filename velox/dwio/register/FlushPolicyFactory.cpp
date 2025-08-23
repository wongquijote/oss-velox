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

#include "velox/dwio/common/FlushPolicy.h"
#include "velox/common/base/Exceptions.h"
#include <unordered_map>

#include "velox/dwio/register/FlushPolicyFactory.h"
#include "velox/dwio/dwrf/writer/FlushPolicy.h"

#ifdef VELOX_ENABLE_PARQUET
#include "velox/dwio/parquet/writer/Writer.h"
#endif

using namespace facebook::velox;
using namespace facebook::velox::dwrf;

#ifdef VELOX_ENABLE_PARQUET
using namespace facebook::velox::parquet;
#endif

namespace facebook::velox::dwio::common {
namespace {

using FlushPolicyFactoriesMap =
  std::unordered_map<FileFormat,
      std::unordered_map<std::string, std::unique_ptr<dwio::common::FlushPolicy>>>;

FlushPolicyFactoriesMap& flushPolicyFactories() {
  static FlushPolicyFactoriesMap factories;
  return factories;
}

} // namespace

bool registerDWRFLambdaFlushPolicy(const std::string& keyName, std::function<bool()> flushPolicy) {
  auto [fileMapIt, _] = flushPolicyFactories().try_emplace(FileFormat::DWRF, std::unordered_map<std::string, std::unique_ptr<dwio::common::FlushPolicy>>());

  auto& fileMap = fileMapIt->second;
  auto [policyIt, inserted] = fileMap.try_emplace(keyName, std::make_unique<facebook::velox::dwrf::LambdaFlushPolicy>(
      flushPolicy
    ));
  return inserted;
}

bool registerParquetLambdaFlushPolicy(const std::string& keyName, uint64_t rowsInRowGroup,
    int64_t bytesInRowGroup, std::function<bool()> flushPolicy) {
  auto [fileMapIt, _] = flushPolicyFactories().try_emplace(FileFormat::PARQUET, std::unordered_map<std::string, std::unique_ptr<dwio::common::FlushPolicy>>());

  auto& fileMap = fileMapIt->second;
  auto [policyIt, inserted] = fileMap.try_emplace(keyName, std::make_unique<facebook::velox::parquet::LambdaFlushPolicy>(
      rowsInRowGroup,
      bytesInRowGroup,
      flushPolicy
    ));
  return inserted;
}

bool registerFlushPolicy(
    FileFormat format,
    const std::string& policyName,
    std::unique_ptr<dwio::common::FlushPolicy> flushPolicy)
{
  auto [fileMapIt, _] = flushPolicyFactories().try_emplace(format, std::unordered_map<std::string, std::unique_ptr<dwio::common::FlushPolicy>>());

  auto& fileMap = fileMapIt->second;
  auto [policyIt, inserted] = fileMap.try_emplace(policyName, std::move(flushPolicy));

  VELOX_CHECK(
    inserted,
    "Flush policy named {} already exists for format: {}",
    policyName, format);
  return true;
}

bool unregisterFlushPolicy(FileFormat format, const std::string& policyName) {
  if (flushPolicyFactories().find(format) == flushPolicyFactories().end()) {
    return false;
  }

  return (flushPolicyFactories()[format].erase(policyName) == 1);
}

bool unregisterFlushPolicies(FileFormat format) {
  return (flushPolicyFactories().erase(format) == 1);
}

std::unique_ptr<FlushPolicy> getFlushPolicy(
    FileFormat format,
    const std::string& policyName)
{
  auto fileMapIt = flushPolicyFactories().find(format);
  [[maybe_unused]] const bool ok1 = (fileMapIt != flushPolicyFactories().end());
  VELOX_CHECK(
    ok1,
    "No flush policy factory for format: {}",
    format);

  auto policyIt = fileMapIt->second.find(policyName);
  [[maybe_unused]] const bool ok2 = (policyIt != fileMapIt->second.end());
  VELOX_CHECK(
    ok2,
    "No flush policy named {} for format: {}",
    policyName, format);
  std::unique_ptr<FlushPolicy> flushPolicy = std::move(policyIt->second);
  fileMapIt->second.erase(policyName);
  return flushPolicy;
}

std::vector<std::string> listFlushPolicies(FileFormat format) {
  std::vector<std::string> names;
  auto fileMapIt = flushPolicyFactories().find(format);
  if (fileMapIt != flushPolicyFactories().end()) {
    for (auto& kv : fileMapIt->second) {
      names.push_back(kv.first);
    }
  }
  return names;
}

} // namespace facebook::velox::dwio::common