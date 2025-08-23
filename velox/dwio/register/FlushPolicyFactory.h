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

#include "velox/dwio/common/Options.h"
#include <string>
#include <functional>

namespace facebook::velox::dwio::common {

class FlushPolicyFactory {
private:
  FlushPolicyFactory() = default;
};

bool registerDWRFLambdaFlushPolicy(const std::string& keyName, std::function<bool()> flushPolicy);

bool registerParquetLambdaFlushPolicy(const std::string& keyName, uint64_t rowsInRowGroup,
    int64_t bytesInRowGroup, std::function<bool()> flushPolicy);

bool registerFlushPolicy(
    FileFormat format,
    const std::string& policyName,
    std::unique_ptr<dwio::common::FlushPolicy> flushPolicy);

bool unregisterFlushPolicy(FileFormat format, const std::string& policyName);

bool unregisterFlushPolicies(FileFormat format);

std::unique_ptr<FlushPolicy> getFlushPolicy(FileFormat format, const std::string& policyName);

std::vector<std::string> listFlushPolicies(FileFormat format);

} // namespace facebook::velox::dwio::common
