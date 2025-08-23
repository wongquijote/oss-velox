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
#include "velox/dwio/common/Options.h"
#include "velox/dwio/register/FlushPolicyFactory.h"

namespace {
static constexpr size_t kNumDictioanryTestsPerStripe = 3UL;

static constexpr uint64_t kDefaultStripeSizeThreshold = 1234; // 64 * 1024 * 1024; // 64 MB
static constexpr uint64_t kDefaultDictionarySizeThreshold = 0; // 16 * 1024 * 102; // 16 MB
static const std::vector<uint64_t> kDefaultRowsPerStripe = {
    1000000, 2000000, 3000000, 4000000, 5000000};
static constexpr uint64_t kDefaultRowCountThreshold = 1000000; // 1 million rows
} // namespace

namespace facebook::velox::dwrf {

void registerDwrfFlushPolicyFactories() {
  dwio::common::registerFlushPolicy(
    facebook::velox::dwio::common::FileFormat::DWRF,
    "default",
    std::make_unique<DefaultFlushPolicy>(
        kDefaultStripeSizeThreshold,
        kDefaultDictionarySizeThreshold
    )
  );
  dwio::common::registerFlushPolicy(
      facebook::velox::dwio::common::FileFormat::DWRF,
      "rows_per_stripe",
      std::make_unique<RowsPerStripeFlushPolicy>(
        kDefaultRowsPerStripe
      )
  );
  dwio::common::registerFlushPolicy(
      facebook::velox::dwio::common::FileFormat::DWRF,
      "rows_threshold",
      std::make_unique<RowThresholdFlushPolicy>(
        kDefaultRowCountThreshold
      )
  );
  dwio::common::registerFlushPolicy(
      facebook::velox::dwio::common::FileFormat::DWRF,
      "lambda",
      std::make_unique<LambdaFlushPolicy>(
        []() -> bool {
          return true; // Default lambda policy always returns true.
        }
      )
  );
}

void unregisterDwrfFlushPolicyFactories() {
  dwio::common::unregisterFlushPolicies(dwio::common::FileFormat::DWRF);
}


DefaultFlushPolicy::DefaultFlushPolicy(
    uint64_t stripeSizeThreshold,
    uint64_t dictionarySizeThreshold)
    : stripeSizeThreshold_{stripeSizeThreshold},
      dictionarySizeThreshold_{dictionarySizeThreshold},
      dictionaryAssessmentThreshold_{getDictionaryAssessmentIncrement()} {}

uint64_t DefaultFlushPolicy::getDictionaryAssessmentIncrement() const {
  return stripeSizeThreshold_ / kNumDictioanryTestsPerStripe;
}

FlushDecision DefaultFlushPolicy::shouldFlushDictionary(
    bool flushStripe,
    bool overMemoryBudget,
    const dwio::common::StripeProgress& stripeProgress,
    int64_t dictionaryMemoryUsage) {
  if (flushStripe) {
    return FlushDecision::SKIP;
  }

  if (dictionaryMemoryUsage > dictionarySizeThreshold_) {
    return FlushDecision::FLUSH_DICTIONARY;
  }
  if (stripeProgress.stripeSizeEstimate >= dictionaryAssessmentThreshold_) {
    // In the current implementation, since we don't ever change encoding
    // decision after the first stripe, we don't need to ever reset this
    // threshold.
    dictionaryAssessmentThreshold_ +=
        stripeSizeThreshold_ / kNumDictioanryTestsPerStripe;
    return FlushDecision::EVALUATE_DICTIONARY;
  }
  return FlushDecision::SKIP;
}

FlushDecision DefaultFlushPolicy::shouldFlushDictionary(
    bool flushStripe,
    bool overMemoryBudget,
    const dwio::common::StripeProgress& stripeProgress,
    const WriterContext& context) {
  return shouldFlushDictionary(
      flushStripe,
      overMemoryBudget,
      stripeProgress,
      context.getMemoryUsage(MemoryUsageCategory::DICTIONARY));
}

RowsPerStripeFlushPolicy::RowsPerStripeFlushPolicy(
    std::vector<uint64_t> rowsPerStripe)
    : rowsPerStripe_{std::move(rowsPerStripe)} {
  // Note: Vector will be empty for empty files.
  for (auto i = 0; i < rowsPerStripe_.size(); i++) {
    DWIO_ENSURE_GT(
        rowsPerStripe_.at(i),
        0,
        "More than 0 rows expected in the stripe at ",
        i,
        folly::join(",", rowsPerStripe_));
  }
}

// We can throw if writer reported the incoming write to be over memory budget.
bool RowsPerStripeFlushPolicy::shouldFlush(
    const dwio::common::StripeProgress& stripeProgress) {
  const auto stripeIndex = stripeProgress.stripeIndex;
  const auto stripeRowCount = stripeProgress.stripeRowCount;
  DWIO_ENSURE_LT(
      stripeIndex,
      rowsPerStripe_.size(),
      "Stripe index is bigger than expected");

  DWIO_ENSURE_LE(
      stripeRowCount,
      rowsPerStripe_.at(stripeIndex),
      "More rows in Stripe than expected ",
      stripeIndex);

  if ((stripeIndex + 1) == rowsPerStripe_.size()) {
    // Last Stripe is always flushed at the time of close.
    return false;
  }

  return stripeRowCount == rowsPerStripe_.at(stripeIndex);
}
} // namespace facebook::velox::dwrf
