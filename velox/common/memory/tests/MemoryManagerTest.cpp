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

#include <fmt/format.h>
#include <gtest/gtest.h>
#include <vector>

#include <gmock/gmock-matchers.h>
#include "velox/common/base/VeloxException.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/memory/MallocAllocator.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/memory/SharedArbitrator.h"

DECLARE_int32(velox_memory_num_shared_leaf_pools);
DECLARE_bool(velox_enable_memory_usage_track_in_default_memory_pool);

using namespace ::testing;

namespace facebook::velox::memory {

namespace {
constexpr folly::StringPiece kSysRootName{"__sys_root__"};

MemoryManager& toMemoryManager(MemoryManager& manager) {
  return *static_cast<MemoryManager*>(&manager);
}
} // namespace

class MemoryManagerTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    SharedArbitrator::registerFactory();
  }

  inline static const std::string arbitratorKind_{"SHARED"};
};

TEST_F(MemoryManagerTest, ctor) {
  const auto kSharedPoolCount = FLAGS_velox_memory_num_shared_leaf_pools;
  {
    MemoryManager manager{};
    ASSERT_EQ(manager.numPools(), 3);
    ASSERT_EQ(manager.capacity(), kMaxMemory);
    ASSERT_EQ(0, manager.getTotalBytes());
    ASSERT_EQ(manager.alignment(), MemoryAllocator::kMaxAlignment);
    ASSERT_EQ(manager.deprecatedSysRootPool().alignment(), manager.alignment());
    ASSERT_EQ(manager.deprecatedSysRootPool().capacity(), kMaxMemory);
    ASSERT_EQ(manager.deprecatedSysRootPool().maxCapacity(), kMaxMemory);
    ASSERT_EQ(manager.arbitrator()->kind(), "NOOP");
    auto sysPool = manager.deprecatedSysRootPool().shared_from_this();
    ASSERT_NE(sysPool->reclaimer(), nullptr);
    try {
      VELOX_FAIL("Trigger Error");
    } catch (const velox::VeloxRuntimeError& e) {
      VELOX_ASSERT_THROW(
          sysPool->reclaimer()->abort(
              &manager.deprecatedSysRootPool(), std::current_exception()),
          "SysMemoryReclaimer::abort is not supported");
    }
    ASSERT_EQ(sysPool->reclaimer()->priority(), 0);
    memory::MemoryReclaimer::Stats stats;
    ASSERT_EQ(
        sysPool->reclaimer()->reclaim(sysPool.get(), 1'000, 1'000, stats), 0);
    uint64_t reclaimableBytes{0};
    ASSERT_FALSE(
        sysPool->reclaimer()->reclaimableBytes(*sysPool, reclaimableBytes));
  }

  {
    const auto kCapacity = 8L * 1024 * 1024;
    MemoryManager::Options options;
    options.allocatorCapacity = kCapacity;
    options.arbitratorCapacity = kCapacity;
    MemoryManager manager{options};
    ASSERT_EQ(kCapacity, manager.capacity());
    ASSERT_EQ(manager.numPools(), 3);
    ASSERT_EQ(manager.deprecatedSysRootPool().alignment(), manager.alignment());
  }
  {
    const auto kCapacity = 8L * 1024 * 1024;
    MemoryManager::Options options;
    options.alignment = 0;
    options.allocatorCapacity = kCapacity;
    options.arbitratorCapacity = kCapacity;
    MemoryManager manager{options};

    ASSERT_EQ(manager.alignment(), MemoryAllocator::kMinAlignment);
    ASSERT_EQ(manager.deprecatedSysRootPool().alignment(), manager.alignment());
    // TODO: replace with root pool memory tracker quota check.
    ASSERT_EQ(
        kSharedPoolCount + 3, manager.deprecatedSysRootPool().getChildCount());
    ASSERT_EQ(kCapacity, manager.capacity());
    ASSERT_EQ(0, manager.getTotalBytes());
  }
  {
    MemoryManager::Options options;
    const auto kCapacity = 4L << 30;
    options.allocatorCapacity = kCapacity;
    options.arbitratorCapacity = kCapacity;
    std::string arbitratorKind = "SHARED";
    options.arbitratorKind = arbitratorKind;
    MemoryManager manager{options};
    auto* arbitrator = manager.arbitrator();
    ASSERT_EQ(arbitrator->kind(), arbitratorKind);
    ASSERT_EQ(arbitrator->stats().maxCapacityBytes, kCapacity);
    ASSERT_EQ(
        manager.toString(),
        "Memory Manager[capacity 4.00GB alignment 64B usedBytes 0B number of "
        "pools 3\nList of root pools:\n\t__sys_root__\nMemory Allocator[MALLOC "
        "capacity 4.00GB allocated bytes 0 allocated pages 0 mapped pages 0]\n"
        "ARBITRATOR[SHARED CAPACITY[4.00GB] STATS[numRequests 0 numRunning 0 "
        "numSucceded 0 numAborted 0 numFailures 0 numNonReclaimableAttempts 0 "
        "reclaimedFreeCapacity 0B reclaimedUsedCapacity 0B maxCapacity 4.00GB "
        "freeCapacity 4.00GB freeReservedCapacity 0B] "
        "CONFIG[kind=SHARED;capacity=4.00GB;arbitrationStateCheckCb=(unset);]]]");
  }
}

namespace {
class FakeTestArbitrator : public MemoryArbitrator {
 public:
  explicit FakeTestArbitrator(
      const Config& config,
      bool injectAddPoolFailure = false)
      : MemoryArbitrator(
            {.kind = config.kind,
             .capacity = config.capacity,
             .extraConfigs = config.extraConfigs}),
        injectAddPoolFailure_(injectAddPoolFailure) {}

  void shutdown() override {}

  void addPool(const std::shared_ptr<MemoryPool>& /*unused*/) override {
    VELOX_CHECK(!injectAddPoolFailure_, "Failed to add pool");
  }

  void removePool(MemoryPool* /*unused*/) override {}

  void growCapacity(MemoryPool* /*unused*/, uint64_t /*unused*/) override {
    VELOX_NYI();
  }

  uint64_t shrinkCapacity(uint64_t /*unused*/, bool /*unused*/, bool /*unused*/)
      override {
    VELOX_NYI();
  }

  uint64_t shrinkCapacity(MemoryPool* /*unused*/, uint64_t /*unused*/)
      override {
    VELOX_NYI();
  }

  Stats stats() const override {
    VELOX_NYI();
  }

  std::string toString() const override {
    VELOX_NYI();
  }

  std::string kind() const override {
    return "FAKE";
  }

 private:
  const bool injectAddPoolFailure_{false};
};
} // namespace

TEST_F(MemoryManagerTest, createWithCustomArbitrator) {
  const std::string kindString = "FAKE";
  MemoryArbitrator::Factory factory =
      [](const MemoryArbitrator::Config& config) {
        return std::make_unique<FakeTestArbitrator>(config);
      };
  MemoryArbitrator::registerFactory(kindString, factory);
  auto guard = folly::makeGuard(
      [&] { MemoryArbitrator::unregisterFactory(kindString); });
  MemoryManager::Options options;
  options.arbitratorKind = kindString;
  options.allocatorCapacity = 8L << 20;
  options.arbitratorCapacity = 256L << 20;
  MemoryManager manager{options};
  ASSERT_EQ(manager.arbitrator()->capacity(), options.allocatorCapacity);
  ASSERT_EQ(manager.allocator()->capacity(), options.allocatorCapacity);
}

TEST_F(MemoryManagerTest, addPoolFailure) {
  const std::string kindString = "FAKE";
  MemoryArbitrator::Factory factory =
      [](const MemoryArbitrator::Config& config) {
        return std::make_unique<FakeTestArbitrator>(
            config, /*injectAddPoolFailure*/ true);
      };
  MemoryArbitrator::registerFactory(kindString, factory);
  auto guard = folly::makeGuard(
      [&] { MemoryArbitrator::unregisterFactory(kindString); });
  MemoryManager::Options options;
  options.arbitratorKind = kindString;
  MemoryManager manager{options};
  VELOX_ASSERT_THROW(manager.addRootPool(), "Failed to add pool");
}

TEST_F(MemoryManagerTest, addPool) {
  MemoryManager manager{};

  auto rootPool = manager.addRootPool("duplicateRootPool", kMaxMemory);
  ASSERT_EQ(rootPool->capacity(), kMaxMemory);
  ASSERT_EQ(rootPool->maxCapacity(), kMaxMemory);
  { ASSERT_ANY_THROW(manager.addRootPool("duplicateRootPool", kMaxMemory)); }
  auto threadSafeLeafPool = manager.addLeafPool("leafPool", true);
  ASSERT_EQ(threadSafeLeafPool->capacity(), kMaxMemory);
  ASSERT_EQ(threadSafeLeafPool->maxCapacity(), kMaxMemory);
  auto nonThreadSafeLeafPool = manager.addLeafPool("duplicateLeafPool", true);
  ASSERT_EQ(nonThreadSafeLeafPool->capacity(), kMaxMemory);
  ASSERT_EQ(nonThreadSafeLeafPool->maxCapacity(), kMaxMemory);
  { ASSERT_ANY_THROW(manager.addLeafPool("duplicateLeafPool")); }
  const int64_t poolCapacity = 1 << 20;
  auto rootPoolWithMaxCapacity =
      manager.addRootPool("rootPoolWithCapacity", poolCapacity);
  ASSERT_EQ(rootPoolWithMaxCapacity->maxCapacity(), poolCapacity);
  ASSERT_EQ(rootPoolWithMaxCapacity->capacity(), poolCapacity);
  auto leafPool = rootPoolWithMaxCapacity->addLeafChild("leaf");
  ASSERT_EQ(leafPool->maxCapacity(), poolCapacity);
  ASSERT_EQ(leafPool->capacity(), poolCapacity);
  auto aggregationPool = rootPoolWithMaxCapacity->addLeafChild("aggregation");
  ASSERT_EQ(aggregationPool->maxCapacity(), poolCapacity);
  ASSERT_EQ(aggregationPool->capacity(), poolCapacity);
}

TEST_F(MemoryManagerTest, addPoolWithArbitrator) {
  MemoryManager::Options options;
  const auto kCapacity = 32L << 30;
  options.allocatorCapacity = kCapacity;
  options.arbitratorKind = arbitratorKind_;
  // The arbitrator capacity will be overridden by the memory manager's
  // capacity.
  const uint64_t initialPoolCapacity = options.allocatorCapacity / 32;
  using ExtraConfig = SharedArbitrator::ExtraConfig;
  options.extraArbitratorConfigs = {
      {std::string(ExtraConfig::kMemoryPoolInitialCapacity),
       folly::to<std::string>(initialPoolCapacity) + "B"}};
  MemoryManager manager{options};

  auto rootPool = manager.addRootPool(
      "addPoolWithArbitrator", kMaxMemory, MemoryReclaimer::create());
  ASSERT_EQ(rootPool->capacity(), initialPoolCapacity);
  ASSERT_EQ(rootPool->maxCapacity(), kMaxMemory);
  {
    ASSERT_ANY_THROW(manager.addRootPool(
        "addPoolWithArbitrator", kMaxMemory, MemoryReclaimer::create()));
  }
  {
    ASSERT_NO_THROW(manager.addRootPool("addPoolWithArbitrator1", kMaxMemory));
  }
  auto threadSafeLeafPool = manager.addLeafPool("leafPool", true);
  ASSERT_EQ(threadSafeLeafPool->capacity(), kMaxMemory);
  ASSERT_EQ(threadSafeLeafPool->maxCapacity(), kMaxMemory);
  auto nonThreadSafeLeafPool = manager.addLeafPool("duplicateLeafPool", true);
  ASSERT_EQ(nonThreadSafeLeafPool->capacity(), kMaxMemory);
  ASSERT_EQ(nonThreadSafeLeafPool->maxCapacity(), kMaxMemory);
  { ASSERT_ANY_THROW(manager.addLeafPool("duplicateLeafPool")); }
  const int64_t poolCapacity = 1 << 30;
  auto rootPoolWithMaxCapacity = manager.addRootPool(
      "rootPoolWithCapacity", poolCapacity, MemoryReclaimer::create());
  ASSERT_EQ(rootPoolWithMaxCapacity->maxCapacity(), poolCapacity);
  ASSERT_EQ(rootPoolWithMaxCapacity->capacity(), initialPoolCapacity);
  auto leafPool = rootPoolWithMaxCapacity->addLeafChild("leaf");
  ASSERT_EQ(leafPool->maxCapacity(), poolCapacity);
  ASSERT_EQ(leafPool->capacity(), initialPoolCapacity);
  auto aggregationPool = rootPoolWithMaxCapacity->addLeafChild("aggregation");
  ASSERT_EQ(aggregationPool->maxCapacity(), poolCapacity);
  ASSERT_EQ(aggregationPool->capacity(), initialPoolCapacity);
}

// TODO: remove this test when remove deprecatedDefaultMemoryManager.
TEST_F(MemoryManagerTest, defaultMemoryManager) {
  auto& managerA = toMemoryManager(deprecatedDefaultMemoryManager());
  auto& managerB = toMemoryManager(deprecatedDefaultMemoryManager());
  const auto kSharedPoolCount = FLAGS_velox_memory_num_shared_leaf_pools + 3;
  ASSERT_EQ(managerA.numPools(), 3);
  ASSERT_EQ(managerA.deprecatedSysRootPool().getChildCount(), kSharedPoolCount);
  ASSERT_EQ(managerB.numPools(), 3);
  ASSERT_EQ(managerB.deprecatedSysRootPool().getChildCount(), kSharedPoolCount);

  auto child1 = managerA.addLeafPool("child_1");
  ASSERT_EQ(child1->parent()->name(), managerA.deprecatedSysRootPool().name());
  auto child2 = managerB.addLeafPool("child_2");
  ASSERT_EQ(child2->parent()->name(), managerA.deprecatedSysRootPool().name());
  EXPECT_EQ(
      kSharedPoolCount + 2, managerA.deprecatedSysRootPool().getChildCount());
  EXPECT_EQ(
      kSharedPoolCount + 2, managerB.deprecatedSysRootPool().getChildCount());
  ASSERT_EQ(managerA.numPools(), 5);
  ASSERT_EQ(managerB.numPools(), 5);
  auto pool = managerB.addRootPool();
  ASSERT_EQ(managerA.numPools(), 6);
  ASSERT_EQ(managerB.numPools(), 6);
  ASSERT_EQ(
      managerA.toString(),
      "Memory Manager[capacity UNLIMITED alignment 64B usedBytes 0B number of pools 6\nList of root pools:\n\t__sys_root__\n\tdefault_root_0\n\trefcount 2\nMemory Allocator[MALLOC capacity UNLIMITED allocated bytes 0 allocated pages 0 mapped pages 0]\nARBIRTATOR[NOOP CAPACITY[UNLIMITED]]]");
  ASSERT_EQ(
      managerB.toString(),
      "Memory Manager[capacity UNLIMITED alignment 64B usedBytes 0B number of pools 6\nList of root pools:\n\t__sys_root__\n\tdefault_root_0\n\trefcount 2\nMemory Allocator[MALLOC capacity UNLIMITED allocated bytes 0 allocated pages 0 mapped pages 0]\nARBIRTATOR[NOOP CAPACITY[UNLIMITED]]]");
  child1.reset();
  EXPECT_EQ(
      kSharedPoolCount + 1, managerA.deprecatedSysRootPool().getChildCount());
  child2.reset();
  EXPECT_EQ(kSharedPoolCount, managerB.deprecatedSysRootPool().getChildCount());
  ASSERT_EQ(managerA.numPools(), 4);
  ASSERT_EQ(managerB.numPools(), 4);
  pool.reset();
  ASSERT_EQ(managerA.numPools(), 3);
  ASSERT_EQ(managerB.numPools(), 3);
  ASSERT_EQ(
      managerA.toString(),
      "Memory Manager[capacity UNLIMITED alignment 64B usedBytes 0B number of pools 3\nList of root pools:\n\t__sys_root__\nMemory Allocator[MALLOC capacity UNLIMITED allocated bytes 0 allocated pages 0 mapped pages 0]\nARBIRTATOR[NOOP CAPACITY[UNLIMITED]]]");
  ASSERT_EQ(
      managerB.toString(),
      "Memory Manager[capacity UNLIMITED alignment 64B usedBytes 0B number of pools 3\nList of root pools:\n\t__sys_root__\nMemory Allocator[MALLOC capacity UNLIMITED allocated bytes 0 allocated pages 0 mapped pages 0]\nARBIRTATOR[NOOP CAPACITY[UNLIMITED]]]");
  const std::string detailedManagerStr = managerA.toString(true);
  ASSERT_THAT(
      detailedManagerStr,
      testing::HasSubstr(
          "Memory Manager[capacity UNLIMITED alignment 64B usedBytes 0B number of pools 3\nList of root pools:\n__sys_root__ usage 0B reserved 0B peak 0B\n"));
  ASSERT_THAT(
      detailedManagerStr,
      testing::HasSubstr("__sys_spilling__ usage 0B reserved 0B peak 0B\n"));
  ASSERT_THAT(
      detailedManagerStr,
      testing::HasSubstr("__sys_tracing__ usage 0B reserved 0B peak 0B\n"));
  for (int i = 0; i < 32; ++i) {
    ASSERT_THAT(
        managerA.toString(true),
        testing::HasSubstr(fmt::format(
            "__sys_shared_leaf__{} usage 0B reserved 0B peak 0B\n", i)));
  }
}

// TODO: remove this test when remove deprecatedAddDefaultLeafMemoryPool.
TEST(MemoryHeaderTest, addDefaultLeafMemoryPool) {
  auto& manager = toMemoryManager(deprecatedDefaultMemoryManager());
  const auto kSharedPoolCount = FLAGS_velox_memory_num_shared_leaf_pools + 3;
  ASSERT_EQ(manager.deprecatedSysRootPool().getChildCount(), kSharedPoolCount);
  {
    auto poolA = deprecatedAddDefaultLeafMemoryPool();
    ASSERT_EQ(poolA->kind(), MemoryPool::Kind::kLeaf);
    auto poolB = deprecatedAddDefaultLeafMemoryPool();
    ASSERT_EQ(poolB->kind(), MemoryPool::Kind::kLeaf);
    EXPECT_EQ(
        kSharedPoolCount + 2, manager.deprecatedSysRootPool().getChildCount());
    {
      auto poolC = deprecatedAddDefaultLeafMemoryPool();
      ASSERT_EQ(poolC->kind(), MemoryPool::Kind::kLeaf);
      EXPECT_EQ(
          kSharedPoolCount + 3,
          manager.deprecatedSysRootPool().getChildCount());
      {
        auto poolD = deprecatedAddDefaultLeafMemoryPool();
        ASSERT_EQ(poolD->kind(), MemoryPool::Kind::kLeaf);
        EXPECT_EQ(
            kSharedPoolCount + 4,
            manager.deprecatedSysRootPool().getChildCount());
      }
      EXPECT_EQ(
          kSharedPoolCount + 3,
          manager.deprecatedSysRootPool().getChildCount());
    }
    EXPECT_EQ(
        kSharedPoolCount + 2, manager.deprecatedSysRootPool().getChildCount());
  }
  EXPECT_EQ(kSharedPoolCount, manager.deprecatedSysRootPool().getChildCount());

  auto namedPool = deprecatedAddDefaultLeafMemoryPool("namedPool");
  ASSERT_EQ(namedPool->name(), "namedPool");
}

TEST_F(MemoryManagerTest, defaultMemoryUsageTracking) {
  for (bool trackDefaultMemoryUsage : {false, true}) {
    MemoryManager::Options options;
    options.trackDefaultUsage = trackDefaultMemoryUsage;
    MemoryManager manager{options};
    auto defaultPool = manager.addLeafPool("defaultMemoryUsageTracking");
    ASSERT_EQ(defaultPool->trackUsage(), trackDefaultMemoryUsage);
  }

  for (bool trackDefaultMemoryUsage : {false, true}) {
    FLAGS_velox_enable_memory_usage_track_in_default_memory_pool =
        trackDefaultMemoryUsage;
    MemoryManager manager{};
    auto defaultPool = manager.addLeafPool("defaultMemoryUsageTracking");
    ASSERT_EQ(defaultPool->trackUsage(), trackDefaultMemoryUsage);
  }
}

TEST_F(MemoryManagerTest, memoryPoolManagement) {
  const int alignment = 32;
  MemoryManager::Options options;
  options.alignment = alignment;
  MemoryManager manager{options};
  ASSERT_EQ(manager.numPools(), 3);
  const int numPools = 100;
  std::vector<std::shared_ptr<MemoryPool>> userRootPools;
  std::vector<std::shared_ptr<MemoryPool>> userLeafPools;
  for (int i = 0; i < numPools; ++i) {
    const std::string name(std::to_string(i));
    auto pool = i % 2 ? manager.addLeafPool(name) : manager.addRootPool(name);
    ASSERT_EQ(pool->name(), name);
    if (i % 2) {
      ASSERT_EQ(pool->kind(), MemoryPool::Kind::kLeaf);
      userLeafPools.push_back(pool);
      ASSERT_EQ(pool->parent()->name(), manager.deprecatedSysRootPool().name());
    } else {
      ASSERT_EQ(pool->kind(), MemoryPool::Kind::kAggregate);
      ASSERT_EQ(pool->parent(), nullptr);
      userRootPools.push_back(pool);
    }
  }
  auto leafUnamedPool = manager.addLeafPool();
  ASSERT_FALSE(leafUnamedPool->name().empty());
  ASSERT_EQ(leafUnamedPool->kind(), MemoryPool::Kind::kLeaf);
  auto rootUnamedPool = manager.addRootPool();
  ASSERT_FALSE(rootUnamedPool->name().empty());
  ASSERT_EQ(rootUnamedPool->kind(), MemoryPool::Kind::kAggregate);
  ASSERT_EQ(rootUnamedPool->parent(), nullptr);
  ASSERT_EQ(manager.numPools(), 1 + numPools + 3 + 1);
  userLeafPools.clear();
  leafUnamedPool.reset();
  ASSERT_EQ(manager.numPools(), 1 + numPools / 2 + 1 + 1 + 1);
  userRootPools.clear();
  ASSERT_EQ(manager.numPools(), 1 + 3);
  rootUnamedPool.reset();
  ASSERT_EQ(manager.numPools(), 3);
}

// TODO: when run sequentially, e.g. `buck run dwio/memory/...`, this has side
// effects for other tests using process singleton memory manager. Might need to
// use folly::Singleton for isolation by tag.
TEST_F(MemoryManagerTest, globalMemoryManager) {
  initializeMemoryManager(MemoryManager::Options{});
  auto* globalManager = memoryManager();
  ASSERT_TRUE(globalManager != nullptr);
  VELOX_ASSERT_THROW(initializeMemoryManager(MemoryManager::Options{}), "");
  ASSERT_EQ(memoryManager(), globalManager);
  MemoryManager::testingSetInstance(MemoryManager::Options{});
  auto* manager = memoryManager();
  ASSERT_NE(manager, globalManager);
  ASSERT_EQ(manager, memoryManager());
  auto* managerII = memoryManager();
  const auto kSharedPoolCount = FLAGS_velox_memory_num_shared_leaf_pools + 3;
  {
    auto& rootI = manager->deprecatedSysRootPool();
    const std::string childIName("some_child");
    auto childI = rootI.addLeafChild(childIName);
    ASSERT_EQ(rootI.getChildCount(), kSharedPoolCount + 1);

    auto& rootII = managerII->deprecatedSysRootPool();
    ASSERT_EQ(kSharedPoolCount + 1, rootII.getChildCount());
    std::vector<MemoryPool*> pools{};
    rootII.visitChildren([&pools](MemoryPool* child) {
      pools.emplace_back(child);
      return true;
    });
    ASSERT_EQ(pools.size(), kSharedPoolCount + 1);
    int matchedCount = 0;
    for (const auto* pool : pools) {
      if (pool->name() == childIName) {
        ++matchedCount;
      }
    }
    ASSERT_EQ(matchedCount, 1);

    auto childII = manager->addLeafPool("another_child");
    ASSERT_EQ(childII->kind(), MemoryPool::Kind::kLeaf);
    ASSERT_EQ(rootI.getChildCount(), kSharedPoolCount + 2);
    ASSERT_EQ(childII->parent()->name(), kSysRootName.str());
    childII.reset();
    ASSERT_EQ(rootI.getChildCount(), kSharedPoolCount + 1);
    ASSERT_EQ(rootII.getChildCount(), kSharedPoolCount + 1);
    auto userRootChild = manager->addRootPool("rootChild");
    ASSERT_EQ(userRootChild->kind(), MemoryPool::Kind::kAggregate);
    ASSERT_EQ(rootI.getChildCount(), kSharedPoolCount + 1);
    ASSERT_EQ(rootII.getChildCount(), kSharedPoolCount + 1);
    ASSERT_EQ(manager->numPools(), 2 + 3);
  }
  ASSERT_EQ(manager->numPools(), 3);
}

TEST_F(MemoryManagerTest, alignmentOptionCheck) {
  struct {
    uint16_t alignment;
    bool expectedSuccess;

    std::string debugString() const {
      return fmt::format(
          "alignment:{}, expectedSuccess:{}", alignment, expectedSuccess);
    }
  } testSettings[] = {
      {0, true},
      {MemoryAllocator::kMinAlignment - 1, true},
      {MemoryAllocator::kMinAlignment, true},
      {MemoryAllocator::kMinAlignment * 2, true},
      {MemoryAllocator::kMinAlignment + 1, false},
      {MemoryAllocator::kMaxAlignment - 1, false},
      {MemoryAllocator::kMaxAlignment, true},
      {MemoryAllocator::kMaxAlignment + 1, false},
      {MemoryAllocator::kMaxAlignment * 2, false}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    MemoryManager::Options options;
    options.alignment = testData.alignment;
    if (!testData.expectedSuccess) {
      ASSERT_THROW(MemoryManager{options}, VeloxRuntimeError);
      continue;
    }
    MemoryManager manager{options};
    ASSERT_EQ(
        manager.alignment(),
        std::max(testData.alignment, MemoryAllocator::kMinAlignment));
    ASSERT_EQ(
        manager.deprecatedSysRootPool().alignment(),
        std::max(testData.alignment, MemoryAllocator::kMinAlignment));
    auto leafPool = manager.addLeafPool("leafPool");
    ASSERT_EQ(
        leafPool->alignment(),
        std::max(testData.alignment, MemoryAllocator::kMinAlignment));
    auto rootPool = manager.addRootPool("rootPool");
    ASSERT_EQ(
        rootPool->alignment(),
        std::max(testData.alignment, MemoryAllocator::kMinAlignment));
  }
}

TEST_F(MemoryManagerTest, concurrentPoolAccess) {
  MemoryManager manager{};
  const int numAllocThreads = 40;
  std::vector<std::thread> allocThreads;
  std::mutex mu;
  std::vector<std::shared_ptr<MemoryPool>> pools;
  std::atomic<int64_t> poolId{0};
  for (int32_t i = 0; i < numAllocThreads; ++i) {
    allocThreads.push_back(std::thread([&]() {
      for (int i = 0; i < 1000; ++i) {
        if (folly::Random().oneIn(3)) {
          std::shared_ptr<MemoryPool> poolToDelete;
          {
            std::lock_guard<std::mutex> l(mu);
            if (pools.empty()) {
              continue;
            }
            const int idx = folly::Random().rand32() % pools.size();
            poolToDelete = pools[idx];
            pools.erase(pools.begin() + idx);
          }
        } else {
          const std::string name =
              fmt::format("concurrentPoolAccess{}", poolId++);
          std::shared_ptr<MemoryPool> poolToAdd;
          if (folly::Random().oneIn(2)) {
            poolToAdd = manager.addLeafPool(name);
          } else {
            poolToAdd = manager.addRootPool(name);
          }
          std::lock_guard<std::mutex> l(mu);
          pools.push_back(std::move(poolToAdd));
        }
      }
    }));
  }

  std::atomic<bool> stopCheck{false};
  std::thread checkThread([&]() {
    while (!stopCheck) {
      std::this_thread::sleep_for(std::chrono::microseconds(1));
    }
  });

  for (int32_t i = 0; i < allocThreads.size(); ++i) {
    allocThreads[i].join();
  }
  stopCheck = true;
  checkThread.join();
  ASSERT_EQ(manager.numPools(), pools.size() + 3);
  pools.clear();
  ASSERT_EQ(manager.numPools(), 3);
}

TEST_F(MemoryManagerTest, quotaEnforcement) {
  struct {
    int64_t memoryQuotaBytes;
    int64_t smallAllocationBytes;
    int64_t largeAllocationPages;
    bool expectedMemoryExceedError;

    std::string debugString() const {
      return fmt::format(
          "memoryQuotaBytes:{} smallAllocationBytes:{} largeAllocationPages:{} expectedMemoryExceedError:{}",
          succinctBytes(memoryQuotaBytes),
          succinctBytes(smallAllocationBytes),
          largeAllocationPages,
          expectedMemoryExceedError);
    }
  } testSettings[] = {
      {2 << 20, 1 << 20, 256, false},
      {2 << 20, 1 << 20, 512, true},
      {2 << 20, 2 << 20, 256, true},
      {2 << 20, 3 << 20, 0, true},
      {2 << 20, 0, 768, true}};

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    const std::vector<bool> contiguousAllocations = {false, true};
    for (const auto contiguousAlloc : contiguousAllocations) {
      SCOPED_TRACE(fmt::format("contiguousAlloc {}", contiguousAlloc));
      const int alignment = 32;
      MemoryManager::Options options;
      options.alignment = alignment;
      options.allocatorCapacity = testData.memoryQuotaBytes;
      options.arbitratorCapacity = testData.memoryQuotaBytes;
      MemoryManager manager{options};
      auto pool = manager.addLeafPool("quotaEnforcement");
      void* smallBuffer{nullptr};
      if (testData.smallAllocationBytes != 0) {
        if ((testData.largeAllocationPages == 0) &&
            testData.expectedMemoryExceedError) {
          VELOX_ASSERT_THROW(pool->allocate(testData.smallAllocationBytes), "");
          continue;
        }
        smallBuffer = pool->allocate(testData.smallAllocationBytes);
      }
      if (contiguousAlloc) {
        ContiguousAllocation contiguousAllocation;
        if (testData.expectedMemoryExceedError) {
          VELOX_ASSERT_THROW(
              pool->allocateContiguous(
                  testData.largeAllocationPages, contiguousAllocation),
              "");
        } else {
          pool->allocateContiguous(
              testData.largeAllocationPages, contiguousAllocation);
        }
      } else {
        Allocation allocation;
        if (testData.expectedMemoryExceedError) {
          VELOX_ASSERT_THROW(
              pool->allocateNonContiguous(
                  testData.largeAllocationPages, allocation),
              "");
        } else {
          pool->allocateNonContiguous(
              testData.largeAllocationPages, allocation);
        }
      }
      pool->free(smallBuffer, testData.smallAllocationBytes);
    }
  }
}

TEST_F(MemoryManagerTest, disableMemoryPoolTracking) {
  const std::string kSharedKind{"SHARED"};
  const std::string kNoopKind{""};
  MemoryManager::Options options;
  options.disableMemoryPoolTracking = true;
  options.allocatorCapacity = 64LL << 20;
  options.arbitratorCapacity = 64LL << 20;
  std::vector<std::string> arbitratorKinds{kNoopKind, kSharedKind};
  for (const auto& arbitratorKind : arbitratorKinds) {
    options.arbitratorKind = arbitratorKind;
    MemoryManager manager{options};
    auto root0 = manager.addRootPool("root_0", 35LL << 20);
    auto leaf0 = root0->addLeafChild("leaf_0");

    std::shared_ptr<MemoryPool> root0Dup;
    if (arbitratorKind == kSharedKind) {
      // NOTE: shared arbitrator has duplicate check inside.
      VELOX_ASSERT_THROW(
          manager.addRootPool("root_0", 35LL << 20),
          "Memory pool root_0 already exists");
      continue;
    } else {
      // Not throwing since there is no duplicate check.
      root0Dup = manager.addRootPool("root_0", 35LL << 20);
    }

    // 1TB capacity is allowed since there is no capacity check.
    auto root1 = manager.addRootPool("root_1", 1LL << 40);
    auto leaf1 = root1->addLeafChild("leaf_1");

    ASSERT_EQ(root0->capacity(), 35LL << 20);
    if (arbitratorKind == kSharedKind) {
      ASSERT_EQ(root0Dup->capacity(), 29LL << 20);
      ASSERT_EQ(root1->capacity(), 0);
    } else {
      ASSERT_EQ(root0Dup->capacity(), 35LL << 20);
      ASSERT_EQ(root1->capacity(), 1LL << 40);
    }

    ASSERT_EQ(manager.capacity(), 64LL << 20);
    ASSERT_EQ(manager.shrinkPools(), 0);
    // Default 1 system pool with 1 leaf child
    ASSERT_EQ(manager.numPools(), 3);

    VELOX_ASSERT_THROW(
        leaf0->allocate(38LL << 20), "Exceeded memory pool capacity");
    if (arbitratorKind == kSharedKind) {
      VELOX_ASSERT_THROW(
          leaf1->allocate(256LL << 20), "Exceeded memory pool capacity");
    } else {
      VELOX_ASSERT_THROW(
          leaf1->allocate(256LL << 20), "Exceeded memory allocator limit");
    }

    ASSERT_NO_THROW(leaf0.reset());
    ASSERT_NO_THROW(leaf1.reset());
    ASSERT_NO_THROW(root0.reset());
    ASSERT_NO_THROW(root0Dup.reset());
    ASSERT_NO_THROW(root1.reset());
  }
}
} // namespace facebook::velox::memory
