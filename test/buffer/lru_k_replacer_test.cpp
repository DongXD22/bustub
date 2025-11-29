//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer_test.cpp
//
// Identification: test/buffer/lru_k_replacer_test.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

#include <algorithm>
#include <atomic>
#include <cstdio>
#include <memory>
#include <random>
#include <set>
#include <thread>  // NOLINT
#include <vector>

#include "common/exception.h"
#include "gtest/gtest.h"

namespace bustub {

// 测试场景 1: K=1 时，行为应该完全等同于普通的 LRU
TEST(LRUKReplacerTest, LRU_K_1_Test) {
  LRUKReplacer lru_replacer(10, 1);

  // 访问顺序: 1 -> 2 -> 3 -> 4
  lru_replacer.RecordAccess(1);
  lru_replacer.SetEvictable(1, true);
  lru_replacer.RecordAccess(2);
  lru_replacer.SetEvictable(2, true);
  lru_replacer.RecordAccess(3);
  lru_replacer.SetEvictable(3, true);
  lru_replacer.RecordAccess(4);
  lru_replacer.SetEvictable(4, true);

  ASSERT_EQ(4, lru_replacer.Size());

  // 此时最老的应该是 1
  auto result = lru_replacer.Evict();
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(1, result.value());
  ASSERT_EQ(3, lru_replacer.Size());

  // 再次访问 2，使其变成最新的 (MRU)
  lru_replacer.RecordAccess(2);

  // 现在驱逐顺序应该是 3 -> 4 -> 2
  result = lru_replacer.Evict();
  ASSERT_EQ(3, result.value());

  result = lru_replacer.Evict();
  ASSERT_EQ(4, result.value());

  result = lru_replacer.Evict();
  ASSERT_EQ(2, result.value());

  ASSERT_EQ(0, lru_replacer.Size());
}

// 测试场景 2: 测试无穷距离 (+Inf) 和有限距离的混合驱逐
// 规则：历史访问次数 < K 的帧 (Inf) 必须优先于 >= K 的帧被驱逐
TEST(LRUKReplacerTest, Inf_Vs_Finite_Test) {
  // K = 3
  LRUKReplacer lru_replacer(10, 3);

  // Frame 1: 访问 3 次 (满足 K=3，距离为有限值)
  lru_replacer.RecordAccess(1);
  lru_replacer.RecordAccess(1);
  lru_replacer.RecordAccess(1);
  lru_replacer.SetEvictable(1, true);

  // Frame 2: 访问 1 次 (不足 K=3，距离为 +Inf)
  lru_replacer.RecordAccess(2);
  lru_replacer.SetEvictable(2, true);

  // Frame 3: 访问 2 次 (不足 K=3，距离为 +Inf)
  lru_replacer.RecordAccess(3);
  lru_replacer.RecordAccess(3);
  lru_replacer.SetEvictable(3, true);

  ASSERT_EQ(3, lru_replacer.Size());

  // 此时 Frame 1 是安全的，Frame 2 和 3 是 +Inf。
  // 在 +Inf 中，根据 FIFO (最早访问时间)，Frame 2 最早出现。
  // 所以驱逐顺序应该是：2 -> 3 -> 1

  auto result = lru_replacer.Evict();
  ASSERT_EQ(2, result.value());

  result = lru_replacer.Evict();
  ASSERT_EQ(3, result.value());

  result = lru_replacer.Evict();
  ASSERT_EQ(1, result.value());
}

// 测试场景 3: 所有的帧都满足 K 次访问，比较 Backward K-Distance
TEST(LRUKReplacerTest, Backward_K_Distance_Test) {
  // K = 2
  LRUKReplacer lru_replacer(10, 2);

  /*
   * 时间轴模拟:
   * T1: Access 1
   * T2: Access 2
   * T3: Access 3
   * T4: Access 1 (Frame 1 完成 K=2, 倒数第K次是 T1)
   * T5: Access 2 (Frame 2 完成 K=2, 倒数第K次是 T2)
   * T6: Access 3 (Frame 3 完成 K=2, 倒数第K次是 T3)
   */

  lru_replacer.RecordAccess(1);  // T1
  lru_replacer.RecordAccess(2);  // T2
  lru_replacer.RecordAccess(3);  // T3

  lru_replacer.RecordAccess(1);  // T4
  lru_replacer.RecordAccess(2);  // T5
  lru_replacer.RecordAccess(3);  // T6

  lru_replacer.SetEvictable(1, true);
  lru_replacer.SetEvictable(2, true);
  lru_replacer.SetEvictable(3, true);

  // Frame 1 的倒数第 K 次是 T1 (最老)
  // Frame 2 的倒数第 K 次是 T2
  // Frame 3 的倒数第 K 次是 T3
  // 驱逐顺序应该是 1 -> 2 -> 3

  ASSERT_EQ(1, lru_replacer.Evict());
  ASSERT_EQ(2, lru_replacer.Evict());
  ASSERT_EQ(3, lru_replacer.Evict());
}

// 测试场景 4: 复杂的 SetEvictable 和 Size 逻辑
TEST(LRUKReplacerTest, Evictable_Toggle_Test) {
  LRUKReplacer lru_replacer(10, 2);

  lru_replacer.RecordAccess(1);
  lru_replacer.RecordAccess(1);
  // 默认不可驱逐，Size = 0
  ASSERT_EQ(0, lru_replacer.Size());

  lru_replacer.SetEvictable(1, true);
  ASSERT_EQ(1, lru_replacer.Size());

  lru_replacer.SetEvictable(1, true);  // 重复设置，Size 不变
  ASSERT_EQ(1, lru_replacer.Size());

  lru_replacer.SetEvictable(1, false);
  ASSERT_EQ(0, lru_replacer.Size());

  // 尝试驱逐，应该失败，因为 Frame 1 不可驱逐
  auto result = lru_replacer.Evict();
  ASSERT_FALSE(result.has_value());

  lru_replacer.SetEvictable(1, true);
  ASSERT_EQ(1, lru_replacer.Size());

  // 设置一个不存在的 frame，不应影响 size
}

// 测试场景 5: Remove 方法测试
TEST(LRUKReplacerTest, Remove_Test) {
  LRUKReplacer lru_replacer(10, 2);

  lru_replacer.RecordAccess(1);
  lru_replacer.RecordAccess(1);
  lru_replacer.SetEvictable(1, true);

  lru_replacer.RecordAccess(2);
  lru_replacer.RecordAccess(2);
  lru_replacer.SetEvictable(2, true);

  ASSERT_EQ(2, lru_replacer.Size());

  // 移除 Frame 1
  lru_replacer.Remove(1);
  ASSERT_EQ(1, lru_replacer.Size());

  // Evict 应该直接返回 2，跳过 1
  auto result = lru_replacer.Evict();
  ASSERT_EQ(2, result.value());
  ASSERT_EQ(0, lru_replacer.Size());

  // 尝试移除不存在的 Frame，应无事发生
  lru_replacer.Remove(99);

  // 尝试移除不可驱逐的 Frame，应该抛异常
  lru_replacer.RecordAccess(3);  // 默认 evictable=false
  ASSERT_THROW(lru_replacer.Remove(3), Exception);
}

// 测试场景 6: 边界检查与异常
TEST(LRUKReplacerTest, Invalid_Input_Test) {
  LRUKReplacer lru_replacer(5, 2);  // capacity = 5 (id 0-4)

  // 访问越界 ID
  ASSERT_THROW(lru_replacer.RecordAccess(5), Exception);
  ASSERT_THROW(lru_replacer.SetEvictable(6, true), Exception);

  // Remove 虽然不需要抛越界异常，但通常实现里越界查找会返回 end，所以也不会崩
  lru_replacer.Remove(6);
}

// 测试场景 7: 并发测试 (检测死锁和段错误)
TEST(LRUKReplacerTest, Concurrency_Test) {
  const int num_threads = 4;
  const int num_ops = 1000;
  LRUKReplacer lru_replacer(100, 2);

  std::vector<std::thread> threads;
  std::atomic<int> success_evicts{0};

  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back([&, i]() {
      for (int j = 0; j < num_ops; ++j) {
        int frame_id = (i * 100 + j) % 50;  // 限制在 0-49 范围内

        // 随机执行操作
        lru_replacer.RecordAccess(frame_id);

        if (j % 2 == 0) {
          lru_replacer.SetEvictable(frame_id, true);
        }

        if (j % 10 == 0) {
          auto res = lru_replacer.Evict();
          if (res.has_value()) {
            success_evicts++;
          }
        }
      }
    });
  }

  for (auto &t : threads) {
    t.join();
  }

  // 只要没崩就是胜利，最后验证一下 size 不为负数
  ASSERT_GE(lru_replacer.Size(), 0);
}

}  // namespace bustub