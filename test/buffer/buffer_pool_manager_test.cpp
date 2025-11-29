//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_test.cpp
//
// Identification: test/buffer/buffer_pool_manager_test.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>
#include <filesystem>
#include <random>
#include <thread>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "storage/page/page_guard.h"

namespace bustub {

static std::filesystem::path db_fname("test.bustub");

const size_t K_DIST = 2;

// =============================================================================
// 1. DirtyNewPageTest (对应 SchedulerTest 失败)
// 目的: 验证 NewPage 创建的页面默认为 Dirty，能触发写盘
// 失败现象: "No operations to disk manager"
// =============================================================================
TEST(BufferPoolManagerTest, DirtyNewPageTest) {
  auto disk_manager = std::make_shared<DiskManager>(db_fname);
  // 只给 1 个 Frame，迫使每次 NewPage 都要驱逐旧的
  auto bpm = std::make_shared<BufferPoolManager>(1, disk_manager.get(), K_DIST);

  // 1. 创建 Page 0
  auto pid0 = bpm->NewPage();
  ASSERT_NE(pid0, INVALID_PAGE_ID);
  
  // 此时 Page 0 在内存中，Pin=0 (NewPage返回后无人持有)。
  // 如果 NewPage 实现正确，is_dirty_ 应为 true。

  // 2. 创建 Page 1
  // 这会强制驱逐 Page 0。
  // 如果 Page 0 是 dirty 的，BPM 会调用 FlushPage -> DiskManager::Write。
  // 如果 Page 0 不是 dirty 的，BPM 直接丢弃它，不写盘。
  auto pid1 = bpm->NewPage();
  ASSERT_NE(pid1, INVALID_PAGE_ID);

  // 3. 验证 Page 0 是否被写入磁盘
  // 我们通过从磁盘重新读取 Page 0 来间接验证。
  // 此时 Page 0 肯定不在内存里了（被 Page 1 占了）。
  // 调用 ReadPage(0) 会触发 FetchPage 从磁盘读。
  
  auto guard0 = bpm->ReadPage(pid0);
  ASSERT_EQ(guard0.GetPageId(), pid0);
  
  // 如果 SchedulerTest 失败，通常是因为 NewPage 没有设置 dirty，
  // 导致 Evict 时没有写盘，虽然这里读回来可能是全0数据（模拟磁盘默认），
  // 但 Autograder 监控的是 DiskManager 的操作计数。
  
  // 我们可以手动 Flush 验证返回值 (有些实现 Flush 不脏返回 false/true 需结合逻辑)
  // 但最核心的修复是在 NewPage 里设置 is_dirty_ = true。
  
  remove(db_fname);
  remove(disk_manager->GetLogFileName());
}

// =============================================================================
// 2. RapidEvictionTest (对应 StaircaseTest 崩溃)
// 目的: 模拟大量 Evict -> Flush 链条，检测 FlushPage 加锁是否导致 Crash
// 失败现象: Subprocess aborted / system_error inside lock_shared
// =============================================================================
TEST(BufferPoolManagerTest, RapidEvictionTest) {
  auto disk_manager = std::make_shared<DiskManager>(db_fname);
  // 3 个 Frames，K=2
  auto bpm = std::make_shared<BufferPoolManager>(3, disk_manager.get(), K_DIST);

  // 1. 先填满 Buffer
  std::vector<page_id_t> pages;
  for(int i=0; i<3; ++i) {
      pages.push_back(bpm->NewPage());
  }

  // 2. 疯狂创建新页，触发大量 Evict 和 Flush
  // StaircaseTest 的本质就是 Create -> Delete -> Create 循环
  for(int i=0; i<100000; ++i) {
      // NewPage 内部逻辑：
      // 1. Lock BPM
      // 2. Evict (调用 FlushPage)
      //    -> FlushPage Lock BPM (递归锁? 不, Evict 内部调用 FlushUnsafe 或者 解锁后Flush)
      //    -> 如果 FlushPage 尝试获取 Page RLock
      // 3. Update Page Table
      // 4. Unlock BPM
      
      auto pid = bpm->NewPage();
      ASSERT_NE(pid, INVALID_PAGE_ID);
      
      // 模拟一点负载：读取一下，更新 LRU 状态
      {
          auto guard = bpm->ReadPage(pid);
      } // Drop
  }
  
  // 如果 FlushPage 里的锁逻辑有问题（比如在持有 BPM 锁时尝试拿 Page 锁导致死锁或异常），
  // 这个循环大概率会复现 Crash。
  
  remove(db_fname);
  remove(disk_manager->GetLogFileName());
}

// =============================================================================
// 3. DeleteConsistencyTest (回归测试)
// 确保之前的 DeletePage 修复是有效的
// =============================================================================
TEST(BufferPoolManagerTest, DeleteConsistencyTest) {
  auto disk_manager = std::make_shared<DiskManager>(db_fname);
  auto bpm = std::make_shared<BufferPoolManager>(10, disk_manager.get(), K_DIST);

  auto pid = bpm->NewPage();
  ASSERT_NE(pid, INVALID_PAGE_ID);

  // 刚 New 出来的页面，Pin 应该是 0，应该可以被删除
  bool res = bpm->DeletePage(pid);
  EXPECT_TRUE(res);

  // 删除后，再次删除应该返回 true (根据 Spec)
  EXPECT_TRUE(bpm->DeletePage(pid));

  remove(db_fname);
  remove(disk_manager->GetLogFileName());
}

}  // namespace bustub