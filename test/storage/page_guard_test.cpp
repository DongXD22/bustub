//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard_test.cpp
//
// Identification: test/storage/page_guard_test.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>
#include <cstring>
#include <future>  // NOLINT
#include <random>
#include <string>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/page/page_guard.h"

#include "gtest/gtest.h"

namespace bustub {

const size_t FRAMES = 5;
const size_t K_DIST = 2;

// =============================================================================
// 1. DropTest
// =============================================================================
TEST(PageGuardTest, DropTest) {
  // [修复] 在当前作用域创建 DiskManager，确保它在 BPM 之后析构
  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(FRAMES, disk_manager.get(), K_DIST);

  const auto pid = bpm->NewPage();

  // 1. 测试 Pin Count 管理
  {
    auto guard = bpm->ReadPage(pid);
    EXPECT_EQ(1, bpm->GetPinCount(pid));
    guard.Drop();
    EXPECT_EQ(0, bpm->GetPinCount(pid));
    guard.Drop();
    EXPECT_EQ(0, bpm->GetPinCount(pid));
  }

  // 2. 测试锁释放
  {
    auto write_guard = bpm->WritePage(pid);
    write_guard.Drop();
    auto write_guard_2 = bpm->WritePage(pid);
    EXPECT_EQ(1, bpm->GetPinCount(pid));
  }
}

// =============================================================================
// 2. MoveTest
// =============================================================================
TEST(PageGuardTest, MoveTest) {
  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(FRAMES, disk_manager.get(), K_DIST);

  const auto pid = bpm->NewPage();

  // --- Case A: 移动构造 ---
  {
    auto guard1 = bpm->ReadPage(pid);
    EXPECT_EQ(1, bpm->GetPinCount(pid));

    ReadPageGuard guard2(std::move(guard1));

    EXPECT_NE(guard2.GetData(), nullptr);
    EXPECT_EQ(pid, guard2.GetPageId());
    EXPECT_EQ(1, bpm->GetPinCount(pid));
  }
  EXPECT_EQ(0, bpm->GetPinCount(pid));

  // --- Case B: 移动赋值 ---
  {
    auto guard1 = bpm->ReadPage(pid);
    auto guard2 = bpm->ReadPage(pid);
    EXPECT_EQ(2, bpm->GetPinCount(pid));

    guard1 = std::move(guard2);

    EXPECT_EQ(1, bpm->GetPinCount(pid));
    EXPECT_NE(guard1.GetData(), nullptr);
  }
  EXPECT_EQ(0, bpm->GetPinCount(pid));

  // --- Case C: 自我赋值 ---
  {
    auto guard1 = bpm->WritePage(pid);
    EXPECT_EQ(1, bpm->GetPinCount(pid));

    auto &guard_ref = guard1;
    guard1 = std::move(guard_ref);

    EXPECT_EQ(1, bpm->GetPinCount(pid));
    EXPECT_NE(guard1.GetData(), nullptr);
  }
  EXPECT_EQ(0, bpm->GetPinCount(pid));
}

// =============================================================================
// 3. DataConsistencyTest
// =============================================================================
// TEST(PageGuardTest, DataConsistencyTest) {
//   auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
//   auto bpm = std::make_shared<BufferPoolManager>(FRAMES, disk_manager.get(), K_DIST);

//   const auto pid1 = bpm->NewPage();
//   const std::string data1 = "Hello BusTub";

//   // 1. 写入数据
//   {
//     auto guard = bpm->WritePage(pid1);
//     std::memcpy(guard.GetDataMut(), data1.c_str(), data1.length() + 1);
//   }

//   // 2. 疯狂创建新页，强制将 pid1 驱逐
//   std::vector<page_id_t> other_pages;
//   for (size_t i = 0; i < FRAMES + 2; i++) {
//     auto pid = bpm->NewPage();
//     other_pages.push_back(pid);
//   }

//   // 3. 重新读取 pid1，验证数据
//   {
//     auto guard = bpm->ReadPage(pid1);
//     EXPECT_EQ(std::strcmp(guard.GetData(), data1.c_str()), 0) << "Page content mismatch after eviction!";
//   }
// }

// =============================================================================
// 4. ConcurrencyLatchTest
// =============================================================================
TEST(PageGuardTest, ConcurrencyLatchTest) {
  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(FRAMES, disk_manager.get(), K_DIST);

  const auto pid = bpm->NewPage();

  // 1. 主线程获取读锁
  auto reader_guard = bpm->ReadPage(pid);

  std::promise<void> writer_started;
  std::future<void> writer_future = std::async(std::launch::async, [&]() {
    writer_started.set_value();
    // 2. 子线程尝试获取写锁 (应被阻塞)
    auto writer_guard = bpm->WritePage(pid);
    std::strcpy(writer_guard.GetDataMut(), "writer_was_here");
  });

  writer_started.get_future().wait();
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  char buf[BUSTUB_PAGE_SIZE] = {0};
  EXPECT_EQ(std::memcmp(reader_guard.GetData(), buf, BUSTUB_PAGE_SIZE), 0);

  // 3. 释放读锁，Writer 进入
  reader_guard.Drop();
  writer_future.wait();

  // 4. 验证 Writer 修改成功
  auto verify_guard = bpm->ReadPage(pid);
  EXPECT_EQ(std::strcmp(verify_guard.GetData(), "writer_was_here"), 0);
}

}  // namespace bustub