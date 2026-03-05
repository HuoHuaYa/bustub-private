//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_insert_test.cpp
//
// Identification: test/storage/b_plus_tree_insert_test.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cstdio>
#include <random>

#include "buffer/buffer_pool_manager.h"
#include "storage/b_plus_tree_utils.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/index/b_plus_tree.h"
#include "test_util.h" // NOLINT
#include "gtest/gtest.h"

namespace bustub {

using bustub::DiskManagerUnlimitedMemory;

TEST(BPlusTreeTests, BasicInsertTest) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  // allocate header_page
  page_id_t page_id = bpm->NewPage();
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree(
      "foo_pk", page_id, bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  RID rid;

  int64_t key = 42;
  int64_t value = key & 0xFFFFFFFF;
  rid.Set(static_cast<int32_t>(key), value);
  index_key.SetFromInteger(key);
  tree.Insert(index_key, rid);

  auto root_page_id = tree.GetRootPageId();
  auto root_page_guard = bpm->ReadPage(root_page_id);
  auto root_page = root_page_guard.As<BPlusTreePage>();
  ASSERT_NE(root_page, nullptr);
  ASSERT_TRUE(root_page->IsLeafPage());

  auto root_as_leaf =
      root_page_guard
          .As<BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>>();
  ASSERT_EQ(root_as_leaf->GetSize(), 1);
  ASSERT_EQ(comparator(root_as_leaf->KeyAt(0), index_key), 0);

  delete bpm;
}

TEST(BPlusTreeTests, OptimisticInsertTest) {
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  // allocate header_page
  page_id_t page_id = bpm->NewPage();
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree(
      "foo_pk", page_id, bpm, comparator, 4, 3);
  GenericKey<8> index_key;
  RID rid;

  size_t num_keys = 25;
  for (size_t i = 0; i < num_keys; i++) {
    int64_t value = i & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(i >> 32), value);
    index_key.SetFromInteger(2 * i);
    tree.Insert(index_key, rid);
  }

  size_t to_insert = num_keys + 1;
  auto leaf = IndexLeaves<GenericKey<8>, RID, GenericComparator<8>>(
      tree.GetRootPageId(), bpm);
  while (leaf.Valid()) {
    if (((*leaf)->GetSize() + 1) < (*leaf)->GetMaxSize()) {
      to_insert = (*leaf)->KeyAt(0).GetAsInteger() + 1;
    }
    ++leaf;
  }
  EXPECT_NE(to_insert, num_keys + 1);

  auto base_reads = tree.bpm_->GetReads();
  auto base_writes = tree.bpm_->GetWrites();

  index_key.SetFromInteger(to_insert);
  int64_t value = to_insert & 0xFFFFFFFF;
  rid.Set(static_cast<int32_t>(to_insert >> 32), value);
  tree.Insert(index_key, rid);

  auto new_reads = tree.bpm_->GetReads();
  auto new_writes = tree.bpm_->GetWrites();

  EXPECT_GT(new_reads - base_reads, 0);
  EXPECT_EQ(new_writes - base_writes, 1);

  delete bpm;
}

TEST(BPlusTreeTests, InsertTest1NoIterator) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  // allocate header_page
  page_id_t page_id = bpm->NewPage();
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree(
      "foo_pk", page_id, bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  RID rid;

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid);
  }

  bool is_present;
  std::vector<RID> rids;

  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    is_present = tree.GetValue(index_key, &rids);

    EXPECT_EQ(is_present, true);
    EXPECT_EQ(rids.size(), 1);
    EXPECT_EQ(rids[0].GetPageId(), 0);
    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }
  delete bpm;
}

TEST(BPlusTreeTests, InsertTest2) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  // allocate header_page
  page_id_t page_id = bpm->NewPage();
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree(
      "foo_pk", page_id, bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  RID rid;

  std::vector<int64_t> keys = {5, 4, 3, 2, 1};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  for (auto iter = tree.Begin(); iter != tree.End(); ++iter) {
    auto pair = *iter;
    auto location = pair.second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  start_key = 3;
  current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); !iterator.IsEnd(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }
  delete bpm;
}
TEST(BPlusTreeTests, DISABLED_HighPressureInsertTest) {
  // 1. 搭建案发现场（初始化磁盘、缓冲池、比较器）
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  page_id_t page_id = bpm->NewPage();

  // 2. 构造 B+ 树
  // 🌟 核心阴险设计：故意把节点容量调到极小 (叶子=3，内部=4)
  // 这样插上万个数据会疯狂触发你的 Split 和
  // InsertIntoParent，把你的代码按在地上摩擦！
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree(
      "foo_pk", page_id, bpm, comparator, 3, 4);

  GenericKey<8> index_key;
  RID rid;

  // 测试规模：一万条数据连续轰炸！
  int scale = 10000;

  // ==========================================
  // 3. 高压写入：顺序插入 1 到 10000
  // ==========================================
  for (int64_t key = 1; key <= scale; ++key) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);

    bool is_inserted = tree.Insert(index_key, rid);
    ASSERT_TRUE(is_inserted); // 断言：每次插入必须成功，绝不能半路失败
  }

  // ==========================================
  // 4. 严苛验尸：去树里把这 10000 个数据一滴不漏地找出来
  // ==========================================
  std::vector<RID> rids;
  for (int key = 1; key <= scale; ++key) {
    rids.clear();
    index_key.SetFromInteger(key);
    bool is_present = tree.GetValue(index_key, &rids);

    ASSERT_TRUE(
        is_present); // 断言：数据一定得存在！如果触发说明你分裂时把节点搞丢了！
    ASSERT_EQ(rids.size(), 1); // 断言：必须有且只有一个结果

    int64_t value = key & 0xFFFFFFFF;
    ASSERT_EQ(rids[0].GetSlotNum(), value); // 断言：取出来的值必须完全吻合
  }

  // 5. 打扫战场，防止被 ASan 抓内存泄漏
  delete bpm;
}
TEST(BPlusTreeTests, DISABLED_RandomInsertTest) {
  // 1. 搭建案发现场
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());
  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  page_id_t page_id = bpm->NewPage();

  // 依然用极其变态的小容量 (3, 4)
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree(
      "foo_pk", page_id, bpm, comparator, 3, 4);

  GenericKey<8> index_key;
  RID rid;
  int scale = 10000;

  // ==========================================
  // 2. 制造混乱：生成 10000 个完全打乱的乱序 Key
  // ==========================================
  std::vector<int64_t> keys;
  for (int64_t i = 1; i <= scale; ++i) {
    keys.push_back(i);
  }
  // 用魔法把数组彻底打乱
  std::random_device rd;
  std::mt19937 g(rd());
  std::shuffle(keys.begin(), keys.end(), g);

  // ==========================================
  // 3. 乱序轰炸开始！
  // ==========================================
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);

    bool is_inserted = tree.Insert(index_key, rid);
    ASSERT_TRUE(is_inserted); // 乱序插入也必须每次成功！
  }

  // ==========================================
  // 4. 挑刺测试：故意插入一个已经存在的重复 Key
  // ==========================================
  index_key.SetFromInteger(keys[0]); // 拿刚才插进去的第一个 key 再插一次
  bool is_duplicate_inserted = tree.Insert(index_key, rid);
  ASSERT_FALSE(
      is_duplicate_inserted); // 断言：你的代码必须无情拒绝并返回 false！

  // ==========================================
  // 5. 严苛验尸：乱序插进去的，能不能精确找出来？
  // ==========================================
  std::vector<RID> rids;
  for (int64_t key = 1; key <= scale; ++key) {
    rids.clear();
    index_key.SetFromInteger(key);
    bool is_present = tree.GetValue(index_key, &rids);

    ASSERT_TRUE(is_present);
    ASSERT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    ASSERT_EQ(rids[0].GetSlotNum(), value);
  }

  delete bpm;
}
TEST(BPlusTreeTests, Easy_Remove) {
  // 1. 搭建案发现场
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());
  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  page_id_t page_id = bpm->NewPage();

  // 依然用极其变态的小容量 (3, 4)
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>, 2> tree(
      "foo_pk", page_id, bpm, comparator, 3, 4);
  GenericKey<8> index_key;
  RID rid;

  for (int64_t key = 1; key <= 3; key++) {
    rid.Set(static_cast<int32_t>(key >> 32), key & 0xFFFFFFFF);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid);
  }

  tree.Print(bpm);
  // auto iter = tree.Begin();
  // iter.page();
  // const auto &pair = *iter;
  // auto location = pair.second;
  // std::cout<<location.GetSlotNum()<<' '<<location.GetPageId()<<'\n';
  // std::cout<<"has iter begin()\n";
  // // std::cout<<iter.current_page_id_<<' '<<iter.current_index_<<'\n';
  // ++iter;
  // ++iter;
  // ++iter;
  // ++iter;
  // iter.page();
  // std::cout<<iter.current_page_id_<<' '<<iter.current_index_<<'\n';
  delete bpm;
}
} // namespace bustub
