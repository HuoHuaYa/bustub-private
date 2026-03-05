//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_leaf_page.cpp
//
// Identification: src/storage/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/b_plus_tree_leaf_page.h"

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * @brief Init method after creating a new leaf page
 *
 * After creating a new leaf page from buffer pool, must call initialize method
 * to set default values, including set page type, set current size to zero, set
 * page id/parent id, set next page id and set max size.
 *
 * @param max_size Max size of the leaf node
 */
// 初始化
FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  // page type
  SetPageType(IndexPageType::LEAF_PAGE);
  // current size
  SetSize(0);
  // page id , parent id

  // next page id
  SetNextPageId(INVALID_PAGE_ID);
  // max size
  SetMaxSize(max_size);

  num_tombstones_ = 0;
}

/**
 * @brief Helper function for fetching tombstones of a page.
 * @return The last `NumTombs` keys with pending deletes in this page in order
 * of recency (oldest at front).
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetTombstones() const -> std::vector<KeyType> {
  std::vector<KeyType> temp_tombstones;
  temp_tombstones.reserve(num_tombstones_);
  for (size_t i = 0; i < num_tombstones_; i++) {
    temp_tombstones.emplace_back(key_array_[tombstones_[i]]);
  }
  return temp_tombstones;
}

/**
 * Helper methods to set/get next page id
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t {
  return next_page_id_;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
}

/*
 * Helper method to find and return the key associated with input "index" (a.k.a
 * array offset)
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  assert(index >= 0);
  return key_array_[index];
}

// add
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key,
                                          const KeyComparator &comparator) const
    -> int {
  auto start = key_array_;
  auto end = key_array_ + GetSize();
  // 都是类似一个个点，直接lower
  auto target = std::lower_bound(start, end, key,
                                 [&](const KeyType &k1, const KeyType &k2) {
                                   return comparator(k1, k2) < 0;
                                 });
  int index = std::distance(key_array_, target);
  return index;
}
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  assert(index >= 0);
  return rid_array_[index];
}
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::IsTombstone(int index) const -> bool {
  for (int i = 0; i < static_cast<int>(num_tombstones_); i++) {
    if (static_cast<int>(tombstones_[i]) == index) {
      return true;
    }
  }
  return false;
}
// 在分裂，合并时，带着墓碑就没法写了，所以compact清空所有墓碑
// 双指针，不用开辟额外空间
FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Compact() {
  if (num_tombstones_ == 0) {
    return;
  }
  int new_index = 0;
  int current_size = GetSize();
  // 双指针跑一下
  for (int old_index = 0; old_index < current_size; old_index++) {
    if (IsTombstone(old_index)) {
      continue;
    }
    // 如果索引都没变，就不更新了
    if (new_index != old_index) {
      key_array_[new_index] = key_array_[old_index];
      rid_array_[new_index] = rid_array_[old_index];
    }
    new_index++;
  }
  SetSize(new_index);
  num_tombstones_ = 0;
}
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key,
                                        const ValueType &value,
                                        const KeyComparator &comparator)
    -> bool {
  // 首先检查是否存在相同的key
  int index = KeyIndex(key, comparator);
  // 有这个key的墓碑
  if (index < GetSize() && comparator(KeyAt(index), key) == 0) {
    if (IsTombstone(index)) {
      // 覆盖
      rid_array_[index] = value;
      // 从墓碑数组中删掉
      for (int i = 0; i < static_cast<int>(num_tombstones_); i++) {
        if (tombstones_[i] == static_cast<size_t>(index)) {
          // 后面所有人往前移
          for (int j = i; j < static_cast<int>(num_tombstones_) - 1; j++) {
            tombstones_[j] = tombstones_[j + 1];
          }
          // tombstones_[i] = tombstones_[num_tombstones_ - 1];
          num_tombstones_--;
          return true;
        }
      }
    }
    return false;
  }

  // 不存在同样key的墓碑时
  if (GetSize() == GetMaxSize()) {
    Compact();
    if (GetSize() == GetMaxSize()) {
      return false;
    }
  }
  // 如果compact删了一些墓碑条目，这个时候索引index需要更新
  index = KeyIndex(key, comparator);
  // 检查索引是否有效
  if (index < 0 || index > GetSize()) {
    return false;
  }
  // 插入键值对
  for (int i = GetSize(); i > index; i--) {
    key_array_[i] = key_array_[i - 1];
    rid_array_[i] = rid_array_[i - 1];
  }
  for (int i = 0; i < static_cast<int>(num_tombstones_); i++) {
    if (tombstones_[i] >= static_cast<size_t>(index)) {
      tombstones_[i]++;
    }
  }
  key_array_[index] = key;
  rid_array_[index] = value;
  SetSize(GetSize() + 1);
  return true;
}
FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Split(B_PLUS_TREE_LEAF_PAGE_TYPE *recipient,
                                       const KeyType &key,
                                       const ValueType &value,
                                       const KeyComparator &comparator) {
  // leaf insert 如果页面满载就会compact，保险起见这里再来一次
  Compact();
  // 先把所有条目统一存放在一起
  std::vector<std::pair<KeyType, ValueType>> temp;
  temp.reserve(GetSize() + 1);
  for (int i = 0; i < GetSize(); i++) {
    temp.emplace_back(KeyAt(i), ValueAt(i));
  }
  // 找到插入位置，用标准库insert安全插入新键值对
  int insert_index = KeyIndex(key, comparator);
  temp.insert(temp.begin() + insert_index, {key, value});

  // 对半分配，保证分裂后节点大小合规
  int left_size = temp.size() / 2;
  int right_size = temp.size() - left_size;

  // 填充左节点（当前节点）
  SetSize(0);
  for (int i = 0; i < left_size; i++) {
    key_array_[i] = temp[i].first;
    rid_array_[i] = temp[i].second;
  }
  SetSize(left_size);

  // 填充右节点（recipient）
  recipient->SetSize(0);
  for (int i = left_size, j = 0; i < static_cast<int>(temp.size()); i++, j++) {
    recipient->key_array_[j] = temp[i].first;
    recipient->rid_array_[j] = temp[i].second;
  }
  recipient->SetSize(right_size);
  // std::cout << "DEBUG_SPLIT_CHECK: Left Page Content: [ ";
  // for (int i = 0; i < GetSize(); i++) std::cout << KeyAt(i) << " ";
  // std::cout << "]" << std::endl;
}
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(
    const KeyType &key, const KeyComparator &comparator) -> bool {
  int index = KeyIndex(key, comparator);
  if (index < 0 || index >= GetSize() || comparator(KeyAt(index), key) != 0) {
    return false;
  }
  if (IsTombstone(index)) {
    return false;
  }
  // 判断是否支持墓碑（软删除机制）
  if (LEAF_PAGE_TOMB_CNT > 0) {
    bool index_shifted = false;
    while (num_tombstones_ >= LEAF_PAGE_TOMB_CNT) {
      RemoveOldestTombstone();
      index_shifted = true;
    }
    if (index_shifted) {
      index = KeyIndex(key, comparator);
    }
    // 软删除：标记并增加墓碑数
    tombstones_[num_tombstones_] = index;
    num_tombstones_++;
    return true;
  }
  for (int i = index; i < GetSize() - 1; i++) {
    key_array_[i] = key_array_[i + 1];
    rid_array_[i] = rid_array_[i + 1];
  }
  SetSize(GetSize() - 1);
  return true;
}
FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::PushFront(const KeyType &key,
                                           const ValueType &value) {
  for (int i = GetSize(); i >= 1; i--) {
    key_array_[i] = key_array_[i - 1];
    rid_array_[i] = rid_array_[i - 1];
  }
  key_array_[0] = key;
  rid_array_[0] = value;
  SetSize(GetSize() + 1);
}
FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::PushBack(const KeyType &key,
                                          const ValueType &value) {
  key_array_[GetSize()] = key;
  rid_array_[GetSize()] = value;
  SetSize(GetSize() + 1);
}
FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {
  while (LEAF_PAGE_TOMB_CNT > 0 &&
         recipient->GetTombstonesNum() + num_tombstones_ > LEAF_PAGE_TOMB_CNT) {
    if (recipient->GetTombstonesNum() > 0) {
      // 优先踢掉左节点（recipient）里最老的墓碑
      recipient->RemoveOldestTombstone();
    } else {
      RemoveOldestTombstone();
    }
  }

  int r_size = recipient->GetSize(); // 拿到的绝对是稳定且准确的 size
  for (int i = 0; i < GetSize(); i++) {
    recipient->key_array_[i + r_size] = key_array_[i];
    recipient->rid_array_[i + r_size] = rid_array_[i];
  }
  recipient->SetSize(r_size + GetSize());

  // 搬运墓碑数据
  for (int i = 0; i < static_cast<int>(num_tombstones_); i++) {
    // 此时绝对有空位，且 r_size 绝对精准，不会有任何错位
    recipient->tombstones_[recipient->GetTombstonesNum()] =
        tombstones_[i] + r_size;
    recipient->num_tombstones_++;
  }

  // 清空自己
  SetSize(0);
  num_tombstones_ = 0;

  // 更新兄弟指针
  recipient->SetNextPageId(GetNextPageId());
  SetNextPageId(INVALID_PAGE_ID);
}
FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(
    BPlusTreeLeafPage *recipient) {
  // 右移左
  int first_index = 0;
  KeyType key = KeyAt(first_index);
  ValueType value = ValueAt(first_index);
  bool is_tomb = IsTombstone(first_index);

  int rec_size = recipient->GetSize();
  recipient->key_array_[rec_size] = key;
  recipient->rid_array_[rec_size] = value;
  if (is_tomb) {
    recipient->tombstones_[recipient->num_tombstones_] = rec_size;
    recipient->num_tombstones_++;
  }
  recipient->SetSize(rec_size + 1);

  // 自己的数据整体左移
  for (int i = 1; i < GetSize(); i++) {
    key_array_[i - 1] = key_array_[i];
    rid_array_[i - 1] = rid_array_[i];
  }

  // 自己的墓碑坐标也必须跟着左移（-1），并且丢掉被借走的那个
  int new_tomb_cnt = 0;
  for (size_t i = 0; i < num_tombstones_; i++) {
    if (tombstones_[i] == 0) {
      continue; // 这是被借走的那个，不要了
    }
    tombstones_[new_tomb_cnt] = tombstones_[i] - 1;
    new_tomb_cnt++;
  }
  num_tombstones_ = new_tomb_cnt;
  SetSize(GetSize() - 1);
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(
    BPlusTreeLeafPage *recipient) {
  int last_index = GetSize() - 1;
  KeyType key = KeyAt(last_index);
  ValueType value = ValueAt(last_index);
  bool is_tomb = IsTombstone(last_index);

  int rec_size = recipient->GetSize();

  // recipient 的所有数据先往后挪一个位置
  for (int i = rec_size; i > 0; i--) {
    recipient->key_array_[i] = recipient->key_array_[i - 1];
    recipient->rid_array_[i] = recipient->rid_array_[i - 1];
  }

  for (size_t i = 0; i < recipient->num_tombstones_; i++) {
    recipient->tombstones_[i]++;
  }
  if (is_tomb) {
    // 按照加入时间来算新旧,这算新的
    recipient->tombstones_[recipient->num_tombstones_] =
        0; // 新墓碑放在墓碑数组的第一个
    recipient->num_tombstones_++;
  }

  // 把借来的数据放到 recipient 头部
  recipient->key_array_[0] = key;
  recipient->rid_array_[0] = value;
  recipient->SetSize(rec_size + 1);

  // 更新自己的墓碑，丢掉被借走的那个（原来这部分逻辑是没问题的）
  int new_tomb_cnt = 0;
  for (size_t i = 0; i < num_tombstones_; i++) {
    if (tombstones_[i] == static_cast<size_t>(last_index)) {
      continue;
    }
    tombstones_[new_tomb_cnt] = tombstones_[i];
    new_tomb_cnt++;
  }
  num_tombstones_ = new_tomb_cnt;
  SetSize(GetSize() - 1);
}
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::RealRemove(const KeyType &key,
                                            const KeyComparator &comparator)
    -> bool {
  int index = KeyIndex(key, comparator);
  if (index < 0 || index >= GetSize() || comparator(KeyAt(index), key) != 0) {
    return false;
  }
  if (IsTombstone(index)) {
    return false;
  }
  for (int i = index; i < GetSize() - 1; i++) {
    key_array_[i] = key_array_[i + 1];
    rid_array_[i] = rid_array_[i + 1];
  }
  SetSize(GetSize() - 1);
  return true;
}
FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveOldestTombstone() {
  if (num_tombstones_ == 0) {
    return;
  }

  // 最老的墓碑索引就是 tombstones_[0]
  int index_to_remove = tombstones_[0];

  // 1. 物理删除：将该索引之后的所有数据全部左移一位
  for (int i = index_to_remove; i < GetSize() - 1; i++) {
    key_array_[i] = key_array_[i + 1];
    rid_array_[i] = rid_array_[i + 1];
  }

  // 2. 墓碑队列更新：剔除 tombstones_[0]，后续的墓碑坐标左移
  for (size_t i = 0; i < num_tombstones_ - 1; i++) {
    int next_tomb_index = tombstones_[i + 1];
    // 因为前面的数据被物理删除了，所以排在它后面的墓碑坐标必须 -1
    if (next_tomb_index > index_to_remove) {
      next_tomb_index--;
    }
    tombstones_[i] = next_tomb_index;
  }

  num_tombstones_--;
  SetSize(GetSize() - 1);
}
// ！！！！！！！！显式实例化，h，cpp这种分离编译，需要显式实例化，同时只用显式实例化才能正常声明
template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, 3>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, 2>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, 1>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, -1>;

template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
} // namespace bustub
