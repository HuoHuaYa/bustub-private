//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_internal_page.cpp
//
// Identification: src/storage/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/b_plus_tree_internal_page.h"

#include <iostream>
#include <sstream>

#include "common/exception.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * @brief Init method after creating a new internal page.
 *
 * Writes the necessary header information to a newly created page,
 * including set page type, set current size, set page id, set parent id and set
 * max page size, must be called after the creation of a new page to make a
 * valid BPlusTreeInternalPage.
 *
 * @param max_size Maximal size of the page
 */
// 创建新的内部页面的初始化方式。
// 为了让这个内部页面合法化必须有页面类型，当前大小，页面id，父亲id，最大页面大小
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  // page type
  SetPageType(IndexPageType::INTERNAL_PAGE);
  // current size
  SetSize(0);
  // page id

  // parent id

  // max size
  SetMaxSize(max_size);
}
//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!存疑：这里根本就没有任何pageid
//! ，以及parentid的信息，这个初始化ai说不需要，目前没有办法先往下写

/**
 * @brief Helper method to get/set the key associated with input "index"(a.k.a
 * array offset).
 *
 * @param index The index of the key to get. Index must be non-zero.
 * @return Key at index
 */
INDEX_TEMPLATE_ARGUMENTS
// 查键值
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  assert(index > 0);
  return key_array_[index];
}

/**
 * @brief Set key at the specified index.
 *
 * @param index The index of the key to set. Index must be non-zero.
 * @param key The new value for key
 */
// 修改键
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  assert(index > 0);
  key_array_[index] = key;
}

/**
 * @brief Helper method to get the value associated with input "index"(a.k.a
 * array offset)
 *
 * @param index The index of the value to get.
 * @return Value at index
 */
// 索引对应的值
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  return page_id_array_[index];
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index,
                                                const ValueType &value) {
  page_id_array_[index] = value;
}
// ValueType :page id
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(
    const KeyType &key, const KeyComparator &comparator) const -> ValueType {
  // 第一位无效，因此范围如下
  auto start = key_array_ + 1;
  auto end = start + GetSize() - 1;
  // 为什么二分：因为page最大size可能会很多，几百个的话二分更优
  // 为什么使用upper后再-1
  // 首先我们要明白internal是充当路标，因此本身不是值。他只能确定一个值的范围。
  // 如果当前要找的值是15,我们当前page为10 ， 20，30路标意味着：<=10 , 10 < ...
  // <= 20,20 <... <=30可以用lower直接求 但是反着来设置最小值 <10,10<= ... < 20
  // , 20<=........，
  //  再这么求就会某些情况跑偏，届时还要判断是否-1很麻烦。因此upper - 1为最优解
  auto target = std::upper_bound(start, end, key,
                                 [&](const KeyType &k1, const KeyType &k2) {
                                   return comparator(k1, k2) < 0;
                                 });
  int index = std::distance(key_array_, target);
  return ValueAt(index - 1);
}
// 下层的page，分裂出一个新的page，添加给父亲
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value,
                                                     const KeyType &new_key,
                                                     const ValueType &new_value)
    -> int {
  // old page 的索引位置
  int old_index = ValueIndex(old_value);
  if (old_index == -1) {
    return -1;
  }
  int new_index = old_index + 1;
  // 封装改，好debug
  for (int i = GetSize(); i > new_index; i--) {
    SetKeyAt(i, KeyAt(i - 1));
    SetValueAt(i, ValueAt(i - 1));
  }
  SetKeyAt(new_index, new_key);
  SetValueAt(new_index, new_value);
  SetSize(GetSize() + 1);
  return GetSize();
}
// 根分裂后有两个分支，对应两个路标
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value) {
  // key 0 是无效的，不需要赋值
  SetValueAt(0, old_value);
  SetKeyAt(1, new_key);
  SetValueAt(1, new_value);

  SetSize(2);
}
// 内部节点分裂
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Split(
    B_PLUS_TREE_INTERNAL_PAGE_TYPE *recipient, KeyType *split_key) {
  int split_idx = GetSize() / 2;
  // 传给父亲的新键
  *split_key = KeyAt(split_idx);
  // 开始给新节点赋值
  recipient->SetValueAt(0, ValueAt(split_idx));
  int move_count = GetSize() - split_idx - 1;
  for (int i = 0; i < move_count; i++) {
    recipient->SetKeyAt(i + 1, KeyAt(split_idx + 1 + i));
    recipient->SetValueAt(i + 1, ValueAt(split_idx + 1 + i));
  }
  recipient->SetSize(1 + move_count);
  SetSize(split_idx);
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const
    -> int {
  for (int i = 0; i < GetSize(); i++) {
    if (page_id_array_[i] == value) {
      return i;
    }
  }
  return -1;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PushFront(const KeyType &key,
                                               const ValueType &value) {
  for (int i = GetSize(); i >= 1; i--) {
    key_array_[i] = key_array_[i - 1];
    page_id_array_[i] = page_id_array_[i - 1];
  }
  key_array_[1] = key;
  page_id_array_[0] = value;
  SetSize(GetSize() + 1);
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PushBack(const KeyType &key,
                                              const ValueType &value) {
  key_array_[GetSize()] = key;
  page_id_array_[GetSize()] = value;
  SetSize(GetSize() + 1);
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient,
                                               const KeyType &middle_key) {
  // recipient 是左儿子
  int index = recipient->GetSize();
  recipient->key_array_[index] = middle_key;
  recipient->page_id_array_[index] = page_id_array_[0];
  for (int i = 1; i < GetSize(); i++) {
    recipient->key_array_[index + i] = key_array_[i];
    recipient->page_id_array_[index + i] = page_id_array_[i];
  }
  recipient->SetSize(GetSize() + recipient->GetSize());
  SetSize(0);
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  for (int i = index + 1; i < GetSize(); i++) {
    key_array_[i - 1] = key_array_[i];
    page_id_array_[i - 1] = page_id_array_[i];
  }
  SetSize(GetSize() - 1);
}
// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t,
                                     GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t,
                                     GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t,
                                     GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t,
                                     GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t,
                                     GenericComparator<64>>;
} // namespace bustub
