//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_iterator.cpp
//
// Identification: src/storage/index/index_iterator.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * index_iterator.cpp
 */
#include "storage/index/index_iterator.h"

#include <cassert>

namespace bustub {

/**
 * @note you can change the destructor/constructor method here
 * set your own input parameters
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(TracedBufferPoolManager *buffer_pool_manager,
                                  page_id_t page_id, int index,
                                  ReadPageGuard &&guard)
    : bpm_(buffer_pool_manager), current_page_id_(page_id),
      current_index_(index), guard_(std::move(guard)) {
  if (current_page_id_ != INVALID_PAGE_ID) {
    // 是不是有效index
    auto leaf_page = guard_.template As<
        BPlusTreeLeafPage<KeyType, ValueType, KeyComparator, NumTombs>>();
    if (current_index_ >= leaf_page->GetSize() ||
        leaf_page->IsTombstone(current_index_)) {
      operator++();
    }
  }
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  return current_page_id_ == INVALID_PAGE_ID;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default; // NOLINT

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*()
    -> std::pair<const KeyType, const ValueType> {
  auto leaf_page = guard_.template As<
      BPlusTreeLeafPage<KeyType, ValueType, KeyComparator, NumTombs>>();
  return {leaf_page->KeyAt(current_index_), leaf_page->ValueAt(current_index_)};
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (IsEnd()) {
    return *this;
  }
  current_index_++;
  // 遇到正常数据break，没路可以走了break
  while (!IsEnd()) {
    auto leaf_page = guard_.template As<
        BPlusTreeLeafPage<KeyType, ValueType, KeyComparator, NumTombs>>();
    // 走出当前页面
    // std::cout<<leaf_page->GetSize()<<'\n';

    if (current_index_ >= leaf_page->GetSize()) {
      page_id_t next_page_id = leaf_page->GetNextPageId();
      if (next_page_id == INVALID_PAGE_ID) {
        guard_.Drop();
        current_page_id_ = INVALID_PAGE_ID;
        current_index_ = 0;
        break;
      }
      // nextpageid可能被其他线程修改，所以我们要在guard被drop之前定义guard
      // std::cout<<"Success\n";
      auto next_guard = bpm_->ReadPage(next_page_id);
      guard_.Drop();
      guard_ = std::move(next_guard);
      current_index_ = 0;
      current_page_id_ = next_page_id;
      continue;
    }
    // 判断是不是墓碑
    auto is_tombstones = leaf_page->IsTombstone(current_index_);
    if (is_tombstones) {
      current_index_++;
      continue;
    }
    break;
  }
  return *this;
}
FULL_INDEX_TEMPLATE_ARGUMENTS
void INDEXITERATOR_TYPE::Page() {
  std::cout << "PAGE!!!!!!!!!\n"
            << current_page_id_ << ' ' << current_index_ << '\n';
  auto page = guard_.template As<
      BPlusTreeLeafPage<KeyType, ValueType, KeyComparator, NumTombs>>();
  std::cout << page->KeyAt(0) << ' ' << page->ValueAt(0) << '\n';
  std::cout << page->KeyAt(1) << ' ' << page->ValueAt(1) << '\n';

  std::cout << "-----\n";
}
template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, 3>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, 2>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, 1>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, -1>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace bustub
