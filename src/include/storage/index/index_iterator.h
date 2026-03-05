//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_iterator.h
//
// Identification: src/include/storage/index/index_iterator.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include <utility>
#include "buffer/traced_buffer_pool_manager.h"
#include "common/config.h"
#include "common/macros.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator, NumTombs>
#define SHORT_INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

FULL_INDEX_TEMPLATE_ARGUMENTS_DEFN
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator(TracedBufferPoolManager *buffer_pool_manager, page_id_t page_id, int index, ReadPageGuard &&guard);
  IndexIterator() = default;
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> std::pair<const KeyType, const ValueType>;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    if (current_page_id_ == INVALID_PAGE_ID && itr.current_page_id_ == INVALID_PAGE_ID) {
      return true;
    }
    return current_page_id_ == itr.current_page_id_ && current_index_ == itr.current_index_;
  }

  auto operator!=(const IndexIterator &itr) const -> bool { return !(*this == itr); }
  void Page();

 private:
  // add your own private member variables here
  TracedBufferPoolManager *bpm_{nullptr};
  page_id_t current_page_id_{INVALID_PAGE_ID};
  int current_index_{-1};
  ReadPageGuard guard_;
};

}  // namespace bustub
