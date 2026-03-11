//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// watermark.h
//
// Identification: src/include/concurrency/watermark.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <queue>
#include <unordered_map>
#include <vector>

#include "concurrency/transaction.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * @brief tracks all the read timestamps.
 *
 */
class Watermark {
public:
  explicit Watermark(timestamp_t commit_ts)
      : commit_ts_(commit_ts), watermark_(commit_ts) {}

  auto AddTxn(timestamp_t read_ts) -> void;

  auto RemoveTxn(timestamp_t read_ts) -> void;

  /** The caller should update commit ts before removing the txn from the
   * watermark so that we can track watermark correctly. */
  auto UpdateCommitTs(timestamp_t commit_ts) { commit_ts_ = commit_ts; }

  auto GetWatermark() -> timestamp_t {
    if (current_reads_.empty()) {
      return commit_ts_;
    }
    return watermark_;
  }
  // 整个数据库中，最后一次成功提交的事务的时间戳
  timestamp_t commit_ts_;
  // 最老资历，因为我们维护的空间有限
  timestamp_t watermark_;
  // 有多少事务在用这些时间戳
  std::unordered_map<timestamp_t, int> current_reads_;
  // 时间戳自增，直接找最小值就行了
  std::priority_queue<timestamp_t, std::vector<timestamp_t>,
                      std::greater<timestamp_t>>
      min_heap_;
};

}; // namespace bustub
