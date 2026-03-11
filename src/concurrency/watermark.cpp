//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// watermark.cpp
//
// Identification: src/concurrency/watermark.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/watermark.h"
#include <algorithm>
#include <exception>
#include "common/exception.h"
namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }

  // 在哈希表中增加活跃计数
  current_reads_[read_ts]++;

  // 将时间戳推入最小堆
  min_heap_.push(read_ts);

  // 更新当前水位线为堆顶最小值
  watermark_ = min_heap_.top();
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  // 如果找不到，直接返回
  if (current_reads_.find(read_ts) == current_reads_.end()) {
    return;
  }
  current_reads_[read_ts]--;
  if (current_reads_[read_ts] == 0) {
    current_reads_.erase(read_ts);
  }
  // 只要堆顶的元素在哈希表里找不到（说明它已经被 erase 掉了，是个死人）
  // 千万不要用 current_reads_[top] == 0 判断，会重新插入一个值为 0 的废键
  while (!min_heap_.empty() && current_reads_.find(min_heap_.top()) == current_reads_.end()) {
    min_heap_.pop();
  }
  if (min_heap_.empty()) {
    // 没活人了喵
    watermark_ = commit_ts_;
  } else {
    // 还有活人，堆顶就是最老的那个活人
    watermark_ = min_heap_.top();
  }
}

}  // namespace bustub
