//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {
struct HashJoinKey {
  std::vector<Value> join_keys_;
  auto operator==(const HashJoinKey &other) const -> bool {
    if (join_keys_.size() != other.join_keys_.size()) {
      return false;
    }
    for (size_t i = 0; i < join_keys_.size(); i++) {
      if (join_keys_[i].CompareEquals(other.join_keys_[i]) !=
          CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};
} // namespace bustub

namespace std {

template <> struct hash<bustub::HashJoinKey> {
  auto operator()(const bustub::HashJoinKey &key) const -> std::size_t {
    size_t curr_hash = 0;
    for (const auto &v : key.join_keys_) {
      if (!v.IsNull()) {
        // 使用 BusTub 底层提供的哈希工具，把每一列的哈希值揉在一起
        curr_hash = bustub::HashUtil::CombineHashes(
            curr_hash, bustub::HashUtil::HashValue(&v));
      }
    }
    return curr_hash;
  }
};
} // namespace std
  // namespace std
namespace bustub {
/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
public:
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child,
                   std::unique_ptr<AbstractExecutor> &&right_child);

  void Init() override;

  auto Next(std::vector<bustub::Tuple> *tuple_batch,
            std::vector<bustub::RID> *rid_batch, size_t batch_size)
      -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override {
    return plan_->OutputSchema();
  };

private:
  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  // 保存左右两个子执行器机器搬运工
  std::unique_ptr<AbstractExecutor> left_child_;
  std::unique_ptr<AbstractExecutor> right_child_;

  // 建在右表上的哈希表
  // Key: 提取出来的 Join 字段，Value: 所有拥有相同 Key 的右表 Tuple 数组
  std::unordered_map<HashJoinKey, std::vector<Tuple>> jht_;
  // 经典套件
  //  状态机变量,配合 batch_size 分段吐数据的要求
  std::vector<Tuple> left_tuple_batch_;
  size_t left_idx_{0};
  bool is_left_done_{false};

  // 当左表的一行数据，在哈希表里匹配到了多条右表数据时，用来记录当前吐到哪一条了
  std::vector<Tuple> current_right_matches_;
  size_t right_match_idx_{0};
  bool is_current_left_matched_{false};
};

} // namespace bustub
