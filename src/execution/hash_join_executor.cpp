//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "common/macros.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Construct a new HashJoinExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The HashJoin join plan to be executed
 * @param left_child The child executor that produces tuples for the left side
 * of join
 * @param right_child The child executor that produces tuples for the right side
 * of join
 */
HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for Spring 2025: You ONLY need to implement left join and inner
    // join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  // UNIMPLEMENTED("TODO(P3): Add implementation.");
}

/** Initialize the join */
void HashJoinExecutor::Init() {
  // UNIMPLEMENTED("TODO(P3): Add implementation.");
  left_child_->Init();
  right_child_->Init();
  // 状态机重置
  jht_.clear();
  left_tuple_batch_.clear();
  left_idx_ = 0;
  is_left_done_ = false;
  current_right_matches_.clear();
  right_match_idx_ = 0;
  is_current_left_matched_ = false;

  // 构建阶段：拉取右表数据，直到一滴都不剩
  // 中断管道
  std::vector<Tuple> right_batch;
  std::vector<RID> right_rids;

  // 不断向右表要数据，这里假设一次要 128 条
  while (right_child_->Next(&right_batch, &right_rids, 128)) {
    for (const auto &tuple : right_batch) {
      // 提取这行数据用来Join的列，算Hash的Key
      std::vector<Value> join_keys;
      for (const auto &expr : plan_->RightJoinKeyExpressions()) {
        join_keys.push_back(expr->Evaluate(&tuple, right_child_->GetOutputSchema()));
      }

      // 包装成我们自定义的 HashJoinKey
      HashJoinKey hash_key{join_keys};

      // 塞进哈希表，如果有相同 Key 的，追加到 vector 后面
      jht_[hash_key].push_back(tuple);
    }
    // 准备接下一批右表数据
    right_batch.clear();
    right_rids.clear();
  }
}

/**
 * Yield the next tuple batch from the hash join.
 * @param[out] tuple_batch The next tuple batch produced by the hash join
 * @param[out] rid_batch The next tuple RID batch produced by the hash join
 * @param batch_size The number of tuples to be included in the batch (default:
 * BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto HashJoinExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                            size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();

  const auto &left_schema = left_child_->GetOutputSchema();
  const auto &right_schema = right_child_->GetOutputSchema();

  // 只要没装满，且左表还没扫完，就继续循环
  while (tuple_batch->size() < batch_size && (!is_left_done_ || right_match_idx_ < current_right_matches_.size())) {
    // 情况1：我们手里刚好还有上一轮没吐完的匹配项！
    // 比如左表一条数据匹配了10条右表数据，上一轮只吐了5条就被 batch_size 截断了
    if (right_match_idx_ < current_right_matches_.size()) {
      auto &right_tuple = current_right_matches_[right_match_idx_];

      // 拼接 Schema：左表字段 + 右表字段
      std::vector<Value> values;
      auto &left_tuple = left_tuple_batch_[left_idx_];
      for (uint32_t i = 0; i < left_schema.GetColumnCount(); ++i) {
        values.push_back(left_tuple.GetValue(&left_schema, i));
      }
      for (uint32_t i = 0; i < right_schema.GetColumnCount(); ++i) {
        values.push_back(right_tuple.GetValue(&right_schema, i));
      }

      tuple_batch->emplace_back(Tuple{values, &GetOutputSchema()});
      rid_batch->emplace_back(RID{});

      right_match_idx_++;
      is_current_left_matched_ = true;
      continue;  // 直接进入下一次 while 判断是否满载
    }

    // 走到这里，说明当前左表数据的匹配项已经全部吐完了，我们该处理下一条左表数据了
    // 如果左表本批次游标到底了，去拉取下一批左表数据
    if (left_tuple_batch_.empty() || left_idx_ >= left_tuple_batch_.size() - 1) {
      // 但如果是第一轮，还没拉取过，left_tuple_batch_ 是空的，特殊处理一下
      if (!left_tuple_batch_.empty()) {
        left_tuple_batch_.clear();
      }

      std::vector<RID> dummy_rids;
      bool has_data = false;
      while (left_child_->Next(&left_tuple_batch_, &dummy_rids, 128)) {
        if (!left_tuple_batch_.empty()) {
          has_data = true;
          break;
        }
      }

      if (!has_data) {
        is_left_done_ = true;  // 左表彻底被榨干了
        break;                 // 结束死循环
      }

      left_idx_ = 0;  // 重置游标
    } else {
      left_idx_++;  // 正常推进到下一条
    }
    // 还没有匹配上时
    is_current_left_matched_ = false;
    current_right_matches_.clear();
    right_match_idx_ = 0;

    // 取出最新的左表元组，去哈希表里找
    auto &current_left_tuple = left_tuple_batch_[left_idx_];
    std::vector<Value> left_keys;
    for (const auto &expr : plan_->LeftJoinKeyExpressions()) {
      left_keys.push_back(expr->Evaluate(&current_left_tuple, left_schema));
    }
    HashJoinKey probe_key{left_keys};

    if (jht_.count(probe_key) > 0) {
      // 查到了就把匹配到的右表数组全部提取出来备用
      current_right_matches_ = jht_[probe_key];
      right_match_idx_ = 0;
      // 这里的 continue 非常巧妙，它会跳回到 while 循环开头，
      // 进入上面那个第一个情况，一条条安全地吐出数据！
      continue;
    }

    // 如果没查到，看看是不是 LEFT JOIN，如果是的话要补 NULL
    if (plan_->GetJoinType() == JoinType::LEFT && !is_current_left_matched_) {
      std::vector<Value> values;
      for (uint32_t i = 0; i < left_schema.GetColumnCount(); ++i) {
        values.push_back(current_left_tuple.GetValue(&left_schema, i));
      }

      for (uint32_t i = 0; i < right_schema.GetColumnCount(); ++i) {
        values.push_back(ValueFactory::GetNullValueByType(right_schema.GetColumn(i).GetType()));
      }
      tuple_batch->emplace_back(Tuple{values, &GetOutputSchema()});
      rid_batch->emplace_back(RID{});
    }
  }

  return !tuple_batch->empty();
}

}  // namespace bustub
