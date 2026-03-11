//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.h
//
// Identification: src/include/execution/executors/nested_index_join_executor.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/nested_index_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * NestedIndexJoinExecutor executes index join operations.
 */
class NestedIndexJoinExecutor : public AbstractExecutor {
 public:
  NestedIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                          std::unique_ptr<AbstractExecutor> &&child_executor);

  /** @return The output schema for the nested index join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  void Init() override;

  auto Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch, size_t batch_size)
      -> bool override;

 private:
  /** The nested index join plan node. */
  const NestedIndexJoinPlanNode *plan_;

  // 唯一的一个子算子（左表）
  std::unique_ptr<AbstractExecutor> child_executor_;

  // 右表的元数据
  // 从 Catalog 里拿出来的表和索引信息。
  // 拿着左边提炼出的 Key 去 index_info_->index_ 里搜出门牌号，
  // 然后去 table_info_->table_ 里把右表的真实数据揪出来。
  std::shared_ptr<TableInfo> table_info_;
  std::shared_ptr<IndexInfo> index_info_;

  // 左表状态缓存
  std::vector<Tuple> left_tuple_batch_;
  size_t left_idx_{0};
  bool is_left_done_{false};

  // 右表结果缓存
  // 因为一个 Key 可能会在 B+ 树里查出多个 RID（比如重名的人）。
  // 当这批 RID 没处理完时，如果输出批次满了，我们得记住当前处理到了第几个 RID。
  std::vector<RID> match_rids_;
  size_t match_idx_{0};
  bool is_current_left_matched_{false};
};
}  // namespace bustub
