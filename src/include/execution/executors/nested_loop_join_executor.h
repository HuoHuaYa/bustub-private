//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.h
//
// Identification: src/include/execution/executors/nested_loop_join_executor.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * NestedLoopJoinExecutor executes a nested-loop JOIN on two tables.
 */
class NestedLoopJoinExecutor : public AbstractExecutor {
 public:
  NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                         std::unique_ptr<AbstractExecutor> &&left_executor,
                         std::unique_ptr<AbstractExecutor> &&right_executor);

  void Init() override;

  auto Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch, size_t batch_size)
      -> bool override;

  /** @return The output schema for the insert */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** The NestedLoopJoin plan node to be executed. */
  const NestedLoopJoinPlanNode *plan_;

  // 为什么必须在这里定义状态变量？
  // 在 Vectorized Execution Model 下，你的 Next() 函数每次最多只能返回
  // batch_size 个元组！ 这意味着函数会不断地 return
  // 并退出！如果你用局部变量来写双重 for 循环，
  // 函数一退出，循环的游标（查到了左表第几行、右表第几行）就全部丢失了！
  // 所以，你必须把它们提取成类的成员变量，把执行器彻底变成一个【状态机】

  // 子执行器指针
  // 通过构造函数传入。
  // 调用 left_executor_->Next() 拉取外表数据，调用 right_executor_->Next()
  // 拉取内表数据。
  std::unique_ptr<AbstractExecutor> left_executor_;
  std::unique_ptr<AbstractExecutor> right_executor_;

  // 左表（外表）的状态缓存
  // 是什么：保存从底层一次性拉取上来的左表数据批次，以及当前遍历到的索引。
  std::vector<Tuple> left_tuple_batch_;
  size_t left_idx_{0};        // 当前正在处理 left_tuple_batch_ 中的第几个元组
  bool is_left_done_{false};  // 左表是否已经彻底查到底了

  // 右表（内表）的状态缓存
  // 保存从底层拉取上来的右表数据批次，以及当前遍历到的索引。
  std::vector<Tuple> right_tuple_batch_;
  size_t right_idx_{0};  // 当前正在处理 right_tuple_batch_ 中的第几个元组

  // Left Join 专用匹配标记
  // 记录当前的左表元组在遍历整个右表的过程中，是否至少匹配成功过一次。
  // Left Join 的硬性要求是：如果左边的一行在右边转了一整圈都没找到匹配的，

  bool is_current_left_matched_{false};
};

}  // namespace bustub
