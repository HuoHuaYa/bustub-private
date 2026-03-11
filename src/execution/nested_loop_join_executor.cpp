//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "common/macros.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Construct a new NestedLoopJoinExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The nested loop join plan to be executed
 * @param left_executor The child executor that produces tuple for the left side
 * of join
 * @param right_executor The child executor that produces tuple for the right
 * side of join
 */
NestedLoopJoinExecutor::NestedLoopJoinExecutor(
    ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
    std::unique_ptr<AbstractExecutor> &&left_executor,
    std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx), plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT &&
      plan->GetJoinType() != JoinType::INNER) {
    // Note for Spring 2025: You ONLY need to implement left join and inner
    // join.
    throw bustub::NotImplementedException(
        fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  // UNIMPLEMENTED("TODO(P3): Add implementation.");
}

/** Initialize the join */
void NestedLoopJoinExecutor::Init() {
  // UNIMPLEMENTED("TODO(P3): Add implementation.");
  // 初始化底层子算子
  // 所有的算子在被调用 Next 之前，必须先 Init() 以回到数据流的起点。
  left_executor_->Init();
  right_executor_->Init();

  // 重置状态机的所有变量
  // 如果你在这个节点上方有个嵌套循环，这个算子可能会被多次 Init()。
  // 如果你不清空缓存，就会拿到上一次的脏数据！
  left_tuple_batch_.clear();
  right_tuple_batch_.clear();
  left_idx_ = 0;
  right_idx_ = 0;
  is_left_done_ = false;
  is_current_left_matched_ = false;
}

/**
 * Yield the next tuple batch from the join.
 * @param[out] tuple_batch The next tuple batch produced by the join
 * @param[out] rid_batch The next tuple RID batch produced by the join
 * @param batch_size The number of tuples to be included in the batch (default:
 * BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto NestedLoopJoinExecutor::Next(std::vector<bustub::Tuple> *tuple_batch,
                                  std::vector<bustub::RID> *rid_batch,
                                  size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();
  bool emitted = false;
  const auto &predicate = plan_->Predicate();
  // std::cout<<"success predicate\n";
  const auto &left_schema = left_executor_->GetOutputSchema();
  const auto &right_schema = right_executor_->GetOutputSchema();
  // uint32_t emit_cnt = 0;
  // 只要上层要的数据没装满batch_size，且左表还没扫完，就死循环执行
  while (tuple_batch->size() < batch_size && !is_left_done_) {
    // 维护左表的状态（确保手里拿着一条外表数据）
    if (left_idx_ >= left_tuple_batch_.size()) {
      // 当前左表缓存用光了，去拉取下一批左表数据
      left_tuple_batch_.clear();
      std::vector<RID> dummy_rids;
      // 这里的 128 可以随意，是底层每次给你送上来的数量
      while (left_executor_->Next(&left_tuple_batch_, &dummy_rids, 128)) {
        // 如果连左表都没数据了，彻底结束整个 Join 过程
        if (!left_tuple_batch_.empty()) {
          break;
        }
      }
      if (left_tuple_batch_.empty()) {
        is_left_done_ = true;
        break;
      }
      left_idx_ = 0; // 重置左表游标

      // 左表换了新的一行，之前右表的匹配状态全部作废，必须重置！
      is_current_left_matched_ = false;
    }

    // 取出当前的左表元组
    auto &current_left_tuple = left_tuple_batch_[left_idx_];
    // std::string left_tuple_str = current_left_tuple.ToString(&left_schema);

    // 打印左表游标位置和具体内容
    // std::cout << "[DEBUG NLJ] LeftIdx: " << left_idx_
    //       << " | Tuple: " << left_tuple_str
    //       << " | BatchSize: " << left_tuple_batch_.size() << std::endl;
    // 维护右表的状态（确保手里拿着一条内表数据，或者内表转完了一圈）
    // 说明改换新的左表了
    if (right_idx_ >= right_tuple_batch_.size()) {
      right_tuple_batch_.clear();
      std::vector<RID> dummy_rids;

      bool has_right_data = false;
      // 🚨 防御机制：同样使用 while 过滤掉返回 true 但没有数据的幽灵批次
      while (right_executor_->Next(&right_tuple_batch_, &dummy_rids, 128)) {
        if (!right_tuple_batch_.empty()) {
          has_right_data = true;
          break;
        }
      }

      if (!has_right_data || right_tuple_batch_.empty()) {
        // !:内表转完了一整圈拉不到数据了，此时该怎么办？
        // 如果是 LEFT JOIN，且左表这行数据在整个右表里一个都没匹配上
        if (plan_->GetJoinType() == JoinType::LEFT &&
            !is_current_left_matched_) {
          // 如果是左连接，但是没有匹配上，右表就是null
          std::vector<Value> values;

          // 塞入左表数据
          for (uint32_t i = 0; i < left_schema.GetColumnCount(); ++i) {
            values.push_back(current_left_tuple.GetValue(&left_schema, i));
          }
          // 右表全部填 NULL（必须用对应列的正确类型去生成 NULL！）
          for (uint32_t i = 0; i < right_schema.GetColumnCount(); ++i) {
            values.push_back(ValueFactory::GetNullValueByType(
                right_schema.GetColumn(i).GetType()));
          }

          tuple_batch->emplace_back(Tuple{values, &GetOutputSchema()});
          // 虚拟信息，不需要去b+树拿门牌号
          rid_batch->emplace_back(RID{});
          emitted = true;
          // emit_cnt++;
        }

        // 左表推进到下一行
        left_idx_++;
        is_current_left_matched_ = false;

        // 极其重要：把右表复位！
        // 既然左表换了新的一行，右表必须从头再扫一遍！如果不
        // Init()，右表永远是耗尽状态！
        right_executor_->Init();
        right_idx_ = 0;

        // 退出本轮内部循环，外层 while 会自动接管，重新用新的 left_idx 去匹配。
        continue;
      }

      // 如果顺利拉到了右表数据，重置右表批次游标
      right_idx_ = 0;
    }

    // 取出当前的右表元组
    auto &current_right_tuple = right_tuple_batch_[right_idx_];
    bool is_match = true;
    // ！ 计算谓词并生成 Join 元组
    // 获得sql 中 on t1.id = t2.id 这样的信息
    // std::cout<<"useful right tuple "<<" "<<right_idx_<<' '<<left_idx_<<'\n';
    // auto predicate = plan_->Predicate();
    if (predicate != nullptr) {
      // 必须用抽象表达树函数：EvaluateJoin，因为它知道怎么从左、右两个独立的
      // Tuple 和 Schema 里提取字段
      Value match_val = predicate->EvaluateJoin(
          &current_left_tuple, left_schema, &current_right_tuple, right_schema);
      // SQL 里的 NULL 逻辑：如果有任何 NULL 导致结果不是 true，就不算匹配
      is_match = (!match_val.IsNull() && match_val.GetAs<bool>());
    }

    // 前面这么多铺垫，现在才是真正的成功了
    if (is_match) {
      // 只要匹配上了，标记为 true，保证不是null了
      is_current_left_matched_ = true;

      std::vector<Value> values;

      // Schema 拼接规则：左侧表上的所有列，然后是右表上的所有列
      for (uint32_t i = 0; i < left_schema.GetColumnCount(); ++i) {
        values.push_back(current_left_tuple.GetValue(&left_schema, i));
      }
      for (uint32_t i = 0; i < right_schema.GetColumnCount(); ++i) {
        values.push_back(current_right_tuple.GetValue(&right_schema, i));
      }

      tuple_batch->emplace_back(Tuple{values, &GetOutputSchema()});
      rid_batch->emplace_back(RID{}); // Join 操作是虚拟表，无需返回真实 RID
      emitted = true;
      // emit_cnt++;
    }

    // 无论是否匹配，右表都要往下走一格
    right_idx_++;
  }

  // 如果 tuple_batch 里装了哪怕一条数据，就返回 true。如果一滴都不剩了，返回
  // false。
  return emitted;
}

} // namespace bustub
