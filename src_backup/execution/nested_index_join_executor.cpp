//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "common/macros.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Creates a new nested index join executor.
 * @param exec_ctx the context that the nested index join should be performed in
 * @param plan the nested index join plan to be executed
 * @param child_executor the outer table
 */
NestedIndexJoinExecutor::NestedIndexJoinExecutor(
    ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
    std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan),
      child_executor_(std::move(child_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT &&
      plan->GetJoinType() != JoinType::INNER) {
    // Note for Spring 2025: You ONLY need to implement left join and inner
    // join.
    throw bustub::NotImplementedException(
        fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  // UNIMPLEMENTED("TODO(P3): Add implementation.");
}

void NestedIndexJoinExecutor::Init() {
  // UNIMPLEMENTED("TODO(P3): Add implementation.");
  child_executor_->Init();

  // 获取 Catalog 中的右表和索引元数据
  // GetIndexOid() 返回的是规划器分配给这个索引的唯一 ID。
  auto catalog = exec_ctx_->GetCatalog();
  index_info_ = catalog->GetIndex(plan_->GetIndexOid());
  table_info_ = catalog->GetTable(index_info_->table_name_);

  // 清空所有状态
  left_tuple_batch_.clear();
  left_idx_ = 0;
  is_left_done_ = false;

  match_rids_.clear();
  match_idx_ = 0;
  is_current_left_matched_ = false;
}
// 判断有没有处理好的rid没有装走，然后判断rid全没有，left 就要输出null的情况
// 最后就是正常的lefttuple里寻找配对的rid
auto NestedIndexJoinExecutor::Next(std::vector<bustub::Tuple> *tuple_batch,
                                   std::vector<bustub::RID> *rid_batch,
                                   size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();
  bool emitted = false;
  const auto &left_schema = child_executor_->GetOutputSchema();
  const auto &right_schema = table_info_->schema_;
  // uint32_t emit_cnt = 0;
  // std::cout<<"next\nnext\nnext\n";
  // assert(static_cast<int>(emit_cnt) < 0);
  while (tuple_batch->size() < batch_size && !is_left_done_) {

    // 阶段一：处理当前左表元组查出来的一堆 RID
    // 为什么我不直接while把数组所有的处理完呢
    // 因为一次元组数量有限，不一定卸的完
    if (match_idx_ < match_rids_.size()) {
      // 拿着当前门牌号去Table Heap里拉取右表的真实数据
      RID current_rid = match_rids_[match_idx_];
      auto [meta, right_tuple] = table_info_->table_->GetTuple(current_rid);

      // 拼装 Tuple 的逻辑和之前一模一样
      std::vector<Value> values;
      auto &current_left_tuple = left_tuple_batch_[left_idx_];

      for (uint32_t i = 0; i < left_schema.GetColumnCount(); ++i) {
        values.push_back(current_left_tuple.GetValue(&left_schema, i));
      }
      for (uint32_t i = 0; i < right_schema.GetColumnCount(); ++i) {
        values.push_back(right_tuple.GetValue(&right_schema, i));
      }

      tuple_batch->emplace_back(Tuple{values, &GetOutputSchema()});
      rid_batch->emplace_back(RID{});
      emitted = true;
      // emit_cnt++;
      is_current_left_matched_ = true; // 匹配成功！

      match_idx_++;
      continue; // 优先把这一批 RID 处理完
    }

    // 阶段二：当前的 RID 全处理完了（或者本来就是空的），判断是否需要补 NULL

    if (!left_tuple_batch_.empty()) {
      // 如果这不是循环的第一次运行，说明刚才那个左表元组所有的后事都料理完了。
      // 我们需要拷问它：LEFT JOIN 且没匹配上
      if (plan_->GetJoinType() == JoinType::LEFT && !is_current_left_matched_) {
        std::vector<Value> values;
        auto &current_left_tuple = left_tuple_batch_[left_idx_];

        for (uint32_t i = 0; i < left_schema.GetColumnCount(); ++i) {
          values.push_back(current_left_tuple.GetValue(&left_schema, i));
        }
        for (uint32_t i = 0; i < right_schema.GetColumnCount(); ++i) {
          values.push_back(ValueFactory::GetNullValueByType(
              right_schema.GetColumn(i).GetType()));
        }

        tuple_batch->emplace_back(Tuple{values, &GetOutputSchema()});
        rid_batch->emplace_back(RID{});
        emitted = true;
        // emit_cnt++;
      }

      // 左表游标往下走一格
      left_idx_++;
    }

    // 阶段三：去左表拉取下一个元组，并去 B+ 树里搜！

    if (left_idx_ >= left_tuple_batch_.size()) {
      left_tuple_batch_.clear();
      std::vector<RID> dummy_rids;

      while (child_executor_->Next(&left_tuple_batch_, &dummy_rids, 128)) {
        if (!left_tuple_batch_.empty()) {
          break;
        }
      }
      if (left_tuple_batch_.empty()) {
        is_left_done_ = true;
        break; // 彻底结束
      }
      left_idx_ = 0;
    }

    auto &current_left_tuple = left_tuple_batch_[left_idx_];
    is_current_left_matched_ = false;
    std::string left_tuple_str = current_left_tuple.ToString(&left_schema);
    // std::cout << "[DEBUG NLJ] LeftIdx: " << left_idx_
    //       << " | Tuple: " << left_tuple_str
    //       << " | BatchSize: " << left_tuple_batch_.size() << std::endl;
    // B+ 树搜寻核心逻辑
    // 获取查询条件：比如 ON t1.id = t2.id，这行代码会提取出 t1.id 的值
    Value probe_value =
        plan_->KeyPredicate()->Evaluate(&current_left_tuple, left_schema);

    // 构造用来查询的 Tuple：BusTub 的索引要求你传进去的 Key 也是一个 Tuple 格式
    std::vector<Value> probe_values{probe_value};
    Tuple probe_tuple{probe_values, index_info_->index_->GetKeySchema()};

    // 去 B+ 树里查出来的所有 RID 会被填充进 match_rids_
    match_rids_.clear();
    index_info_->index_->ScanKey(probe_tuple, &match_rids_,
                                 exec_ctx_->GetTransaction());

    match_idx_ = 0;
  }

  return emitted;
}

} // namespace bustub
