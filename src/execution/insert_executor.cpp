//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/macros.h"
#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

/**
 * Construct a new InsertExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The insert plan to be executed
 * @param child_executor The child executor from which inserted tuples are
 * pulled
 */
InsertExecutor::InsertExecutor(
    ExecutorContext *exec_ctx, const InsertPlanNode *plan,
    std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan),
      child_executor_(std::move(child_executor)) {
  // UNIMPLEMENTED("TODO(P3): Add implementation.");
}

/** Initialize the insert */
void InsertExecutor::Init() {
  auto calalog = exec_ctx_->GetCatalog();
  table_info_ = calalog->GetTable(plan_->GetTableOid());
  if (table_info_ == nullptr) {
    throw std::runtime_error("INSERT:Table not found in catalog");
  }
  table_indexes_ = calalog->GetTableIndexes(table_info_->name_);
  // 非常重要，没有会导致访问没有初始化的垃圾内存，野指针解引用
  child_executor_->Init();
  has_inserted_ = false;
}

/**
 * Yield the number of rows inserted into the table.
 * 返回插入到表中的行数
 * @param[out] tuple_batch The tuple batch with one integer indicating the
 * number of rows inserted into the table
 * @param[out] rid_batch The next tuple RID batch produced by the insert
 * (ignore, not used)
 * @param batch_size The number of tuples to be included in the batch (default:
 * BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: InsertExecutor::Next() does not use the `rid_batch` out-parameter.
 * NOTE: InsertExecutor::Next() returns true with the number of inserted rows
 * produced only once.
 */
/*
child_executor就是用的valuesexecutor
auto ValuesExecutor::Next(std::vector<bustub::Tuple> *tuple_batch,
std::vector<bustub::RID> *rid_batch, size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();

  while (tuple_batch->size() < batch_size && cursor_ <
plan_->GetValues().size()) { std::vector<Value> values{};
    values.reserve(GetOutputSchema().GetColumnCount());

    const auto &row_expr = plan_->GetValues()[cursor_];
    for (const auto &col : row_expr) {
      values.push_back(col->Evaluate(nullptr, dummy_schema_));
    }

    tuple_batch->emplace_back(values, &GetOutputSchema());
    rid_batch->emplace_back(RID{});
    cursor_ += 1;
  }
  return !tuple_batch->empty();
}
*/
auto InsertExecutor::Next(std::vector<bustub::Tuple> *tuple_batch,
                          std::vector<bustub::RID> *rid_batch,
                          size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();
  if (has_inserted_) {
    return false;
  }
  has_inserted_ = true;
  // 记录有多少行插入了
  int32_t insert_count = 0;

  std::vector<bustub::Tuple> child_tuples;
  std::vector<bustub::RID> child_rids;
  while (child_executor_->Next(&child_tuples, &child_rids, batch_size)) {
    for (const auto &tuple : child_tuples) {
      // 默认构造tuplemeta,时间戳和is_deleted
      TupleMeta meta{0, false};
      // 如果页满了，或者元组太大，可能会插入失败，所以用optional包裹
      auto rid_opt = table_info_->table_->InsertTuple(meta, tuple);
      // 如果成功inserttuple
      if (rid_opt.has_value()) {
        auto rid = rid_opt.value();
        for (const auto &index_info : table_indexes_) {
          // schema是类型，keyfromtuple就是提取出我们要的schema
          // auto Tuple::KeyFromTuple(const Schema &schema, const Schema
          // &key_schema, const std::vector<uint32_t> &key_attrs) const
          //     -> Tuple {
          //   std::vector<Value> values;
          //   values.reserve(key_attrs.size());
          //   for (auto idx : key_attrs) {
          //     values.emplace_back(this->GetValue(&schema, idx));
          //   }
          //   return {values, &key_schema};
          // }
          Tuple key_tuple =
              tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_,
                                 index_info->index_->GetKeyAttrs());
          // 将我们提取出来的key与rid真正的插入b+tree
          index_info->index_->InsertEntry(key_tuple, rid,
                                          exec_ctx_->GetTransaction());
        }
        insert_count++;
      }
    }
    // 清空临时容器
    child_tuples.clear();
    child_rids.clear();
  }
  // 通过tuple_batch去返回插入的行数
  //{}显式的调用默认函数
  std::vector<Value> values{};
  values.reserve(1);
  values.emplace_back(TypeId::INTEGER, insert_count);
  // 他要指针所有有 &
  Tuple result_tuple{values, &GetOutputSchema()};

  tuple_batch->push_back(result_tuple);
  return true;
}

} // namespace bustub
