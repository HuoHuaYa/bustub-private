//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include "common/macros.h"

#include "execution/executors/delete_executor.h"

namespace bustub {

/**
 * Construct a new DeleteExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The delete plan to be executed
 * @param child_executor The child executor that feeds the delete
 */
DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

/** Initialize the delete */
void DeleteExecutor::Init() {
  auto calalog = exec_ctx_->GetCatalog();
  table_info_ = calalog->GetTable(plan_->GetTableOid());
  if (table_info_ == nullptr) {
    throw std::runtime_error("DELETE : not found ");
  }
  table_indexes_ = calalog->GetTableIndexes(table_info_->name_);

  child_executor_->Init();
  has_deleted_ = false;
}

/**
 * Yield the number of rows deleted from the table.
 * @param[out] tuple_batch The tuple batch with one integer indicating the
 * number of rows deleted from the table
 * @param[out] rid_batch The next tuple RID batch produced by the delete
 * (ignore, not used)
 * @param batch_size The number of tuples to be included in the batch (default:
 * BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: DeleteExecutor::Next() does not use the `rid_batch` out-parameter.
 * NOTE: DeleteExecutor::Next() returns true with the number of deleted rows
 * produced only once.
 */
auto DeleteExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                          size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();

  if (has_deleted_) {
    return false;
  }
  has_deleted_ = true;

  int32_t delete_count = 0;
  std::vector<bustub::Tuple> child_tuples;
  std::vector<bustub::RID> child_rids;

  while (child_executor_->Next(&child_tuples, &child_rids, batch_size)) {
    for (size_t i = 0; i < child_tuples.size(); i++) {
      const auto &tuple = child_tuples[i];
      const auto &rid = child_rids[i];

      TupleMeta meta = table_info_->table_->GetTupleMeta(rid);
      meta.is_deleted_ = true;
      table_info_->table_->UpdateTupleMeta(meta, rid);
      for (const auto &index_info : table_indexes_) {
        // AI:GetKeyAttrs() 的全称是 Get Key Attributes（获取键属性 /
        // 获取索引列坐标）
        // 如果用一句大白话来解释：它就是一张“精确到列的采摘清单”，告诉系统到底该从完整的表中，把哪几列抽出来作为
        // B+ 树的 Key。
        Tuple key =
            tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        // 物理删除
        index_info->index_->DeleteEntry(key, rid, exec_ctx_->GetTransaction());
      }

      delete_count++;
    }
    child_tuples.clear();
    child_rids.clear();
  }

  std::vector<Value> result_values{};
  result_values.emplace_back(TypeId::INTEGER, delete_count);

  Tuple result_tuple{result_values, &GetOutputSchema()};
  tuple_batch->push_back(result_tuple);
  return true;
}

}  // namespace bustub
