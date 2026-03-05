//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/macros.h"
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

/**
 * Construct a new UpdateExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The update plan to be executed
 * @param child_executor The child executor that feeds the update
 */
UpdateExecutor::UpdateExecutor(
    ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
    std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), table_info_(nullptr),
      child_executor_(std::move(child_executor)) {}

/** Initialize the update */
void UpdateExecutor::Init() {
  auto catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->GetTableOid());
  if (table_info_ == nullptr) {
    throw std::runtime_error("UPDATE : Table not found in catalog");
  }
  table_indexes_ = catalog->GetTableIndexes(table_info_->name_);
  child_executor_->Init();
  has_updated_ = false;
}

/**
 * Yield the number of rows updated in the table.
 * @param[out] tuple_batch The tuple batch with one integer indicating the
 * number of rows updated in the table
 * @param[out] rid_batch The next tuple RID batch produced by the update
 * (ignore, not used)
 * @param batch_size The number of tuples to be included in the batch (default:
 * BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: UpdateExecutor::Next() does not use the `rid_batch` out-parameter.
 * NOTE: UpdateExecutor::Next() returns true with the number of updated rows
 * produced only once.
 */
auto UpdateExecutor::Next(std::vector<bustub::Tuple> *tuple_batch,
                          std::vector<bustub::RID> *rid_batch,
                          size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();

  if (has_updated_) {
    return false;
  }
  has_updated_ = true;

  int32_t update_count = 0;
  // 存放挖出来的数据和地址
  std::vector<bustub::Tuple> child_tuples;
  std::vector<bustub::RID> child_rids;
  // 底层矿工，遍历表把数据挖出来
  while (child_executor_->Next(&child_tuples, &child_rids, batch_size)) {
    for (size_t i = 0; i < child_tuples.size(); i++) {

      const auto &old_tuple = child_tuples[i];
      const auto &old_rid = child_rids[i];
      // 装老数据的容器
      std::vector<Value> values;
      // child_executor_ :ValuesExecutor : public AbstractExecutor
      //  outputschema return valueplan->GetOutputSchema()
      //  outputshcema auto OutputSchema() const -> const Schema & { return
      //  *output_schema_; } 从父类abstarctplannode继承而来 Schema ： auto
      //  GetColumnCount() const -> uint32_t { return
      //  static_cast<uint32_t>(columns_.size()); }
      values.reserve(child_executor_->GetOutputSchema().GetColumnCount());
      // updatePlannode中： std::vector<AbstractExpressionRef>
      // target_expressions_; 用来进行表达式计算
      for (const auto &expr : plan_->target_expressions_) {
        // 对老元组的每一列进行计算，比如 age = age + 1
        // 通过多态的override，得到动态分发的效果
        values.push_back(
            expr->Evaluate(&old_tuple, child_executor_->GetOutputSchema()));
      }
      Tuple new_tuple{values, &table_info_->schema_};

      // 标记删除老元组
      // 先获取它原本的门牌号信息，把墓碑（is_deleted_）打上，然后写回磁盘
      TupleMeta old_meta = table_info_->table_->GetTupleMeta(old_rid);
      old_meta.is_deleted_ = true;
      // table heap中的void TableHeap::UpdateTupleMeta(const TupleMeta &meta,
      // RID rid) { auto page_guard = bpm_->WritePage(rid.GetPageId()); auto
      // page = page_guard.AsMut<TablePage>(); page->UpdateTupleMeta(meta, rid);
      //
      // void TablePage::UpdateTupleMeta(const TupleMeta &meta, const RID &rid)
      // { auto tuple_id = rid.GetSlotNum(); if (tuple_id >= num_tuples_) {
      //   throw bustub::Exception("Tuple ID out of range");
      //   }
      // auto &[offset, size, old_meta] = tuple_info_[tuple_id];
      // if (!old_meta.is_deleted_ && meta.is_deleted_) {
      //   num_deleted_tuples_++;
      // }
      // tuple_info_[tuple_id] = std::make_tuple(offset, size, meta);
      // }

      // AI：标记删除 - 将 TupleMeta 的 is_deleted_ 设置为 true，表示该 tuple
      // 已被删除 更新时间戳 - 设置 ts_ 字段（用于 MVCC 事务管理） 物理更新 -
      // 修改页面上的元数据存储
      table_info_->table_->UpdateTupleMeta(old_meta, old_rid);
      TupleMeta new_meta{0, false}; // 伪造新户口本
      auto new_rid_opt = table_info_->table_->InsertTuple(new_meta, new_tuple);
      // 新标识也有可能创建失败
      if (new_rid_opt.has_value()) {
        auto new_rid = new_rid_opt.value();

        for (const auto &index_info : table_indexes_) {
          // 提取老 Key，从 B+ 树里抹除
          Tuple old_key = old_tuple.KeyFromTuple(
              table_info_->schema_, index_info->key_schema_,
              index_info->index_->GetKeyAttrs());
          index_info->index_->DeleteEntry(old_key, old_rid,
                                          exec_ctx_->GetTransaction());

          // 提取新 Key，在 B+ 树里登记
          Tuple new_key = new_tuple.KeyFromTuple(
              table_info_->schema_, index_info->key_schema_,
              index_info->index_->GetKeyAttrs());
          index_info->index_->InsertEntry(new_key, new_rid,
                                          exec_ctx_->GetTransaction());
        }
        update_count++;
      }
    }
    child_tuples.clear();
    child_rids.clear();
  }

  // 要返回一行：成功更新的行数
  std::vector<Value> result_values{};
  result_values.emplace_back(TypeId::INTEGER, update_count);
  Tuple result_tuple{result_values, &GetOutputSchema()};
  tuple_batch->push_back(result_tuple);

  return true;
}

} // namespace bustub
