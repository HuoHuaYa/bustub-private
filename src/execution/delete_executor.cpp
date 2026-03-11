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

#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
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

  // 保证算子只执行一次
  if (has_deleted_) {
    return false;
  }
  has_deleted_ = true;

  auto *txn = exec_ctx_->GetTransaction();
  auto *txn_mgr = exec_ctx_->GetTransactionManager();
  int32_t delete_count = 0;
  // 流水线中断器
  std::vector<Tuple> child_tuples;
  std::vector<RID> child_rids;
  std::vector<Tuple> buffered_tuples;
  std::vector<RID> buffered_rids;

  while (child_executor_->Next(&child_tuples, &child_rids, batch_size)) {
    for (size_t i = 0; i < child_tuples.size(); i++) {
      buffered_tuples.push_back(child_tuples[i]);
      buffered_rids.push_back(child_rids[i]);
    }
    child_tuples.clear();
    child_rids.clear();
  }

  // 遍历缓冲区，物理层面删除

  for (size_t i = 0; i < buffered_tuples.size(); i++) {
    RID rid = buffered_rids[i];
    // 给新的log准备成员
    // tuple可能在之前有改变，获取新鲜的
    auto [meta, base_tuple] = table_info_->table_->GetTuple(rid);
    std::optional<UndoLink> old_link = txn_mgr->GetUndoLink(rid);
    // 宏观写写冲突检测
    bool is_self = (meta.ts_ == txn->GetTransactionTempTs());

    if (!is_self) {
      if (meta.ts_ > txn->GetReadTs() || (meta.ts_ > TXN_START_ID && meta.ts_ != txn->GetTransactionTempTs())) {
        txn->SetTainted();
        throw ExecutionException("Write-Write Conflict in Delete!");
      }
    }

    // 构造meta
    TupleMeta new_meta = meta;
    new_meta.is_deleted_ = true;
    new_meta.ts_ = txn->GetTransactionTempTs();

    std::optional<UndoLink> link_to_update = old_link;
    // 不是写写冲突，并且不是当前事务改的，就是第一次改，要更新meta，ts
    if (!is_self) {
      UndoLog new_log;
      // 之前是死是活
      new_log.is_deleted_ = meta.is_deleted_;
      // 删除操作必须全量保存旧数据，位图全设为true
      new_log.modified_fields_ = std::vector<bool>(child_executor_->GetOutputSchema().GetColumnCount(), true);
      // 必须保存完整的tuple（内含rid）
      new_log.tuple_ = base_tuple;
      new_log.ts_ = meta.ts_;
      // 有老版本，就记得带上
      if (old_link.has_value()) {
        new_log.prev_version_ = *old_link;
      }
      // 追加到事务内存，拿到新指针
      link_to_update = txn->AppendUndoLog(new_log);

    } else {
      if (old_link.has_value()) {
        UndoLog old_log = txn->GetUndoLog(old_link->prev_log_idx_);
        // 说明之前不是insert，而是update
        if (!old_log.is_deleted_) {
          std::optional<Tuple> original_tuple =
              ReconstructTuple(&child_executor_->GetOutputSchema(), base_tuple, meta, {old_log});
          // 成功构造出了事务之前的tuple，把他刷到物理层
          if (original_tuple.has_value()) {
            old_log.tuple_ = original_tuple.value();
            old_log.modified_fields_ = std::vector<bool>(child_executor_->GetOutputSchema().GetColumnCount(), true);
            txn->ModifyUndoLog(old_link->prev_log_idx_, old_log);
          }
        }
      }
    }
    bool is_updated = UpdateTupleAndUndoLink(
        txn_mgr, rid, link_to_update, table_info_->table_.get(), txn, new_meta, base_tuple,
        // 双重检查锁定
        [meta = meta](const TupleMeta &fresh_meta, const Tuple &, RID, std::optional<UndoLink>) -> bool {
          return fresh_meta.ts_ == meta.ts_ && fresh_meta.is_deleted_ == meta.is_deleted_;
        });
    // 如果update失败，就是被写写冲突阻拦了
    if (!is_updated) {
      txn->SetTainted();
      throw ExecutionException("Write-Write Conflict detected during atomic delete!");
    }

    // 记录作案现场
    txn->AppendWriteSet(table_info_->oid_, rid);
    // ！！！！索引不能删，因为他可能要给其他版本用，不要直接从物理层面完全删除
    // 只有当这条数据之前还活着的时候，我们才去删它的索引（防止重复删除索引，出现错误）
    // if (!meta.is_deleted_) {
    //   for (const auto &index_info : table_indexes_) {
    //     Tuple key = base_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_,
    //     index_info->index_->GetKeyAttrs()); index_info->index_->DeleteEntry(key, rid, txn);
    //   }
    // }

    delete_count++;
  }
  std::vector<Value> result_values{};
  result_values.emplace_back(TypeId::INTEGER, delete_count);
  Tuple result_tuple{result_values, &GetOutputSchema()};

  tuple_batch->push_back(result_tuple);
  return true;
}

}  // namespace bustub
