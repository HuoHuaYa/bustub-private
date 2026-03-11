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

#include "execution/execution_common.h"
#include "concurrency/transaction_manager.h"
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

  if (has_updated_) { return false; }
  has_updated_ = true;

  auto *txn = exec_ctx_->GetTransaction();
  auto *txn_mgr = exec_ctx_->GetTransactionManager();

  std::vector<Tuple> child_tuples;
  std::vector<RID> child_rids;
  std::vector<Tuple> buffered_tuples;
  std::vector<RID> buffered_rids;

  // 1. 缓冲所有老数据
  while (child_executor_->Next(&child_tuples, &child_rids, batch_size)) {
    for (size_t i = 0; i < child_tuples.size(); i++) {
      buffered_tuples.push_back(child_tuples[i]);
      buffered_rids.push_back(child_rids[i]);
    }
  }

  if (buffered_tuples.empty()) {
    std::vector<Value> result_values{{TypeId::INTEGER, 0}};
    tuple_batch->push_back(Tuple{result_values, &GetOutputSchema()});
    return true;
  }

  std::vector<Tuple> new_tuples;
  std::vector<bool> pk_changed(buffered_tuples.size(), false);

  // 2. 预计算所有的新 Tuple，并判断主键是否改变
  for (size_t i = 0; i < buffered_tuples.size(); i++) {
    std::vector<Value> values;
    values.reserve(child_executor_->GetOutputSchema().GetColumnCount());
    for (const auto &expr : plan_->target_expressions_) {
      values.push_back(expr->Evaluate(&buffered_tuples[i], child_executor_->GetOutputSchema()));
    }
    Tuple new_tuple{values, &child_executor_->GetOutputSchema()};
    new_tuples.push_back(new_tuple);

    bool changed = false;
    for (const auto &index_info : table_indexes_) {
      Tuple old_key = buffered_tuples[i].KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      Tuple new_key = new_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      
      for (uint32_t col_idx = 0; col_idx < index_info->key_schema_.GetColumnCount(); col_idx++) {
        if (old_key.GetValue(&index_info->key_schema_, col_idx).CompareNotEquals(new_key.GetValue(&index_info->key_schema_, col_idx)) == CmpBool::CmpTrue) {
          changed = true;
          break;
        }
      }
      if (changed) break;
    }
    pk_changed[i] = changed;
  }

  // =========================================================================
  // 阶段 1：把所有【主键改变】的老坑位，统统砸成墓碑！
  // =========================================================================
  for (size_t i = 0; i < buffered_tuples.size(); i++) {
    if (pk_changed[i]) {
      RID old_rid = buffered_rids[i];
      auto [old_meta, old_tuple] = table_info_->table_->GetTuple(old_rid);
      
      // 🛡️ W-W 检测 1
      if (old_meta.ts_ > txn->GetReadTs() && old_meta.ts_ != txn->GetTransactionTempTs()) {
        txn->SetTainted();
        throw ExecutionException("Write-Write Conflict");
      }

      TupleMeta tombstone_meta = old_meta;
      tombstone_meta.ts_ = txn->GetTransactionTempTs();
      tombstone_meta.is_deleted_ = true;

      std::optional<UndoLink> old_link = txn_mgr->GetUndoLink(old_rid);
      std::optional<UndoLink> del_link_to_use;

      if (old_meta.ts_ == txn->GetTransactionTempTs()) {
        del_link_to_use = old_link;
      } else {
        UndoLink prev_link = old_link.has_value() ? *old_link : UndoLink{};
        // 🎯 修复点 1：打墓碑时，必须强制记录所有的老数据 (all_modified = true)！绝不使用 GenerateNewUndoLog！
        std::vector<bool> all_modified(table_info_->schema_.GetColumnCount(), true);
        UndoLog del_log{false, all_modified, old_tuple, old_meta.ts_, prev_link};
        del_link_to_use = txn->AppendUndoLog(del_log);
      }

      bool success = UpdateTupleAndUndoLink(
          txn_mgr, old_rid, del_link_to_use, table_info_->table_.get(), txn, tombstone_meta, old_tuple,
          [meta = old_meta](const TupleMeta &fresh_meta, const Tuple &, RID, std::optional<UndoLink>) {
            return fresh_meta.ts_ == meta.ts_ && fresh_meta.is_deleted_ == meta.is_deleted_;
          });
      if (!success) {
        txn->SetTainted();
        throw ExecutionException("Write-Write Conflict");
      }
      txn->AppendWriteSet(table_info_->oid_, old_rid);
    }
  }

  // =========================================================================
  // 阶段 2：对于没变主键的执行原地更新；对于变了主键的去抢新坑位
  // =========================================================================
  for (size_t i = 0; i < buffered_tuples.size(); i++) {
    RID old_rid = buffered_rids[i];
    
    if (!pk_changed[i]) {
      // ------------------------------------------
      // 分支 A：原地更新 (包含日志融合逻辑)
      // ------------------------------------------
      auto [old_meta, old_tuple] = table_info_->table_->GetTuple(old_rid); 
      
      //  W-W 检测 2
      if (old_meta.ts_ > txn->GetReadTs() && old_meta.ts_ != txn->GetTransactionTempTs()) {
        txn->SetTainted();
        throw ExecutionException("Write-Write Conflict");
      }

      std::optional<UndoLink> old_link = txn_mgr->GetUndoLink(old_rid);
      std::optional<UndoLink> link_to_use = old_link;

      if (old_meta.ts_ == txn->GetTransactionTempTs()) {
        if (old_link.has_value()) {
          UndoLog old_log = txn_mgr->GetUndoLog(*old_link);
          auto orig_tuple_opt = ReconstructTuple(&table_info_->schema_, old_tuple, old_meta, {old_log});
          if (orig_tuple_opt.has_value()) {
            Tuple orig_tuple = *orig_tuple_opt;
            std::vector<bool> merged_fields = old_log.modified_fields_;
            for (uint32_t c = 0; c < table_info_->schema_.GetColumnCount(); c++) {
              if (!merged_fields[c]) {
                if (old_tuple.GetValue(&table_info_->schema_, c).CompareNotEquals(new_tuples[i].GetValue(&table_info_->schema_, c)) == CmpBool::CmpTrue) {
                  merged_fields[c] = true;
                }
              }
            }
            std::vector<Column> partial_cols;
            std::vector<Value> partial_vals;
            for (uint32_t c = 0; c < table_info_->schema_.GetColumnCount(); c++) {
              if (merged_fields[c]) {
                partial_cols.push_back(table_info_->schema_.GetColumn(c));
                partial_vals.push_back(orig_tuple.GetValue(&table_info_->schema_, c));
              }
            }
            Schema partial_schema(partial_cols);
            Tuple partial_tuple(partial_vals, &partial_schema);

            UndoLog updated_log{old_log.is_deleted_, merged_fields, partial_tuple, old_log.ts_, old_log.prev_version_};
            txn->ModifyUndoLog(old_link->prev_log_idx_, updated_log);
          }
        }
      } else {
        UndoLink prev_link = old_link.has_value() ? *old_link : UndoLink{};
        UndoLog new_log = GenerateNewUndoLog(&table_info_->schema_, &old_tuple, &new_tuples[i], old_meta.ts_, prev_link);
        link_to_use = txn->AppendUndoLog(new_log);
      }

      TupleMeta new_meta = old_meta;
      new_meta.ts_ = txn->GetTransactionTempTs();

      bool success = UpdateTupleAndUndoLink(
          txn_mgr, old_rid, link_to_use, table_info_->table_.get(), txn, new_meta, new_tuples[i],
          [meta = old_meta](const TupleMeta &fresh_meta, const Tuple &, RID, std::optional<UndoLink>) {
            return fresh_meta.ts_ == meta.ts_ && fresh_meta.is_deleted_ == meta.is_deleted_;
          });
      if (!success) {
        txn->SetTainted();
        throw ExecutionException("Write-Write Conflict");
      }
      txn->AppendWriteSet(table_info_->oid_, old_rid);

    } else {
      // ------------------------------------------
      // 分支 B：拿着新主键找新坑位落户
      // ------------------------------------------
      auto index_info = table_indexes_[0];
      Tuple new_key = new_tuples[i].KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      
      std::vector<RID> result_rids;
      index_info->index_->ScanKey(new_key, &result_rids, txn);

      if (!result_rids.empty()) {
        RID target_rid = result_rids[0];
        auto [target_meta, target_tuple] = table_info_->table_->GetTuple(target_rid);

        if (target_meta.ts_ > txn->GetReadTs() && target_meta.ts_ != txn->GetTransactionTempTs()) {
          txn->SetTainted();
          throw ExecutionException("Write-Write Conflict");
        }

        bool is_friendly = false;
        for (const auto &rid : buffered_rids) {
          if (rid == target_rid) {
            is_friendly = true;
            break;
          }
        }

        if (target_meta.is_deleted_ || is_friendly) {
          TupleMeta resurrect_meta = target_meta;
          resurrect_meta.ts_ = txn->GetTransactionTempTs();
          resurrect_meta.is_deleted_ = false;

          std::optional<UndoLink> tgt_link = txn_mgr->GetUndoLink(target_rid);
          std::optional<UndoLink> res_link_to_use;

          if (target_meta.ts_ == txn->GetTransactionTempTs()) {
            res_link_to_use = tgt_link;
          } else {
            UndoLink tgt_prev = tgt_link.has_value() ? *tgt_link : UndoLink{};
            // 🎯 修复点 2：复活墓碑时，旧状态一定是“死”的，强制记录 is_deleted=true 且不需要任何数据 (no_modified)！
            std::vector<bool> no_modified(table_info_->schema_.GetColumnCount(), false);
            UndoLog res_log{true, no_modified, Tuple{}, target_meta.ts_, tgt_prev};
            res_link_to_use = txn->AppendUndoLog(res_log);
          }

          bool success = UpdateTupleAndUndoLink(
              txn_mgr, target_rid, res_link_to_use, table_info_->table_.get(), txn, resurrect_meta, new_tuples[i],
              [meta = target_meta](const TupleMeta &fresh_meta, const Tuple &, RID, std::optional<UndoLink>) {
                return fresh_meta.ts_ == meta.ts_ && fresh_meta.is_deleted_ == meta.is_deleted_;
              });
          if (!success) {
            txn->SetTainted();
            throw ExecutionException("Write-Write Conflict");
          }
          txn->AppendWriteSet(table_info_->oid_, target_rid);
        } else {
          txn->SetTainted();
          throw ExecutionException("Primary Key Violation on Update!");
        }
      } else {
        // 全新插入坑位
        TupleMeta new_insert_meta = {txn->GetTransactionTempTs(), false};
        auto new_rid_opt = table_info_->table_->InsertTuple(new_insert_meta, new_tuples[i], exec_ctx_->GetLockManager(), txn, table_info_->oid_);
        if (new_rid_opt.has_value()) {
          txn->AppendWriteSet(table_info_->oid_, *new_rid_opt);
          index_info->index_->InsertEntry(new_key, *new_rid_opt, txn);
        }
      }
    }
  }
  
  std::vector<Value> result_values{{TypeId::INTEGER, static_cast<int32_t>(buffered_tuples.size())}};
  tuple_batch->push_back(Tuple{result_values, &GetOutputSchema()});
  return true;
}

} // namespace bustub
