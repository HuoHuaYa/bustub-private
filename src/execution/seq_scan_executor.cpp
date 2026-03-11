//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "common/macros.h"
#include "execution/execution_common.h"
#include "concurrency/transaction_manager.h"
namespace bustub {

/**
 * Construct a new SeqScanExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The sequential scan plan to be executed
 */
SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx,
                                 const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

/** Initialize the sequential scan */
void SeqScanExecutor::Init() {
  // 获取catalog指针
  auto catalog = exec_ctx_->GetCatalog();
  // 获取tableinfo
  table_info_ = catalog->GetTable(plan_->GetTableOid());
  if (table_info_ == nullptr) {
    throw std::runtime_error("Table not found in catalog");
  }
  iter_.emplace(table_info_->table_->MakeIterator());
  auto *txn = exec_ctx_->GetTransaction();
  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    // 把当前扫描的表OID和过滤谓词（filter_predicate_）记录下来
    txn->AppendScanPredicate(table_info_->oid_, plan_->filter_predicate_);
  }
}

/**
 * Yield the next tuple batch from the seq scan.
 * @param[out] tuple_batch The next tuple batch produced by the scan
 * @param[out] rid_batch The next tuple RID batch produced by the scan
 * @param batch_size The number of tuples to be included in the batch (default:
 * BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto SeqScanExecutor::Next(std::vector<bustub::Tuple> *tuple_batch,
                           std::vector<bustub::RID> *rid_batch,
                           size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();

  auto *txn_mgr = exec_ctx_->GetTransactionManager();
  auto *txn = exec_ctx_->GetTransaction();

  // 只要表还没扫完并且批次还没装满就一直扫
  while (!iter_->IsEnd() && tuple_batch->size() < batch_size) {
    auto rid = iter_->GetRID();
    
    // 获取基线数据和link
    auto [meta, base_tuple, undo_link] = GetTupleAndUndoLink(txn_mgr, table_info_->table_.get(), rid);

    ++(*iter_);

    // 时间戳<=视力或者当前事务是刚才改的脏数据

    if (meta.ts_ <= txn->GetReadTs() || meta.ts_ == txn->GetTransactionTempTs()) {
      if (!meta.is_deleted_) {
        // 判断where内容
        bool is_match = true;
        if (plan_->filter_predicate_ != nullptr) {
          auto value = plan_->filter_predicate_->Evaluate(&base_tuple, table_info_->schema_);
          is_match = !value.IsNull() && value.GetAs<bool>();
        }
        if (is_match)
        {tuple_batch->push_back(base_tuple);
        rid_batch->push_back(rid);}
      }
      // 在sql被删除后，不应该返回null应该直接滚蛋
      // 如果 is_deleted_ 是 true，说明是个墓碑，直接 continue 看下一行
      continue;
    }

    // tuple需要回滚
    std::vector<UndoLog> logs_to_apply;
    auto current_link = undo_link;
    bool found_visible_version = false;

    // 用outlink找
    while (current_link.has_value() && current_link->IsValid()) {
      auto undo_log = txn_mgr->GetUndoLog(*current_link);
      logs_to_apply.push_back(undo_log);
      
      // 找到最近的可见节点
      if (undo_log.ts_ <= txn->GetReadTs()) {
        found_visible_version = true;
        break; 
      }
      current_link = undo_log.prev_version_;
    }

    // 成功匹配启动重构
    if (found_visible_version) {
      auto history_tuple = ReconstructTuple(&table_info_->schema_, base_tuple, meta, logs_to_apply);
      
      // 重构成功
      if (history_tuple.has_value()) {
        bool is_match = true;
        if (plan_->filter_predicate_ != nullptr) {
          auto value = plan_->filter_predicate_->Evaluate(&history_tuple.value(), table_info_->schema_);
          is_match = !value.IsNull() && value.GetAs<bool>();
        }
        if(is_match)
        {tuple_batch->push_back(history_tuple.value());
        rid_batch->push_back(rid);}
      }
    }
    // 如果found_visible_version是 false，或者 history_tuple是nullopt，
    // 说明最近的可见节点这行数据不存在continue。
  }
  // 如果全是空的说明表扫到底了，返回 false。
  return !tuple_batch->empty();
}

} // namespace bustub
