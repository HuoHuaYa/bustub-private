//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/index_scan_executor.h"
#include "common/macros.h"
#include "storage/index/b_plus_tree_index.h"
#include "execution/execution_common.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

/**
 * Creates a new index scan executor.
 * @param exec_ctx the executor context
 * @param plan the index scan plan to be executed
 */
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx,
                                     const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  // UNIMPLEMENTED("TODO(P3): Add implementation.");
}

// 如果prekeys是空的，说明需要全局遍历
void IndexScanExecutor::Init() {
  auto catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->table_oid_);
  if (table_info_ == nullptr) {
    throw std::runtime_error("index_scan : not found");
  }
  index_info_ = catalog->GetIndex(plan_->index_oid_);

  rids_.clear();
  cursor_ = 0;
  auto *txn = exec_ctx_->GetTransaction();
  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    // 把当前扫描的表OID和过滤谓词（filter_predicate_）记录下来
    txn->AppendScanPredicate(table_info_->oid_, plan_->filter_predicate_);
  }
  // INDEX_TEMPLATE_ARGUMENTS
  // void BPLUSTREE_INDEX_TYPE::ScanKey(const Tuple &key, std::vector<RID>
  // *result, Transaction *transaction) {
  //   KeyType index_key;
  //   index_key.SetFromKey(key);
  //   container_->GetValue(index_key, result);
  // }
  if (!plan_->pred_keys_.empty()) {
    std::vector<Value> values;
    /**
     * indexscanplan:std::vector<AbstractExpressionRef> pred_keys_;
     * The constant value keys to lookup.
     * For example when dealing "WHERE v = 1" we could store the constant value
     * 1 here
     */
    values.reserve(plan_->pred_keys_.size());
    for (const auto &expr : plan_->pred_keys_) {
      // where v1 = 1
      // (*tuple, &schema)
      // h中提到了传进来的一定是常量，常量表达式的处理不需要用到tuple
      values.push_back(expr->Evaluate(nullptr, plan_->OutputSchema()));
    }
    Tuple lookup_key{values, &index_info_->key_schema_};

    // 去 B+ 树里发起精准打击，把匹配的门牌号全都装进 rids_ 数组里
    index_info_->index_->ScanKey(lookup_key, &rids_,
                                 exec_ctx_->GetTransaction());
  } else {
    // ORDER BY v1
    // 强制转换为底层的单列整数 B+ 树对象 (BusTub
    // 官方默认测试用例都是单列整数索引)
    auto *tree = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(
        index_info_->index_.get());
    BUSTUB_ASSERT(tree != nullptr, "not a BPlusTreeIndexForTwoIntegerColumn");

    // 获取迭代器，从最左边的叶子节点开始，顺着链表把所有门牌号全收集起来！
    for (auto it = tree->GetBeginIterator(); !it.IsEnd(); ++it) {
      // .second 里面存的就是 RID
      rids_.push_back((*it).second);
    }
  }
}

auto IndexScanExecutor::Next(std::vector<bustub::Tuple> *tuple_batch,
                             std::vector<bustub::RID> *rid_batch,
                             size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();
  auto *txn = exec_ctx_->GetTransaction();
  auto *txn_mgr = exec_ctx_->GetTransactionManager();
  // 从cursor_开始往下查
  while (cursor_ < static_cast<size_t>(rids_.size())) {
    RID rid = rids_[cursor_++];

    // 获取案发现场带上UndoLink
    auto [meta, base_tuple, undo_link] = GetTupleAndUndoLink(txn_mgr, table_info_->table_.get(), rid);
    // 找到事务可见版本
    bool is_visible = false;
    // 当前版本是不是一个墓碑
    bool is_deleted = false;
    Tuple visible_tuple = base_tuple;

    // 是我自己改的，或者是比我读时间戳更老的已提交版本
    if (meta.ts_ == txn->GetTransactionTempTs() || meta.ts_ <= txn->GetReadTs()) {
      is_visible = true;
      is_deleted = meta.is_deleted_;
    } else {
      // 版本太新了，顺着 UndoLink 往历史深处找
      std::vector<UndoLog> logs_to_apply;
      auto curr_link = undo_link;
      while (curr_link.has_value() && curr_link->IsValid()) {
        auto log_opt = txn_mgr->GetUndoLogOptional(curr_link.value());
        // 无法继续获得新的link了
        if (!log_opt.has_value()) {
          break;
        }
        
        auto log = log_opt.value();
        logs_to_apply.push_back(log);

        // 找到了最近可见版本，直接停下
        if (log.ts_ <= txn->GetReadTs()) {
          is_visible = true;
          is_deleted = log.is_deleted_;
          break;
        }
        curr_link = log.prev_version_;
      }

      // 如果找到了历史可见版本拼出完整的旧数据
      if (is_visible) {
        auto reconstructed = ReconstructTuple(&table_info_->schema_, base_tuple, meta, logs_to_apply);
        if (reconstructed.has_value()) {
          visible_tuple = reconstructed.value();
        } else {
          // 如果构造失败，说明退回到了还没Insert的ts，也就是没数据
          is_visible = false; 
        }
      }
    }

    // 可见且不是墓碑，才能吐给上层
    if (is_visible && !is_deleted) {
      // 过滤条件判定
      if (plan_->filter_predicate_ != nullptr) {
        auto result = plan_->filter_predicate_->Evaluate(&visible_tuple, table_info_->schema_);
        // 不满足条件就滚蛋
        if (result.IsNull() || !result.GetAs<bool>()) {
          continue;
        }
      }

      // 装载数据
      tuple_batch->push_back(visible_tuple);
      rid_batch->push_back(rid);

      // 满足batch_size就提前下班
      if (static_cast<size_t>(tuple_batch->size()) >= batch_size) {
        return true;
      }
    }
  }
  return !tuple_batch->empty();
}

} // namespace bustub
