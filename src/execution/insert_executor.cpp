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
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"

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
  int32_t insert_count = 0;

  // 提取事务上下文和管理器
  auto *txn = exec_ctx_->GetTransaction();
  auto *txn_mgr = exec_ctx_->GetTransactionManager();
  auto table_oid = plan_->GetTableOid();

  std::vector<bustub::Tuple> child_tuples;
  std::vector<bustub::RID> child_rids;
  
  while (child_executor_->Next(&child_tuples, &child_rids, batch_size)) {
    for (const auto &tuple : child_tuples) {
        
      // 表只有一个主键，但可以有多个索引，但没索引就是没有主键，不需要估计直接insert
      // 没有索引就不可能存在主键约束，直接无脑insert
  if (table_indexes_.empty()) {
    TupleMeta meta{txn->GetTransactionTempTs(), false};
    auto rid_opt = table_info_->table_->InsertTuple(meta, tuple);
    if (rid_opt.has_value()) {
      txn->AppendWriteSet(table_oid, rid_opt.value());
      insert_count++;
    }
    continue;
  }

  // 有索引的情况准备查找是否存在主键
  // bustub为了偷懒，默认第一个index是主键
  auto index_info = table_indexes_[0];
  Tuple key_tuple = tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());

  std::vector<bustub::RID> result_rids;
  index_info->index_->ScanKey(key_tuple, &result_rids, txn);
  // 根本就没有主键
  if (result_rids.empty()) {
    TupleMeta meta{txn->GetTransactionTempTs(), false};
    auto rid_opt = table_info_->table_->InsertTuple(meta, tuple);
    if (rid_opt.has_value()) {
      auto rid = rid_opt.value();
      // 记录，并写入，创造新的rid给他占位
      txn->AppendWriteSet(table_oid, rid);
      bool is_inserted = index_info->index_->InsertEntry(key_tuple, rid, txn);
      // 可能被其他线程抢占主键，导致insert失败，保留第一个线程
      if (!is_inserted) {
    txn->SetTainted();
    throw ExecutionException("Primary Key Violation");
}
      insert_count++;
    }
  } else {
    // 有了主键，如果不是墓碑就是写写冲突，是墓碑就直接覆盖，不生成新的rid
    auto existing_rid = result_rids[0];
    auto [old_meta, old_tuple, old_link] = GetTupleAndUndoLink(txn_mgr, table_info_->table_.get(), existing_rid);
    // 写写冲突
    if (old_meta.ts_ > txn->GetReadTs() && old_meta.ts_ != txn->GetTransactionTempTs()) {
      txn->SetTainted();
      throw ExecutionException("Write-Write Conflict");
    }

    // 主键冲突
    if (!old_meta.is_deleted_) {
      txn->SetTainted();
      throw ExecutionException("Primary Key Violation");
    }
    // 用新数据覆盖它，并把它的死亡记录挂到undolink上
    
    std::optional<UndoLink> new_link = old_link; 

    // 如果这个墓碑不是当前事务造成的，我就得老老实实给它立个死亡证明UndoLog
    if (old_meta.ts_ != txn->GetTransactionTempTs()) {
      UndoLog tombstone_log;
      tombstone_log.is_deleted_ = true;
      tombstone_log.ts_ = old_meta.ts_;
      tombstone_log.prev_version_ = old_link.has_value() ? old_link.value() : UndoLink{};
      // 不需要存 Tuple，因为它是墓碑，以前没数据
      new_link = txn->AppendUndoLog(tombstone_log);
    }
    // 如果这个墓碑就是当前事务造成的，那连日志都不用新造了，直接复用老link，也不用生成日志，就用老的
    TupleMeta fresh_meta{txn->GetTransactionTempTs(), false};
    // 原子更新tuple和link
    bool is_updated = UpdateTupleAndUndoLink(
    txn_mgr, existing_rid, new_link, table_info_->table_.get(), txn,
    fresh_meta, tuple,
    [txn](const TupleMeta &check_meta, const Tuple &check_tuple, RID check_rid, std::optional<UndoLink> check_link) {
        return check_meta.ts_ == txn->GetTransactionTempTs() || check_meta.is_deleted_; 
    }
);

    if (is_updated) {
      // 记录
      // 存在就说明已经占据了rid，不需要更新rid
      txn->AppendWriteSet(table_oid, existing_rid);
      insert_count++;
    } else {
      txn->SetTainted();
    throw ExecutionException("Write-Write Conflict");
    }
    }
  }
  child_tuples.clear();
  child_rids.clear();
}
std::vector<Value> values{};
  values.reserve(1);
  values.emplace_back(TypeId::INTEGER, insert_count);
  Tuple result_tuple{values, &GetOutputSchema()};

  tuple_batch->push_back(result_tuple);
  return true;
  // tuple_batch->clear();
  // rid_batch->clear();
  // if (has_inserted_) {
  //   return false;
  // }
  // has_inserted_ = true;
  // // 记录有多少行插入了
  // int32_t insert_count = 0;

  // std::vector<bustub::Tuple> child_tuples;
  // std::vector<bustub::RID> child_rids;
  // while (child_executor_->Next(&child_tuples, &child_rids, batch_size)) {
  //   for (const auto &tuple : child_tuples) {
  //     // 默认构造tuplemeta,时间戳和is_deleted
  //     TupleMeta meta{0, false};
  //     // 如果页满了，或者元组太大，可能会插入失败，所以用optional包裹
  //     auto rid_opt = table_info_->table_->InsertTuple(meta, tuple);
  //     // 如果成功inserttuple
  //     if (rid_opt.has_value()) {
  //       auto rid = rid_opt.value();
  //       for (const auto &index_info : table_indexes_) {
  //         // schema是类型，keyfromtuple就是提取出我们要的schema
  //         // auto Tuple::KeyFromTuple(const Schema &schema, const Schema
  //         // &key_schema, const std::vector<uint32_t> &key_attrs) const
  //         //     -> Tuple {
  //         //   std::vector<Value> values;
  //         //   values.reserve(key_attrs.size());
  //         //   for (auto idx : key_attrs) {
  //         //     values.emplace_back(this->GetValue(&schema, idx));
  //         //   }
  //         //   return {values, &key_schema};
  //         // }
  //         Tuple key_tuple =
  //             tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_,
  //                                index_info->index_->GetKeyAttrs());
  //         // 将我们提取出来的key与rid真正的插入b+tree
  //         index_info->index_->InsertEntry(key_tuple, rid,
  //                                         exec_ctx_->GetTransaction());
  //       }
  //       insert_count++;
  //     }
  //   }
  //   // 清空临时容器
  //   child_tuples.clear();
  //   child_rids.clear();
  // }
  // // 通过tuple_batch去返回插入的行数
  // //{}显式的调用默认函数
  // std::vector<Value> values{};
  // values.reserve(1);
  // values.emplace_back(TypeId::INTEGER, insert_count);
  // // 他要指针所有有 &
  // Tuple result_tuple{values, &GetOutputSchema()};

  // tuple_batch->push_back(result_tuple);
  // return true;
}

} // namespace bustub
