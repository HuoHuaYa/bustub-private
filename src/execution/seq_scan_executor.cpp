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

namespace bustub {

/**
 * Construct a new SeqScanExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The sequential scan plan to be executed
 */
SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
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
}

/**
 * Yield the next tuple batch from the seq scan.
 * @param[out] tuple_batch The next tuple batch produced by the scan
 * @param[out] rid_batch The next tuple RID batch produced by the scan
 * @param batch_size The number of tuples to be included in the batch (default:
 * BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto SeqScanExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                           size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();
  while (tuple_batch->size() < batch_size && !iter_->IsEnd()) {
    // debug
    // std::cout << "[DEBUG-SeqScan] Trying to read RID -> Page: " <<
    // iter_->GetRID().GetPageId()
    //           << ", Slot: " << iter_->GetRID().GetSlotNum() << std::endl;
    // 获取元组数据
    auto [meta, tuple] = iter_->GetTuple();
    auto rid = iter_->GetRID();
    if (!meta.is_deleted_) {
      bool is_match = true;
      // 判断有没有过滤条件
      if (plan_->filter_predicate_ != nullptr) {
        // 传入当前元组和表的 schema 进行求值
        auto value = plan_->filter_predicate_->Evaluate(&tuple, table_info_->schema_);
        // 有 NULL 值处理逻辑，需要当心，但常规情况下 GetAs<bool> 足够
        is_match = value.GetAs<bool>();
      }

      // 如果没被删除，且符合条件(或者压根没有过滤条件)，就装车
      if (is_match) {
        tuple_batch->push_back(tuple);
        rid_batch->push_back(rid);
      }
    }

    //  前置自增
    ++(*iter_);
  }

  // 只要这趟车里装了哪怕一条数据，就返回 true 告诉上层继续要数据。
  return !tuple_batch->empty();
}

}  // namespace bustub
