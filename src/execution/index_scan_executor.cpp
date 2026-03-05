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

namespace bustub {

/**
 * Creates a new index scan executor.
 * @param exec_ctx the executor context
 * @param plan the index scan plan to be executed
 */
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
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
    index_info_->index_->ScanKey(lookup_key, &rids_, exec_ctx_->GetTransaction());
  } else {
    // ORDER BY v1
    // 强制转换为底层的单列整数 B+ 树对象 (BusTub
    // 官方默认测试用例都是单列整数索引)
    auto *tree = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get());
    BUSTUB_ASSERT(tree != nullptr, "not a BPlusTreeIndexForTwoIntegerColumn");

    // 获取迭代器，从最左边的叶子节点开始，顺着链表把所有门牌号全收集起来！
    for (auto it = tree->GetBeginIterator(); !it.IsEnd(); ++it) {
      // .second 里面存的就是 RID
      rids_.push_back((*it).second);
    }
  }
}

auto IndexScanExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                             size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();
  // 从curosr，上一次停的位置开始跑
  while (cursor_ < static_cast<size_t>(rids_.size())) {
    RID rid = rids_[cursor_++];

    // 用meta看数据是不是似了
    TupleMeta meta = table_info_->table_->GetTupleMeta(rid);
    if (!meta.is_deleted_) {
      auto tuple_pair = table_info_->table_->GetTuple(rid);
      // 判断还有没有额外的where子句
      if (plan_->filter_predicate_ != nullptr) {
        auto result = plan_->filter_predicate_->Evaluate(&tuple_pair.second, table_info_->schema_);
        if (result.IsNull() || !result.GetAs<bool>()) {
          // 如果不满足，就舍弃这段
          continue;
        }
      }
      // 装载数据
      tuple_batch->push_back(tuple_pair.second);
      rid_batch->push_back(rid);
      if (static_cast<size_t>(tuple_batch->size()) >= batch_size) {
        return true;
      }
    }
  }
  return !tuple_batch->empty();
}

}  // namespace bustub
