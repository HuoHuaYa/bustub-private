//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.h
//
// Identification: src/include/execution/executors/seq_scan_executor.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <optional>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The SeqScanExecutor executor executes a sequential table scan.
 */
/**
 执行操作的节点
 SeqScanPlanNode(SchemaRef output, table_oid_t table_oid, std::string
table_name, AbstractExpressionRef filter_predicate = nullptr) :
AbstractPlanNode(std::move(output), {}), table_oid_{table_oid},
        table_name_(std::move(table_name)),
        filter_predicate_(std::move(filter_predicate)) {}
 表的信息
 struct TableInfo {
   TableInfo(Schema schema, std::string name, std::unique_ptr<TableHeap>
&&table, table_oid_t oid) : schema_{std::move(schema)}, name_{std::move(name)},
        table_{std::move(table)},
        oid_{oid} {}

  Schema schema_;                         // 表的模式（列结构）
  const std::string name_;               // 表名
  std::unique_ptr<TableHeap> table_;    // 表堆（存储表数据）
  const table_oid_t oid_;               // 表的唯一标识符
};
 class Catalog {
 public:
  // 静态常量
  static inline const std::shared_ptr<TableInfo> NULL_TABLE_INFO{nullptr};
  static inline const std::shared_ptr<IndexInfo> NULL_INDEX_INFO{nullptr};

  // 构造函数
  Catalog(BufferPoolManager *bpm, LockManager *lock_manager, LogManager
*log_manager);

  // 表操作
  auto CreateTable(...) -> std::shared_ptr<TableInfo>;
  auto GetTable(const std::string &table_name) const ->
std::shared_ptr<TableInfo>; auto GetTable(table_oid_t table_oid) const ->
std::shared_ptr<TableInfo>;

  // 索引操作
  template <class KeyType, class ValueType, class KeyComparator>
  auto CreateIndex(...) -> std::shared_ptr<IndexInfo>;
  auto GetIndex(const std::string &index_name, const std::string &table_name) ->
std::shared_ptr<IndexInfo>; auto GetIndex(const std::string &index_name, const
table_oid_t table_oid) -> std::shared_ptr<IndexInfo>; auto GetIndex(index_oid_t
index_oid) -> std::shared_ptr<IndexInfo>; auto GetTableIndexes(const std::string
&table_name) const -> std::vector<std::shared_ptr<IndexInfo>>; auto
GetTableNames() -> std::vector<std::string>;

 private:
  BufferPoolManager *bpm_;
  LockManager *lock_manager_;
  LogManager *log_manager_;

  std::unordered_map<table_oid_t, std::shared_ptr<TableInfo>> tables_;
  std::unordered_map<std::string, table_oid_t> table_names_;
  std::atomic<table_oid_t> next_table_oid_{0};
  std::unordered_map<index_oid_t, std::shared_ptr<IndexInfo>> indexes_;
  std::unordered_map<std::string, std::unordered_map<std::string, index_oid_t>>
index_names_; std::atomic<index_oid_t> next_index_oid_{0};
};

 */
class SeqScanExecutor : public AbstractExecutor {
 public:
  SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan);

  void Init() override;

  auto Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch, size_t batch_size)
      -> bool override;

  /** @return The output schema for the sequential scan */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The sequential scan plan node to be executed */
  const SeqScanPlanNode *plan_;
  // executorcontext含有catalog，catalog中含有table

  // 表格元信息指针从 Catalog 获取
  std::shared_ptr<TableInfo> table_info_{nullptr};

  // 因为tableiterator没有默认构造函数，所以构造seqscanexecutor时，为了避免他调用不存在的默认构造，我们用optional装
  std::optional<TableIterator> iter_;
};
}  // namespace bustub
