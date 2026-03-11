//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// execution_common.cpp
//
// Identification: src/execution/execution_common.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/execution_common.h"

#include "catalog/catalog.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

TupleComparator::TupleComparator(std::vector<OrderBy> order_bys) : order_bys_(std::move(order_bys)) {}

/** TODO(P3): Implement the comparison method */
auto TupleComparator::operator()(const SortEntry &entry_a, const SortEntry &entry_b) const -> bool {
  // 遍历所有的orderby，和window差不多的判断
  for (size_t i = 0; i < order_bys_.size(); i++) {
    const auto &order_by = order_bys_[i];
    auto order_type = std::get<0>(order_by);
    auto null_type = std::get<1>(order_by);

    // 直接从 SortEntry.first 中取出提前算好的 Value
    const Value &val_a = entry_a.first[i];
    const Value &val_b = entry_b.first[i];
    bool a_null = val_a.IsNull();
    bool b_null = val_b.IsNull();
    if (a_null && b_null) {
      continue;
    }
    if (val_a.CompareEquals(val_b) == CmpBool::CmpTrue) {
      continue;  // 平局，继续比下一列
    }
    if (a_null || b_null) {
      bool a_comes_first;
      if (null_type == OrderByNullType::NULLS_FIRST) {
        a_comes_first = a_null;
      } else if (null_type == OrderByNullType::NULLS_LAST) {
        a_comes_first = b_null;
      } else {
        // 默认情况：ASC 时 NULL 在最前，DESC 时 NULL 在最后
        if (order_type == OrderByType::DESC) {
          a_comes_first = b_null;
        } else {
          a_comes_first = a_null;
        }
      }
      return a_comes_first;
    }

    // 正常的值比大小
    if (order_type == OrderByType::DEFAULT || order_type == OrderByType::ASC) {
      return val_a.CompareLessThan(val_b) == CmpBool::CmpTrue;
    }
    return val_a.CompareGreaterThan(val_b) == CmpBool::CmpTrue;
  }
  // 所有列都相等，返回 false 即可
  return false;
}

/**
 * Generate sort key for a tuple based on the order by expressions.
 *
 * TODO(P3): Implement this method.
 */
auto GenerateSortKey(const Tuple &tuple, const std::vector<OrderBy> &order_bys, const Schema &schema) -> SortKey {
  SortKey sort_key;
  sort_key.reserve(order_bys.size());
  for (const auto &ob : order_bys) {
    // std::get<2> 就是提取 OrderBy 规则里的 AbstractExpressionRef (表达式)
    auto expr = std::get<2>(ob);
    // 通过 Schema 从 Tuple 里抽出这列对应的值，存进 SortKey 中
    sort_key.push_back(expr->Evaluate(&tuple, schema));
  }
  return sort_key;
}

/**
 * Above are all you need for P3.
 * You can ignore the remaining part of this file until P4.
 */

/**
 * @brief Reconstruct a tuple by applying the provided undo logs from the base
 * tuple. All logs in the undo_logs are applied regardless of the timestamp
 *
 * @param schema The schema of the base tuple and the returned tuple.
 * @param base_tuple The base tuple to start the reconstruction from.
 * @param base_meta The metadata of the base tuple.
 * @param undo_logs The list of undo logs to apply during the reconstruction,
 * the front is applied first.
 * @return An optional tuple that represents the reconstructed tuple. If the
 * tuple is deleted as the result, returns std::nullopt.
 */
auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  Tuple current_tuple = base_tuple;
  bool current_is_deleted = base_meta.is_deleted_;
  for (const auto &log : undo_logs) {
    // 上一个状态是被删除的
    if (log.is_deleted_) {
      // 标记deleted表示不存在
      current_is_deleted = true;
      continue;
    }
    // 过去的状态是活的
    // 如果现在被delete就直接搬tuple过来
    if (current_is_deleted) {
      current_tuple = log.tuple_;
      current_is_deleted = false;
    }
    // 手里是活的，过去也是活的，修改部分tuple

    const std::vector<bool> &modified_fields = log.modified_fields_;
    // 通过column构造对应的partial schema
    std::vector<Column> partial_columns;
    for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
      if (modified_fields[i]) {
        partial_columns.push_back(schema->GetColumn(i));
      }
    }
    Schema partial_schema(partial_columns);
    std::vector<Value> reconstructed_values;
    uint32_t log_tuple_idx = 0;
    for (uint32_t col_idx = 0; col_idx < schema->GetColumnCount(); col_idx++) {
      if (modified_fields[col_idx]) {
        Value old_value = log.tuple_.GetValue(&partial_schema, log_tuple_idx);
        reconstructed_values.push_back(old_value);
        log_tuple_idx++;
      } else {
        Value current_value = current_tuple.GetValue(schema, col_idx);
        reconstructed_values.push_back(current_value);
      }
    }
    current_tuple = Tuple(reconstructed_values, schema);
  }
  if (current_is_deleted) {
    return std::nullopt;
  }
  return current_tuple;
}

/**
 * @brief Collects the undo logs sufficient to reconstruct the tuple w.r.t. the
 * txn.
 * 收集撤销日志
 * @param rid The RID of the tuple.
 * @param base_meta The metadata of the base tuple.
 * @param base_tuple The base tuple.
 * @param undo_link The undo link to the latest undo log.
 * @param txn The transaction.
 * @param txn_mgr The transaction manager.
 * @return An optional vector of undo logs to pass to ReconstructTuple().
 * std::nullopt if the tuple did not exist at the time.
 */
auto CollectUndoLogs(RID rid, const TupleMeta &base_meta, const Tuple &base_tuple, std::optional<UndoLink> undo_link,
                     Transaction *txn, TransactionManager *txn_mgr) -> std::optional<std::vector<UndoLog>> {
  std::vector<UndoLog> res;
  timestamp_t current_ts = base_meta.ts_;

  // txn->GetTransactionTempTs(): ts + TXN_START_ID(1<<62),当前事务自己的临时 ID.==说明这是我自己改的
  // txn->GetReadTs(): 读取时间戳，在这之前都可见
  // 这就是终点判断，如果满足说明当前的日志已经够用了
  if (current_ts == txn->GetTransactionTempTs() || current_ts <= txn->GetReadTs()) {
    // 既然直接可见，就不需要时光倒流了，直接返回一个空的日志数组！
    return res;
  }
  if (!undo_link.has_value()) {
    return std::nullopt;
  }
  UndoLink current_link = undo_link.value();
  // 只要指针没断，就一直顺着藤往回摸
  while (current_link.IsValid()) {
    // log_opt: 可选类型日志。来源于调用 TransactionManager 提供的获取方法。
    // 是什么：从对应事务的内存池里拷贝出来的那条真实的撤销日志。
    auto log_opt = txn_mgr->GetUndoLogOptional(current_link);
    if (!log_opt.has_value()) {
      // 致命异常：链表没爬完，但日志找不到了（可能被系统垃圾回收器 GC 销毁了）
      // 这说明我们永远无法还原出可见版本了，直接返回空（nullopt）。
      return std::nullopt;
    }

    // 拿到真实的日志，装进我们的小推车里
    UndoLog log = log_opt.value();
    res.push_back(log);
    // 此时说明录入一个日志，同步更新这个日志的时间戳
    current_ts = log.ts_;
    // 判断终点
    if (current_ts == txn->GetTransactionTempTs() || current_ts <= txn->GetReadTs()) {
      return res;
    }
    // 没有到达终点继续跑link指针
    current_link = log.prev_version_;
  }
  return std::nullopt;
}

/**
 * @brief Generates a new undo log as the transaction tries to modify this tuple
 * at the first time.
 *
 * @param schema The schema of the table.
 * @param base_tuple The base tuple before the update, the one retrieved from
 * the table heap. nullptr if the tuple is deleted.
 * @param target_tuple The target tuple after the update. nullptr if this is a
 * deletion.
 * @param ts The timestamp of the base tuple.
 * @param prev_version The undo link to the latest undo log of this tuple.
 * @return The generated undo log.
 */
auto GenerateNewUndoLog(const Schema *schema, const Tuple *base_tuple, const Tuple *target_tuple, timestamp_t ts,
                        UndoLink prev_version) -> UndoLog {
  // 在insert，delete中根本就没有不要使用函数就几行，所以在这里只处理update
  // 测试样例，明确要求要在这里加上insert，delete，呜呜
  // insert
  if (base_tuple == nullptr) {
    return UndoLog{true, std::vector<bool>(schema->GetColumnCount(), false), Tuple{}, ts, prev_version};
  }

  // delete
  // 必须全量保存 base_tuple 作为历史遗照
  if (target_tuple == nullptr) {
    return UndoLog{false, std::vector<bool>(schema->GetColumnCount(), true), *base_tuple, ts, prev_version};
  }
  std::vector<bool> modified_fields(schema->GetColumnCount(), false);
  std::vector<Value> old_values;
  std::vector<Column> partial_columns;

  // 逐列对比，提取差异
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    Value base_val = base_tuple->GetValue(schema, i);
    Value target_val = target_tuple->GetValue(schema, i);
    // 只要新老值不一样，说明这一列被修改了
    if (!base_val.CompareExactlyEquals(target_val)) {
      modified_fields[i] = true;
      old_values.push_back(base_val);
      partial_columns.push_back(schema->GetColumn(i));
    }
  }

  // 现场手搓 Partial Schema，把老值打包成局部元组
  Schema partial_schema(partial_columns);
  Tuple partial_tuple(old_values, &partial_schema);

  // 返回这条增量日志 (is_deleted_=false, ts和prev_version留给Executor去填)
  return UndoLog{false, modified_fields, partial_tuple, ts, prev_version};
}

/**
 * @brief Generate the updated undo log to replace the old one, whereas the
 * tuple is already modified by this txn once.
 * 替换旧的undolog
 * @param schema The schema of the table.
 * @param base_tuple The base tuple before the update, the one retrieved from
 * the table heap. nullptr if the tuple is deleted.
 * @param target_tuple The target tuple after the update. nullptr if this is a
 * deletion.
 * @param log The original undo log.
 * @return The updated undo log.
 */
auto GenerateUpdatedUndoLog(const Schema *schema, const Tuple *base_tuple, const Tuple *target_tuple,
                            const UndoLog &log) -> UndoLog {
  // 如果旧日志说这行数据过去是不存在的（insert）
  // 那么无论你现在怎么Update，还是不存在，返回本身即可
  // insert
  if (log.is_deleted_) {
    return log;
  }
  // 不管你现在做了什么之前是空的，那就不需要更新了
  if (base_tuple == nullptr) {
    return log;
  }
  // 现在保证之前是存在的update转delete
  if (target_tuple == nullptr) {
    // 重构最古老的tuple，事务开始之前
    auto original_tuple = ReconstructTuple(schema, *base_tuple, TupleMeta{0, false}, {log});
    // 返回一条modified_fields全为true日志
    return UndoLog{false, std::vector<bool>(schema->GetColumnCount(), true), original_tuple.value(), log.ts_,
                   log.prev_version_};
  }
  std::vector<bool> new_modified_fields = log.modified_fields_;
  std::vector<Value> merged_values;
  std::vector<Column> merged_columns;

  // 还原旧日志的PartialSchema
  std::vector<Column> old_log_columns;
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    if (log.modified_fields_[i]) {
      old_log_columns.push_back(schema->GetColumn(i));
    }
  }
  Schema old_log_schema(old_log_columns);
  uint32_t old_log_idx = 0;
  // 逐列对比
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    if (log.modified_fields_[i]) {
      // 这个字段/ 列被改过
      // 保留老tuple，因为我们是还原成事务前的样子，老tuple就是事务执行前的value
      Value oldest_val = log.tuple_.GetValue(&old_log_schema, old_log_idx);
      merged_values.push_back(oldest_val);
      merged_columns.push_back(schema->GetColumn(i));
      old_log_idx++;
    } else {
      // 之前没有被改，比较一下，有没有被update到
      Value base_val = base_tuple->GetValue(schema, i);
      Value target_val = target_tuple->GetValue(schema, i);

      if (!base_val.CompareExactlyEquals(target_val)) {
        // 发现被修改了
        // basetuple这次才被搞到，所以装的就是事务执行前的数据，记得更新fileds
        new_modified_fields[i] = true;
        merged_values.push_back(base_val);
        merged_columns.push_back(schema->GetColumn(i));
      }
    }
  }

  // 重新整理tuple，schema
  Schema merged_schema(merged_columns);
  Tuple merged_tuple(merged_values, &merged_schema);

  // 组装新的undo，但是记得ts，prev_都要继承
  return UndoLog{false, new_modified_fields, merged_tuple, log.ts_, log.prev_version_};
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  // fmt::println(stderr, "debug_hook: {}", info);

  // fmt::println(stderr,
  //              "You see this line of text because you have not implemented "
  //              "`TxnMgrDbg`. You should do this once you have "
  //              "finished task 2. Implementing this helper function will save "
  //              "you a lot of time for debugging in later tasks.");

  // We recommend implementing this function as traversing the table heap and
  // print the version chain. An example output of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1
  //
  auto it = table_heap->MakeIterator();

  while (!it.IsEnd()) {
    RID rid = it.GetRID();
    // 拿到基线数据的元信息和本体
    TupleMeta meta = table_info->table_->GetTupleMeta(rid);
    Tuple tuple = table_info->table_->GetTuple(rid).second;

    // 打印基线数据 (Base Tuple)
    // 格式: RID=页号/槽号 ts=时间戳 tuple=(字段1, 字段2...)
    if (meta.is_deleted_) {
      fmt::println(stderr, "RID={}/{} ts={} <del marker> tuple={}", rid.GetPageId(), rid.GetSlotNum(), meta.ts_,
                   tuple.ToString(&table_info->schema_));
    } else {
      fmt::println(stderr, "RID={}/{} ts={} tuple={}", rid.GetPageId(), rid.GetSlotNum(), meta.ts_,
                   tuple.ToString(&table_info->schema_));
    }

    // 打印这条数据的 UndoLog 版本链
    auto opt_current_link = txn_mgr->GetUndoLink(rid);
    if (!opt_current_link.has_value()) {
      ++it;
      continue;
    }
    auto current_link = opt_current_link.value();
    // 只要链表没断，就一直往下爬
    while (current_link.IsValid()) {
      auto log_opt = txn_mgr->GetUndoLogOptional(current_link);
      // 日志丢失或已被垃圾回收，直接截断
      if (!log_opt.has_value()) {
        break;
      }
      UndoLog log = log_opt.value();
      // 打印这一条历史日志的信息
      if (log.is_deleted_) {
        // Delete日志，说明历史状态不存在
        fmt::println(stderr, "  txn{}@{} <del> ts={}", current_link.prev_txn_, current_link.prev_log_idx_, log.ts_);
      } else {
        // Update日志搓一个partial_schema，同时打印他
        std::vector<Column> partial_columns;
        for (uint32_t i = 0; i < table_info->schema_.GetColumnCount(); i++) {
          if (log.modified_fields_[i]) {
            partial_columns.push_back(table_info->schema_.GetColumn(i));
          }
        }
        Schema partial_schema(partial_columns);
        fmt::println(stderr, "  txn{}@{} {} ts={}", current_link.prev_txn_, current_link.prev_log_idx_,
                     log.tuple_.ToString(&partial_schema), log.ts_);
      }

      // 指针后移，继续看更古老的历史
      current_link = log.prev_version_;
    }

    // 看下一行物理数据
    ++it;
  }
}

}  // namespace bustub
