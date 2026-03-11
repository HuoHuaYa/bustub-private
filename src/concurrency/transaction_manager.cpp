//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Begins a new transaction.
 * @param isolation_level an optional isolation level of the transaction.
 * @return an initialized transaction
 */
auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  // txn_map 的锁
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  // 新事务id
  auto txn_id = next_txn_id_.fetch_add(1);
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));

  // TODO(P4): set the timestamps here. Watermark updated below.
  txn_ref->read_ts_ = last_commit_ts_.load();
  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

/** @brief Verify if a txn satisfies serializability. We will not test this
 * function and you can change / remove it as you want. */
// 判断一个事务是否满足可串行化。
auto TransactionManager::VerifyTxn(Transaction *txn) -> bool {
  // 纯读不用管
  if (txn->GetWriteSets().empty()) {
    return true;
  }
  auto my_predicates = txn->GetScanPredicates();
  if (my_predicates.empty()) {
    return true;
  }

  // 遍历系统中所有的事务，注意加 txn_map_mutex_ 读锁
  std::shared_lock<std::shared_mutex> lck(txn_map_mutex_);

  for (const auto &[other_txn_id, other_txn] : txn_map_) {
    // 在我提交之前提交的事务
    if (other_txn->GetTransactionState() == TransactionState::COMMITTED &&
        other_txn->GetCommitTs() > txn->GetReadTs()) {
      // 查所有表
      for (const auto &[table_oid, rids] : other_txn->GetWriteSets()) {
        // 如果他改的表，刚好是我扫描过的表
        if (my_predicates.find(table_oid) != my_predicates.end()) {
          auto table_info = catalog_->GetTable(table_oid);
          auto predicates_for_table = my_predicates[table_oid];

          // 遍历他改过的每一行数据(RID)
          for (auto rid : rids) {
            auto [meta, current_tuple] = table_info->table_->GetTuple(rid);

            // 用我的扫描条件，去判断一下这行数据
            for (const auto &predicate : predicates_for_table) {
              if (predicate == nullptr) {
                return false;  // 全表扫描的祖宗，别人只要写了表，立刻冲突
              }


              // 别人把不符合条件的数据，改成了符合我条件的
              auto res_new = predicate->Evaluate(&current_tuple, table_info->schema_);
              if (!res_new.IsNull() && res_new.GetAs<bool>()) {
                return false;
              }

              // 别人把我原本能查到的数据，给改没了或者删了
              auto undo_link = GetUndoLink(rid);
              if (undo_link.has_value()) {
                auto undo_log = GetUndoLog(*undo_link);
                // 用最近的一条 UndoLog 重构出别人修改前的老模样
                auto history_tuple = ReconstructTuple(&table_info->schema_, current_tuple, meta, {undo_log});
                if (history_tuple.has_value()) {
                  auto res_old = predicate->Evaluate(&history_tuple.value(), table_info->schema_);
                  if (!res_old.IsNull() && res_old.GetAs<bool>()) {
                    return false;
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  return true;
  // 纯读不用管
  // if (txn->GetWriteSets().empty()) {
  //   return true;
  // }
  // auto my_predicates = txn->GetScanPredicates();
  // if (my_predicates.empty()) {
  //   return true;
  // }

  // // 遍历系统中所有的事务注意加 txn_map_mutex_ 读锁
  // std::shared_lock<std::shared_mutex> lck(txn_map_mutex_);

  // for (const auto &[other_txn_id, other_txn] : txn_map_) {
  //   // 在我提交之前提交的事务
  //   if (other_txn->GetTransactionState() == TransactionState::COMMITTED &&
  //       other_txn->GetCommitTs() > txn->GetReadTs()) {

  //     // 查所有表
  //     for (const auto &[table_oid, rids] : other_txn->GetWriteSets()) {

  //       // 如果他改的表，刚好是我扫描过的表
  //       if (my_predicates.find(table_oid) != my_predicates.end()) {
  //         auto table_info = catalog_->GetTable(table_oid);
  //         auto predicates_for_table = my_predicates[table_oid];

  //         // 遍历他改过的每一行数据(RID)
  //         for (auto rid : rids) {
  //           // auto tuple_meta = table_info->table_->GetTupleMeta(rid);
  //           auto tuple = table_info->table_->GetTuple(rid).second;

  //           // 用我的扫描条件，去判断一下他改过的数据
  //           for (const auto &predicate : predicates_for_table) {
  //             // 如果没有条件就是全表扫描，只要他改了这个表，立刻冲突
  //             if (predicate == nullptr) {
  //               return false;
  //             }

  //             // 拿着他修改后的实体数据，跑一下我的过滤条件
  //             auto result = predicate->Evaluate(&tuple, table_info->schema_);
  //             if (!result.IsNull() && result.GetAs<bool>()) {
  //               // 成功了就说明他会改变我也要改变的数据
  //               return false;
  //             }
  //           }
  //         }
  //       }
  //     }
  //   }
  // }

  // // 没有任何冲突
  // return true;
}

/**
 * Commits a transaction.
 * @param txn the transaction to commit, the txn will be managed by the txn
 * manager so no need to delete it by yourself
 */
auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }
  // TODO(P4): acquire commit ts!
  // 申请提交时间戳
  txn->commit_ts_ = last_commit_ts_.fetch_add(1) + 1;

  // TODO(P4): Implement the commit logic!
  for (const auto &[table_oid, rids] : txn->GetWriteSets()) {
    auto table_info = catalog_->GetTable(table_oid);
    for (const auto &rid : rids) {
      // 获得meta
      auto tuple_meta = table_info->table_->GetTupleMeta(rid);
      // 判断是不是这个事务的修改，如果是就把更新他的提交时间戳
      tuple_meta.ts_ = txn->commit_ts_;
      // 写回缓存池
      table_info->table_->UpdateTupleMeta(tuple_meta, rid);
    }
  }

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);

  // TODO(P4): set commit timestamp + update last committed timestamp here.
  // 设置提交时间戳，更新最新提交时间戳
  // last_commit_ts_.store(txn->commit_ts_);

  txn->state_ = TransactionState::COMMITTED;
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);

  return true;
}

/**
 * Aborts a transaction
 * @param txn the transaction to abort, the txn will be managed by the txn
 * manager so no need to delete it by yourself
 */
void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  // TODO(P4): Implement the abort logic!
  for (const auto &[table_oid, rids] : txn->GetWriteSets()) {
    auto table_info = catalog_->GetTable(table_oid);
    for (const auto &rid : rids) {
      // 获取当前主表数据和钩子
      auto [meta, base_tuple, undo_link] = GetTupleAndUndoLink(this, table_info->table_.get(), rid);

      // 如果这行数据不是我改的就不回滚它 (虽然正常情况下WriteSet里肯定是改的)，这是保险起见
      if (meta.ts_ == txn->GetTransactionTempTs()) {
        if (undo_link.has_value() && undo_link->IsValid()) {
          // 回滚Update/Delete操作
          auto undo_log = GetUndoLog(*undo_link);
          // ReconstructTuple接受vector，打包undo，就是要传被中断的时候的meta，函数就是这么实现的
          auto restored_tuple = ReconstructTuple(&table_info->schema_, base_tuple, meta, {undo_log});
          // 获得上个版本的meta
          TupleMeta old_meta{undo_log.ts_, undo_log.is_deleted_};
          // 一定要使用这个update，保证这些变量的原子性
          bool is_inserted = false;
          if (restored_tuple.has_value()) {
            // 过去是有数据的，连同数据一起砸回物理页
            is_inserted = UpdateTupleAndUndoLink(
                this, rid, undo_log.prev_version_, table_info->table_.get(), txn, old_meta, restored_tuple.value(),
                [txn](const TupleMeta &check_meta, const Tuple &check_tuple, RID check_rid,
                      std::optional<UndoLink> check_link) { return check_meta.ts_ == txn->GetTransactionTempTs(); });
          } else {
            // 过去是墓碑
            is_inserted = UpdateTupleAndUndoLink(
                this, rid, undo_log.prev_version_, table_info->table_.get(), txn, old_meta, base_tuple,
                [txn](const TupleMeta &check_meta, const Tuple &check_tuple, RID check_rid,
                      std::optional<UndoLink> check_link) { return check_meta.ts_ == txn->GetTransactionTempTs(); });
          }
          if (!is_inserted) {
            throw Exception("Abort failed: Tuple was modified by another transaction unexpectedly!");
          }
        } else {
          // 没有link说明是Insert操作
          TupleMeta tombstone_meta{0, true};
          UpdateTupleAndUndoLink(
              this, rid, std::nullopt, table_info->table_.get(), txn, tombstone_meta, base_tuple,
              [txn](const TupleMeta &check_meta, const Tuple &check_tuple, RID check_rid,
                    std::optional<UndoLink> check_link) { return check_meta.ts_ == txn->GetTransactionTempTs(); });
        }
      }
    }
  }
  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

/** @brief Stop-the-world garbage collection. Will be called only when all
 * transactions are not accessing the table heap. */
void TransactionManager::GarbageCollection() {
  // 清理垃圾意味着我们要修改底层的Map
  // 必须加上锁，防止其他线程此时正在读写这里
  std::unique_lock<std::shared_mutex> lock(txn_map_mutex_);

  // 获得水印（即最远ts）
  // 如果当前没有任何人在运行，水位线就是全世界最新的提交时间
  timestamp_t watermark = last_commit_ts_;
  // 正在运行的事务不能被删除，所以和水位线取min
  for (const auto &[txn_id, txn] : txn_map_) {
    if (txn->GetTransactionState() != TransactionState::COMMITTED &&
        txn->GetTransactionState() != TransactionState::ABORTED) {
      watermark = std::min(watermark, txn->GetReadTs());
    }
  }

  // 清洗txn_map
  for (auto it = txn_map_.begin(); it != txn_map_.end();) {
    auto *txn = it->second.get();
    bool is_garbage = false;
    // 没有undolog的人没必要留着
    if (txn->GetTransactionState() == TransactionState::COMMITTED ||
        txn->GetTransactionState() == TransactionState::ABORTED) {
      if (txn->GetUndoLogNum() == 0) {
        is_garbage = true;
      }
      // 已经commit的事务，且它的提交时间在最低水位线之前，就算垃圾了
      else if (txn->GetTransactionState() == TransactionState::COMMITTED) {
        if (txn->GetCommitTs() < watermark) {
          is_garbage = true;
        }
      }
      // abort也一样
      else if (txn->GetTransactionState() == TransactionState::ABORTED) {
        if (txn->GetReadTs() < watermark) {
          is_garbage = true;
        }
      }
    }

    // 执行物理超度
    if (is_garbage) {
      // 在BusTub中，txn_map_的Value是std::shared_ptr<Transaction>。
      // 我们根本不需要手动去 delete 它的内存，也不需要去遍历清空它内部的 undo_logs。
      // 只要把它从 Map 中 erase 掉，shared_ptr 的引用计数归零，
      // C++的智能指针会自动析构整个Transaction对象，顺带炸毁它肚子里的所有 UndoLog！
      // erase 会返回下一个合法元素的迭代器
      it = txn_map_.erase(it);
    } else {
      // 如果不是垃圾，检查下一个
      ++it;
    }
  }
}

}  // namespace bustub
