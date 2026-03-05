//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// window_function_executor.cpp
//
// Identification: src/execution/window_function_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/window_function_executor.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

#include <algorithm>
#include <numeric>

namespace bustub {

/**
 * Construct a new WindowFunctionExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The window aggregation plan to be executed
 */
WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

/** Initialize the window aggregation */
void WindowFunctionExecutor::Init() {
  // throw NotImplementedException("WindowFunctionExecutor is not implemented");
  child_executor_->Init();
  output_tuples_.clear();
  cursor_ = 0;

  // 把下游的所有数据抽干，存进原始水库
  std::vector<Tuple> child_tuples;
  std::vector<Tuple> child_batch;
  std::vector<RID> child_rids;
  while (child_executor_->Next(&child_batch, &child_rids, 128)) {
    for (const auto &t : child_batch) {
      child_tuples.push_back(t);
    }
    child_batch.clear();
    child_rids.clear();
  }

  if (child_tuples.empty()) {
    return;
  }

  const auto &schema = child_executor_->GetOutputSchema();

  // 记录每个窗口函数列计算出来的结果。Key是列号，Value是每一行对应的计算结果
  std::unordered_map<uint32_t, std::vector<Value>> wf_results;

  // 创建一个索引数组 [0, 1, 2, ..., n-1]
  std::vector<uint32_t> indices(child_tuples.size());
  std::iota(indices.begin(), indices.end(), 0);
  if (!plan_->window_functions_.empty()) {
    // 同一个wf中partition ，order都一样，所以这里只排序一次就可以了
    const auto &first_wf = plan_->window_functions_.begin()->second;
    std::sort(indices.begin(), indices.end(), [&](uint32_t a_idx, uint32_t b_idx) {
      const Tuple &a = child_tuples[a_idx];
      const Tuple &b = child_tuples[b_idx];
      // 不考虑排序，只是按照类型分组
      // 第一优先级：PartitionBy
      for (const auto &expr : first_wf.partition_by_) {
        Value val_a = expr->Evaluate(&a, schema);
        Value val_b = expr->Evaluate(&b, schema);
        if (val_a.CompareEquals(val_b) == CmpBool::CmpTrue) {
          continue;
        }
        bool a_null = val_a.IsNull();
        bool b_null = val_b.IsNull();
        if (a_null && b_null) {
          continue;
        }
        if (a_null || b_null) {
          return a_null;
        }
        return val_a.CompareLessThan(val_b) == CmpBool::CmpTrue;
      }

      // 第二优先级：OrderBy排序
      for (const auto &ob : first_wf.order_by_) {
        auto order_type = std::get<0>(ob);
        auto null_type = std::get<1>(ob);
        auto expr = std::get<2>(ob);

        Value val_a = expr->Evaluate(&a, schema);
        Value val_b = expr->Evaluate(&b, schema);
        if (val_a.CompareEquals(val_b) == CmpBool::CmpTrue) {
          continue;
        }
        bool a_null = val_a.IsNull();
        bool b_null = val_b.IsNull();
        if (a_null && b_null) {
          continue;
        }
        if (a_null || b_null) {
          if (null_type == OrderByNullType::NULLS_FIRST) {
            return a_null;
          }
          if (null_type == OrderByNullType::NULLS_LAST) {
            return b_null;
          }
          if (order_type == OrderByType::DESC) {
            return b_null;
          }
          return a_null;
        }

        if (order_type == OrderByType::DEFAULT || order_type == OrderByType::ASC) {
          return val_a.CompareLessThan(val_b) == CmpBool::CmpTrue;
        }
        return val_a.CompareGreaterThan(val_b) == CmpBool::CmpTrue;
      }
      return false;
    });
  }
  // 逐个攻破每一个窗口函数
  // wf窗口函数：包含分组规则partition_by，排序规则order_by，计算引擎function，窗口类型type
  for (const auto &[col_idx, wf] : plan_->window_functions_) {
    wf_results[col_idx] = std::vector<Value>(child_tuples.size());
    size_t n = indices.size();
    // 巨大的滑动窗口，每个分区单独处理
    // 遍历排好序的索引，切分出一个个的分区
    for (size_t start = 0; start < n;) {
      size_t end = start + 1;
      while (end < n) {
        bool same_partition = true;
        for (const auto &expr : wf.partition_by_) {
          Value val_a = expr->Evaluate(&child_tuples[indices[start]], schema);
          Value val_b = expr->Evaluate(&child_tuples[indices[end]], schema);
          if (val_a.CompareEquals(val_b) != CmpBool::CmpTrue) {
            if (!(val_a.IsNull() && val_b.IsNull())) {
              same_partition = false;
              break;
            }
          }
        }
        if (!same_partition) {
          break;
        }
        end++;
      }

      // 在当前分区 [start, end) 内部进行聚合计算！
      Value running_val = ValueFactory::GetNullValueByType(TypeId::INTEGER);
      bool initialized = false;
      // 对count，countstar进行初始化0，其他默认用null
      if (wf.type_ == WindowFunctionType::CountAggregate || wf.type_ == WindowFunctionType::CountStarAggregate) {
        running_val = Value(TypeId::INTEGER, 0);
        initialized = true;
      }

      if (wf.order_by_.empty()) {
        // 场景 A：没有 Order
        // By。算出整个分区的总和，然后无脑分发给分区里的所有人
        for (size_t j = start; j < end; j++) {
          if (wf.type_ == WindowFunctionType::Rank) {
            running_val = Value(TypeId::INTEGER, 1);
          } else {
            Value val = wf.function_->Evaluate(&child_tuples[indices[j]], schema);
            if (wf.type_ == WindowFunctionType::CountStarAggregate) {
              running_val = running_val.Add(Value(TypeId::INTEGER, 1));
            } else if (wf.type_ == WindowFunctionType::CountAggregate) {
              if (!val.IsNull()) {
                running_val = running_val.Add(Value(TypeId::INTEGER, 1));
              }
            } else {
              if (!val.IsNull()) {
                // 没有初始化，就直接赋值
                if (!initialized) {
                  running_val = val;
                  initialized = true;
                } else {
                  if (wf.type_ == WindowFunctionType::SumAggregate) {
                    running_val = running_val.Add(val);
                  } else if (wf.type_ == WindowFunctionType::MinAggregate) {
                    running_val = running_val.CompareLessThan(val) == CmpBool::CmpTrue ? running_val : val;
                  } else if (wf.type_ == WindowFunctionType::MaxAggregate) {
                    running_val = running_val.CompareGreaterThan(val) == CmpBool::CmpTrue ? running_val : val;
                  }
                }
              }
            }
          }
        }
        for (size_t j = start; j < end; j++) {
          wf_results[col_idx][indices[j]] = running_val;
        }
      } else {
        // 场景 B：有 Order By。计算累计值 (Running Total)，遇到相等的同辈
        // (Peer) 要一起算
        for (size_t j = start; j < end;) {
          size_t peer_end = j + 1;

          while (peer_end < end) {
            bool is_peer = true;
            for (const auto &ob : wf.order_by_) {
              auto expr = std::get<2>(ob);
              Value val_a = expr->Evaluate(&child_tuples[indices[j]], schema);
              Value val_b = expr->Evaluate(&child_tuples[indices[peer_end]], schema);
              if (val_a.CompareEquals(val_b) != CmpBool::CmpTrue) {
                if (!(val_a.IsNull() && val_b.IsNull())) {
                  is_peer = false;
                  break;
                }
              }
            }
            if (!is_peer) {
              break;
            }
            peer_end++;
          }
          // 没有orderby rank没有意义，有orderby就有意义，同辈排名相同
          if (wf.type_ == WindowFunctionType::Rank) {
            running_val = Value(TypeId::INTEGER, static_cast<int32_t>(j - start + 1));
            for (size_t k = j; k < peer_end; k++) {
              wf_results[col_idx][indices[k]] = running_val;
            }
          } else {
            // 和没有orderby 一样
            for (size_t k = j; k < peer_end; k++) {
              Value val = wf.function_->Evaluate(&child_tuples[indices[k]], schema);
              if (wf.type_ == WindowFunctionType::CountStarAggregate) {
                running_val = running_val.Add(Value(TypeId::INTEGER, 1));
              } else if (wf.type_ == WindowFunctionType::CountAggregate) {
                if (!val.IsNull()) {
                  running_val = running_val.Add(Value(TypeId::INTEGER, 1));
                }
              } else {
                if (!val.IsNull()) {
                  if (!initialized) {
                    running_val = val;
                    initialized = true;
                  } else {
                    if (wf.type_ == WindowFunctionType::SumAggregate) {
                      running_val = running_val.Add(val);
                    } else if (wf.type_ == WindowFunctionType::MinAggregate) {
                      running_val = running_val.CompareLessThan(val) == CmpBool::CmpTrue ? running_val : val;
                    } else if (wf.type_ == WindowFunctionType::MaxAggregate) {
                      running_val = running_val.CompareGreaterThan(val) == CmpBool::CmpTrue ? running_val : val;
                    }
                  }
                }
              }
            }
            for (size_t k = j; k < peer_end; k++) {
              wf_results[col_idx][indices[k]] = running_val;
            }
          }
          j = peer_end;
        }
      }
      start = end;
    }
  }

  // 将原始数据和刚刚算出的窗口列，严格按照图纸 columns_ 的顺序拼装成最终 Tuple
  for (size_t i = 0; i < child_tuples.size(); i++) {
    uint32_t real_idx = indices[i];  // 🚨 获取排序后第 i 位置对应的原始数据下标
    std::vector<Value> out_values;

    for (size_t col_idx = 0; col_idx < plan_->columns_.size(); col_idx++) {
      if (wf_results.count(col_idx) > 0) {
        // 这里的 wf_results[col_idx] 长度和 child_tuples 一样
        // 我们要拿的是排序后该位置对应的计算值
        out_values.push_back(wf_results[col_idx][real_idx]);
      } else {
        // 普通列也要根据 real_idx 去原始数据里取
        out_values.push_back(plan_->columns_[col_idx]->Evaluate(&child_tuples[real_idx], schema));
      }
    }
    output_tuples_.emplace_back(out_values, &GetOutputSchema());
  }
}

/**
 * Yield the next tuple batch from the window aggregation.
 * @param[out] tuple_batch The next tuple batch produced by the window
 * aggregation
 * @param[out] rid_batch The next tuple RID batch produced by the window
 * aggregation
 * @param batch_size The number of tuples to be included in the batch (default:
 * BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto WindowFunctionExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                                  size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();

  while (tuple_batch->size() < batch_size && cursor_ < output_tuples_.size()) {
    tuple_batch->push_back(output_tuples_[cursor_]);
    rid_batch->push_back(RID{});
    cursor_++;
  }

  return !tuple_batch->empty();
}
}  // namespace bustub
