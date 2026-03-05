//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/macros.h"
#include <memory>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

/**
 * Construct a new AggregationExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The insert plan to be executed
 * @param child_executor The child executor from which inserted tuples are
 * pulled (may be `nullptr`)
 */
AggregationExecutor::AggregationExecutor(
    ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
    std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {
  // UNIMPLEMENTED("TODO(P3): Add implementation.");
}

/** Initialize the aggregation */
void AggregationExecutor::Init() {
  std::cout << "Init() Start" << std::endl;
  // UNIMPLEMENTED("TODO(P3): Add implementation.");
  child_executor_->Init();
  // 在嵌套循环中可能会执行多次init
  aht_.Clear();

  std::vector<Tuple> child_tuples;
  std::vector<RID> child_rids;

  // int debug_count = 0;
  // 只要下层还有数据，就一直抽 (batch_size 设为 1，或者更大都行，这里设 1
  // 为了逻辑清晰)
  while (child_executor_->Next(&child_tuples, &child_rids, 128)) {
    // debug_count++;
    // std::cout<<"child next"<<' '<<debug_count<<'\n';
    // if(debug_count > 10000) {
    //   // std::cout<<"warning \n";
    //   break;
    // }
    for (const auto &tuple : child_tuples) {
      // 从底层的 Tuple 中，提取出我们用来分组的 Key，和用来计算的 Value
      AggregateKey key = MakeAggregateKey(&tuple);
      AggregateValue val = MakeAggregateValue(&tuple);

      // 塞进哈希表。如果 Key 不存在会自动创建，如果存在就会调用你刚才写的
      // CombineAggregateValues 去累加！
      aht_.InsertCombine(key, val);
    }
    child_tuples.clear();
    child_rids.clear();
  }
  // SQL: SELECT COUNT(*) FROM empty_table;
  // 由于表是空的，上面的 while 循环一次都不会进，哈希表是空的。
  // 如果没有任何 GROUP BY (即 empty())，按照 SQL 标准，全局聚合空表必须返回 1
  // 行初始数据 (Count=0) 如果有 GROUP BY，空表返回空集，就不用走这个 if。
  if (aht_.Begin() == aht_.End() && plan_->GetGroupBys().empty()) {
    // 强行往空哈希表里塞一条 Key为空、Value为 0/NULL 初始值的记录。
    aht_.InsertInitial({});
  }

  // 数据全在哈希表里了。把我们用来输出的游标指到哈希表的最开头,在next使用
  aht_iterator_ = aht_.Begin();
}

/**
 * Yield the next tuple batch from the aggregation.
 * @param[out] tuple_batch The next batch of tuples produced by the aggregation
 * @param[out] rid_batch The next batch of tuple RIDs produced by the
 * aggregation
 * @param batch_size The number of tuples to be included in the batch (default:
 * BUSTUB_BATCH_SIZE)
 * @return `true` if any tuples were produced, `false` if there are no more
 * tuples
 */

auto AggregationExecutor::Next(std::vector<bustub::Tuple> *tuple_batch,
                               std::vector<bustub::RID> *rid_batch,
                               size_t batch_size) -> bool {

  tuple_batch->clear();
  rid_batch->clear();

  // std::cout << "Next() Start" << std::endl;
  bool emitted = false;
  // 外界可能一次要 batch_size
  // 条数据。我们就循环去游标里拿，直到拿够，或者游标走到头。
  while (tuple_batch->size() < batch_size && aht_iterator_ != aht_.End()) {
    std::vector<Value> values;

    // AggregationPlanNode 的输出列格式永远是固定的,所有的 GroupBy + 所有的
    // Aggregate 所以我们必须按照这个死顺序，把内存里的 Key 和 Value
    // 拼接成一个大数组。
    values.insert(values.end(), aht_iterator_.Key().group_bys_.begin(),
                  aht_iterator_.Key().group_bys_.end());
    values.insert(values.end(), aht_iterator_.Val().aggregates_.begin(),
                  aht_iterator_.Val().aggregates_.end());

    // 用拼好的数组，生成外界认识的 Tuple，塞进输出批次里。
    Tuple tuple_{values, &GetOutputSchema()};
    tuple_batch->emplace_back(tuple_);

    // 聚合产生的结果是虚拟出来的统计数据，在磁盘上没有真实的门牌号
    // (RID)，给上层返回一个空的/假的 RID 充数。
    rid_batch->emplace_back(RID{});

    // 游标往前推一格
    ++aht_iterator_;
    emitted = true;
  }

  // 如果 tuple_batch 不为空，说明这次成功发出了数据，返回
  // true；如果连一滴数据都挤不出来了，返回 false 通知上层结束。
  return emitted;
}

/** Do not use or remove this function; otherwise, you will get zero points. */
auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * {
  return child_executor_.get();
}

} // namespace bustub
