//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"
#include "common/macros.h"

namespace bustub {

/**
 * Construct a new LimitExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The limit plan to be executed
 * @param child_executor The child executor from which limited tuples are pulled
 */
LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // UNIMPLEMENTED("TODO(P3): Add implementation.");
}

/** Initialize the limit */
void LimitExecutor::Init() {
  child_executor_->Init();
  count_ = 0;
}

/**
 * Yield the next tuple batch from the limit.
 * @param[out] tuple_batch The next tuple batch produced by the limit
 * @param[out] rid_batch The next tuple RID batch produced by the limit
 * @param batch_size The number of tuples to be included in the batch (default:
 * BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto LimitExecutor::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                         size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();

  // 如果之前已经达到或者超过 Limit 上限了，直接拉闸！返回 false 结束查询。
  if (count_ >= plan_->GetLimit()) {
    return false;
  }

  // 计算还能要多少条数据
  // 如果 Limit 只剩 2 条，我们就别跟下游要一整批 (比如 128 条)
  // 了，免得下游做无用功。
  size_t remaining = plan_->GetLimit() - count_;
  size_t fetch_size = std::min(batch_size, remaining);

  // 向上游要数据
  bool status = child_executor_->Next(tuple_batch, rid_batch, fetch_size);

  // 如果上游没数据了，或者吐出来的是空的，直接结束
  if (!status || tuple_batch->empty()) {
    return false;
  }

  // 万一下游是个“老实人”，没听懂我们的 fetch_size，硬塞了超过我们上限的数据
  // 我们就强行把多余的切掉 (Resize)
  if (tuple_batch->size() > remaining) {
    tuple_batch->resize(remaining);
    rid_batch->resize(remaining);
  }

  // 5. 更新计数器，放行！
  count_ += tuple_batch->size();
  return true;
}

}  // namespace bustub
