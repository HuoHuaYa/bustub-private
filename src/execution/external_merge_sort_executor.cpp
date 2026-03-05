//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// external_merge_sort_executor.cpp
//
// Identification: src/execution/external_merge_sort_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/external_merge_sort_executor.h"
#include "common/macros.h"
#include "execution/plans/sort_plan.h"

#include <algorithm>
#include <deque>
#include <vector>

namespace bustub {

template <size_t K>
ExternalMergeSortExecutor<K>::ExternalMergeSortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                                                        std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), cmp_(plan->GetOrderBy()), child_executor_(std::move(child_executor)) {
  // UNIMPLEMENTED("TODO(P3): Add implementation.");
}

template <size_t K>
ExternalMergeSortExecutor<K>::~ExternalMergeSortExecutor() {
  // 如果执行器被销毁时，runs_ 里还有没删干净的页面（比如最终结果页）
  // 必须全部清空，把磁盘空间还给系统！
  final_iter_ = std::nullopt;
  auto bpm = exec_ctx_->GetBufferPoolManager();

  for (auto &run : runs_) {
    for (auto page_id : run.GetPages()) {  // 用你之前加的那个 Getter 方法
      bpm->DeletePage(page_id);
    }
  }
}

/** Initialize the external merge sort */
template <size_t K>
void ExternalMergeSortExecutor<K>::Init() {
  child_executor_->Init();
  runs_.clear();
  final_iter_ = std::nullopt;
  auto bpm = exec_ctx_->GetBufferPoolManager();
  for (auto &run : runs_) {
    for (auto page_id : run.GetPages()) {
      bpm->DeletePage(page_id);
    }
  }
  runs_.clear();
  std::vector<Tuple> child_batch;
  std::vector<RID> child_rids;
  const auto &schema = child_executor_->GetOutputSchema();
  // auto bpm = exec_ctx_->GetBufferPoolManager();

  // 造砖

  std::vector<SortEntry> chunk;
  size_t current_chunk_bytes = 0;
  // 我们设定每次在内存里最多囤 4 个页面的数据量，防OOM
  const size_t max_chunk_bytes = BUSTUB_PAGE_SIZE * 4;

  auto flush_chunk_to_disk = [&]() {
    if (chunk.empty()) {
      return;
    }
    // 在内存里快排
    std::sort(chunk.begin(), chunk.end(), cmp_);

    // 准备落盘
    std::vector<page_id_t> run_pages;
    page_id_t current_pid = bpm->NewPage();
    run_pages.push_back(current_pid);
    auto guard = bpm->WritePage(current_pid);
    auto *ir_page = guard.AsMut<IntermediateResultPage>();
    ir_page->Init();

    // 把排序好的数据塞进磁盘页
    for (const auto &entry : chunk) {
      if (!ir_page->InsertTuple(entry.second)) {
        // 这一页塞满了，换新页！
        // 重置落盘的条件
        guard.Drop();

        current_pid = bpm->NewPage();
        run_pages.push_back(current_pid);
        guard = bpm->WritePage(current_pid);
        ir_page = guard.AsMut<IntermediateResultPage>();
        ir_page->Init();

        // 强行塞入新页
        bool success = ir_page->InsertTuple(entry.second);
        BUSTUB_ASSERT(success, "Tuple size exceeds a whole page!");
      }
    }
    // 删除guard
    guard.Drop();
    runs_.emplace_back(run_pages, bpm);  // 生成了一个数据块
    chunk.clear();
    current_chunk_bytes = 0;
  };

  // 抽水+生成数据块
  while (child_executor_->Next(&child_batch, &child_rids, 128)) {
    for (const auto &tuple : child_batch) {
      SortKey key = GenerateSortKey(tuple, plan_->GetOrderBy(), schema);
      chunk.emplace_back(std::move(key), tuple);
      current_chunk_bytes += tuple.GetLength();

      if (current_chunk_bytes >= max_chunk_bytes) {
        flush_chunk_to_disk();
      }
    }
    child_batch.clear();
    child_rids.clear();
  }
  // 扫尾：把最后不足一个 Chunk 的数据也刷下去
  flush_chunk_to_disk();

  // 如果根本没有数据，直接退出
  if (runs_.empty()) {
    return;
  }

  // 归并数据块们

  while (runs_.size() > 1) {
    std::deque<MergeSortRun> next_level_runs;

    // 每次抓两个出来合并
    while (runs_.size() >= 2) {
      auto run1 = runs_.front();
      runs_.pop_front();
      auto run2 = runs_.front();
      runs_.pop_front();
      next_level_runs.push_back(MergeTwoRuns(run1, run2));
    }
    // 如果还落单了一个，直接晋级下一轮
    if (!runs_.empty()) {
      next_level_runs.push_back(runs_.front());
      runs_.pop_front();
    }
    runs_ = std::move(next_level_runs);
  }

  // 此时，runs_ 里只剩下唯一的、完全有序的Run
  final_iter_ = runs_.front().Begin();
}

// merge过程
template <size_t K>
auto ExternalMergeSortExecutor<K>::MergeTwoRuns(MergeSortRun &run1, MergeSortRun &run2) -> MergeSortRun {
  auto it1 = run1.Begin();
  auto it2 = run2.Begin();
  const auto &schema = child_executor_->GetOutputSchema();
  auto bpm = exec_ctx_->GetBufferPoolManager();

  std::vector<page_id_t> out_pages;
  page_id_t out_pid = bpm->NewPage();
  out_pages.push_back(out_pid);
  auto guard = bpm->WritePage(out_pid);
  auto *ir_page = guard.AsMut<IntermediateResultPage>();
  ir_page->Init();
  // 可能失败，说明这个page满了
  auto insert_to_out = [&](const Tuple &tuple) {
    if (!ir_page->InsertTuple(tuple)) {
      // 重置一个新的page，再次insert
      guard.Drop();
      out_pid = bpm->NewPage();
      out_pages.push_back(out_pid);
      guard = bpm->WritePage(out_pid);
      ir_page = guard.AsMut<IntermediateResultPage>();
      ir_page->Init();
      ir_page->InsertTuple(tuple);
    }
  };

  // 真正的merge
  while (it1 != run1.End() && it2 != run2.End()) {
    Tuple t1 = *it1;
    Tuple t2 = *it2;
    // 重建 SortKey 进行比较
    SortEntry e1{GenerateSortKey(t1, plan_->GetOrderBy(), schema), t1};
    SortEntry e2{GenerateSortKey(t2, plan_->GetOrderBy(), schema), t2};

    if (cmp_(e1, e2)) {
      insert_to_out(t1);
      ++it1;
    } else {
      insert_to_out(t2);
      ++it2;
    }
  }

  // 把剩下的尾巴扫进去
  while (it1 != run1.End()) {
    insert_to_out(*it1);
    ++it1;
  }
  while (it2 != run2.End()) {
    insert_to_out(*it2);
    ++it2;
  }
  it1 = run1.End();
  it2 = run2.End();
  guard.Drop();  // 释放最后一页
  // 旧的全部放转转回收了
  for (auto page_id : run1.GetPages()) {
    bpm->DeletePage(page_id);
  }
  for (auto page_id : run2.GetPages()) {
    bpm->DeletePage(page_id);
  }
  return {out_pages, bpm};
}

/**
 * Yield the next tuple batch from the external merge sort.
 * @param[out] tuple_batch The next tuple batch produced by the external merge
 * sort.
 * @param[out] rid_batch The next tuple RID batch produced by the external merge
 * sort.
 * @param batch_size The number of tuples to be included in the batch (default:
 * BUSTUB_BATCH_SIZE)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
template <size_t K>
auto ExternalMergeSortExecutor<K>::Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch,
                                        size_t batch_size) -> bool {
  tuple_batch->clear();
  rid_batch->clear();

  if (!final_iter_.has_value() || runs_.empty()) {
    return false;
  }

  auto &it = final_iter_.value();
  auto end = runs_.front().End();
  // 把所有tuple抽完为止
  while (tuple_batch->size() < batch_size && it != end) {
    tuple_batch->push_back(*it);
    rid_batch->push_back(RID{});
    ++it;  // 迭代器继续往前滚
  }
  if (it == end) {
    final_iter_ = std::nullopt;  // 销毁迭代器释放读锁
    auto bpm = exec_ctx_->GetBufferPoolManager();
    for (auto &run : runs_) {
      for (auto page_id : run.GetPages()) {
        bpm->DeletePage(page_id);
      }
    }
    runs_.clear();
  }
  return !tuple_batch->empty();
}

template class ExternalMergeSortExecutor<2>;

}  // namespace bustub
