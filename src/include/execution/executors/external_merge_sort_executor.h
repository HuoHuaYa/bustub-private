//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// external_merge_sort_executor.h
//
// Identification:
// src/include/execution/executors/external_merge_sort_executor.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <memory>
#include <utility>
#include <vector>
#include "common/config.h"
#include "common/macros.h"
#include "execution/execution_common.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/sort_plan.h"
#include "storage/page/intermediate_result_page.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * A data structure that holds the sorted tuples as a run during external merge
 * sort. Tuples might be stored in multiple pages, and tuples are ordered both
 * within one page and across pages.
 */
class MergeSortRun {
 public:
  MergeSortRun() = default;
  MergeSortRun(std::vector<page_id_t> pages, BufferPoolManager *bpm) : pages_(std::move(pages)), bpm_(bpm) {}

  auto GetPageCount() -> size_t { return pages_.size(); }
  auto GetPages() const -> const std::vector<page_id_t> & { return pages_; }

  /** Iterator for iterating on the sorted tuples in one run. */
  class Iterator {
    friend class MergeSortRun;

   public:
    Iterator() = default;

    /**
     * Advance the iterator to the next tuple. If the current sort page is
     * exhausted, move to the next sort page.
     */
    auto operator++() -> Iterator & {
      BUSTUB_ASSERT(current_ir_page_ != nullptr, "Iterator is out of bounds!");

      tuple_idx_++;

      if (tuple_idx_ >= current_ir_page_->GetNumTuples()) {
        page_idx_++;
        tuple_idx_ = 0;

        if (page_idx_ < run_->pages_.size()) {
          // 还有下一页，自动加载（内部会触发上一页的 Unpin）
          LoadCurrentPage();
        } else {
          // 🚨 读到尽头了！直接清空 guard，触发最后一页的自动 Unpin 🚨
          current_guard_->Drop();
          current_ir_page_ = nullptr;
        }
      }
      return *this;
    }

    /**
     * Dereference the iterator to get the current tuple in the sorted run that
     * the iterator is pointing to.
     */
    auto operator*() -> Tuple {
      BUSTUB_ASSERT(current_ir_page_ != nullptr, "Iterator is dereferencing a null page!");
      Tuple tuple;
      current_ir_page_->GetTuple(tuple_idx_, &tuple);
      return tuple;
    }

    /**
     * Checks whether two iterators are pointing to the same tuple in the same
     * sorted run.
     */
    auto operator==(const Iterator &other) const -> bool {
      return run_ == other.run_ && page_idx_ == other.page_idx_ && tuple_idx_ == other.tuple_idx_;
    }

    /**
     * Checks whether two iterators are pointing to different tuples in a sorted
     * run or iterating on different sorted runs.
     */
    auto operator!=(const Iterator &other) const -> bool { return !(*this == other); }

   private:
    explicit Iterator(const MergeSortRun *run) : run_(run) {}

    Iterator(const MergeSortRun *run, size_t page_idx, uint32_t tuple_idx)
        : run_(run), page_idx_(page_idx), tuple_idx_(tuple_idx) {
      if (run_ != nullptr && page_idx_ < run_->pages_.size()) {
        LoadCurrentPage();
      }
    }

    const MergeSortRun *run_;
    size_t page_idx_{0};
    uint32_t tuple_idx_{0};

    std::optional<ReadPageGuard> current_guard_{std::nullopt};
    const IntermediateResultPage *current_ir_page_{nullptr};

    void LoadCurrentPage() {
      // 如果当前还有 guard，给它赋 nullopt，它的析构函数会自动帮我们 Unpin
      // 上一页！
      current_guard_ = std::nullopt;

      page_id_t current_pid = run_->pages_[page_idx_];

      // 使用ReadPage 向 BufferPoolManager 借出这页
      current_guard_ = run_->bpm_->ReadPage(current_pid);

      // 从guard中获取裸数据指针并强转为我们的IntermediateResultPage
      current_ir_page_ = current_guard_->template As<IntermediateResultPage>();
    }
  };

  /**
   * Get an iterator pointing to the beginning of the sorted run, i.e. the first
   * tuple.
   */
  auto Begin() -> Iterator { return {this, 0, 0}; }

  /**
   * Get an iterator pointing to the end of the sorted run, i.e. the position
   * after the last tuple.
   */
  auto End() -> Iterator {
    // C++ 迭代器的标准习惯：End() 永远指向最后一个元素的“下一个位置”。
    // 所以这里的 page_idx 是 pages_.size()，表示完全越界的状态。
    return {this, pages_.size(), 0};
  }

 private:
  /** The page IDs of the sort pages that store the sorted tuples. */
  std::vector<page_id_t> pages_;
  /**
   * The buffer pool manager used to read sort pages. The buffer pool manager is
   * responsible for deleting the sort pages when they are no longer needed.
   */
  [[maybe_unused]] BufferPoolManager *bpm_;
};

/**
 * ExternalMergeSortExecutor executes an external merge sort.
 *
 * In Spring 2025, only 2-way external merge sort is required.
 */
template <size_t K>
class ExternalMergeSortExecutor : public AbstractExecutor {
 public:
  ExternalMergeSortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                            std::unique_ptr<AbstractExecutor> &&child_executor);
  ~ExternalMergeSortExecutor() override;
  void Init() override;

  auto Next(std::vector<bustub::Tuple> *tuple_batch, std::vector<bustub::RID> *rid_batch, size_t batch_size)
      -> bool override;

  /** @return The output schema for the external merge sort */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The sort plan node to be executed */
  const SortPlanNode *plan_;

  /** Compares tuples based on the order-bys */
  TupleComparator cmp_;

  /** TODO(P3): You will want to add your own private members here. */
  std::unique_ptr<AbstractExecutor> child_executor_;

  // 用来存放我们在磁盘上造出来的一个个有序数据块
  std::deque<MergeSortRun> runs_;

  // 记录最终那个巨无霸 Run 的读取位置
  std::optional<MergeSortRun::Iterator> final_iter_{std::nullopt};

  // 私有辅助函数，用来将两个 Run 合并成一个更大的 Run
  auto MergeTwoRuns(MergeSortRun &run1, MergeSortRun &run2) -> MergeSortRun;
};

}  // namespace bustub
