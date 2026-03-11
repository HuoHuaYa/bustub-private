//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard.cpp
//
// Identification: src/storage/page/page_guard.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/page_guard.h"
#include "buffer/arc_replacer.h"
#include "common/macros.h"
#include <memory>

namespace bustub {

/**
 * @brief The only constructor for an RAII `ReadPageGuard` that creates a valid
 * guard.
 *
 * Note that only the buffer pool manager is allowed to call this constructor.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The page ID of the page we want to read.
 * @param frame A shared pointer to the frame that holds the page we want to
 * protect.
 * @param replacer A shared pointer to the buffer pool manager's replacer.
 * @param bpm_latch A shared pointer to the buffer pool manager's latch.
 * @param disk_scheduler A shared pointer to the buffer pool manager's disk
 * scheduler.
 */
ReadPageGuard::ReadPageGuard(page_id_t page_id,
                             std::shared_ptr<FrameHeader> frame,
                             std::shared_ptr<ArcReplacer> replacer,
                             std::shared_ptr<std::mutex> bpm_latch,
                             std::shared_ptr<DiskScheduler> disk_scheduler)
    : page_id_(page_id), frame_(std::move(frame)),
      replacer_(std::move(replacer)), bpm_latch_(std::move(bpm_latch)),
      disk_scheduler_(std::move(disk_scheduler)) {
  is_valid_ = true;
  // frame_->rwlatch_.lock_shared();
  // Getlock();
}

/**
 * @brief The move constructor for `ReadPageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with
 * learning materials online. There are many great resources (including
 * articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard; otherwise, you might run into
 * double free problems! For both objects, you need to update _at least_ 5
 * fields each.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 */
// 移动构造
ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  frame_ = std::move(that.frame_);
  replacer_ = std::move(that.replacer_);
  bpm_latch_ = std::move(that.bpm_latch_);
  disk_scheduler_ = std::move(that.disk_scheduler_);
  page_id_ = that.page_id_;
  is_valid_ = that.is_valid_;

  that.is_valid_ = false;
  that.page_id_ = INVALID_PAGE_ID;
  that.frame_ = nullptr;
  that.replacer_ = nullptr;
  that.bpm_latch_ = nullptr;
  that.disk_scheduler_ = nullptr;

} // page_id , fram_....都是指针，所以还要置为空

/**
 * @brief The move assignment operator for `ReadPageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with
 * learning materials online. There are many great resources (including
 * articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard; otherwise, you might run into
 * double free problems! For both objects, you need to update _at least_ 5
 * fields each, and for the current object, make sure you release any resources
 * it might be holding on to.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 * @return ReadPageGuard& The newly valid `ReadPageGuard`.
 */
// 移动
auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept
    -> ReadPageGuard & {
  if (this == &that) {
    return *this;
  }
  Drop();
  this->frame_ = std::move(that.frame_);
  this->replacer_ = std::move(that.replacer_);
  this->bpm_latch_ = std::move(that.bpm_latch_);
  this->disk_scheduler_ = std::move(that.disk_scheduler_);
  this->page_id_ = that.page_id_;
  this->is_valid_ = that.is_valid_;

  that.is_valid_ = false;
  that.page_id_ = INVALID_PAGE_ID;
  that.frame_ = nullptr;
  that.replacer_ = nullptr;
  that.bpm_latch_ = nullptr;
  that.disk_scheduler_ = nullptr;
  return *this;
}

/**
 * @brief Gets the page ID of the page this guard is protecting.
 */
auto ReadPageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return page_id_;
}

/**
 * @brief Gets a `const` pointer to the page of data this guard is protecting.
 */
auto ReadPageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return frame_->GetData();
}

/**
 * @brief Returns whether the page is dirty (modified but not flushed to the
 * disk).
 */
auto ReadPageGuard::IsDirty() const -> bool {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return frame_->is_dirty_;
}

/**
 * @brief Flushes this page's data safely to disk.
 *
 * TODO(P1): Add implementation.
 */
// 把数据刷到磁盘
void ReadPageGuard::Flush() {
  if (!is_valid_ || frame_ == nullptr || disk_scheduler_ == nullptr) {
    return;
  }
  DiskRequest disk_request;
  // 构造promise
  std::promise<bool> req_promise;
  // 构造future
  std::future<bool> req_future = req_promise.get_future();
  // 移动构造
  std::vector<DiskRequest> disk_requests;
  // promise-future
  disk_request.callback_ = std::move(req_promise);

  disk_request.is_write_ = true;
  // 因为他可能在构建pageguard后手动修改data，所以这里getdatamut会使帧变脏，我们要避免
  disk_request.data_ = const_cast<char *>(frame_->GetData());
  disk_request.page_id_ = page_id_;

  disk_requests.push_back(std::move(disk_request));
  disk_scheduler_->Schedule(disk_requests);
  // frame_->pin_count_.fetch_sub(1);
  if (req_future.get()) {
    frame_->is_dirty_ = false;
  }
}

/**
 * @brief Manually drops a valid `ReadPageGuard`'s data. If this guard is
 * invalid, this function does nothing.
 *
 * ### Implementation
 *
 * Make sure you don't double free! Also, think **very** **VERY** carefully
 * about what resources you own and the order in which you release those
 * resources. If you get the ordering wrong, you will very likely fail one of
 * the later Gradescope tests. You may also want to take the buffer pool
 * manager's latch in a very specific scenario...
 *
 * TODO(P1): Add implementation.
 */
void ReadPageGuard::Drop() {
  if (!is_valid_ || frame_ == nullptr) {
    return;
  }
  // std::lock_guard<std::mutex> guard(*bpm_latch_);
  {
    std::unique_lock<std::mutex> guard(*bpm_latch_);
    // replacer_->RecordAccess(frame_->frame_id_, page_id_, AccessType::Lookup);
    frame_->pin_count_.fetch_sub(1, std::memory_order_acq_rel);
    // std::memory_order_seq_cst默认顺序，遵循全局唯一顺序
    // std::memory_order_acq_rel针对同一个原子变量的操作，在操作它的线程之间遵循
    // “获取（acquire）- 释放（release）” 顺序：
    // “获取（acquire）”：该操作之后的所有读写，不能重排到该操作之前；
    // “释放（release）”：该操作之前的所有读写，不能重排到该操作之后；这个比seq_cst开销更小，更严谨

    if (frame_->pin_count_ == 0) {
      replacer_->SetEvictable(frame_->frame_id_, true);
    }
  }
  // Releaselock();
  frame_->rwlatch_.unlock_shared();
  is_valid_ = false;
  frame_ = nullptr;
  replacer_ = nullptr;
  disk_scheduler_ = nullptr;
  bpm_latch_ = nullptr;
}

/** @brief The destructor for `ReadPageGuard`. This destructor simply calls
 * `Drop()`. */
ReadPageGuard::~ReadPageGuard() { Drop(); }

/**********************************************************************************************************************/
/**********************************************************************************************************************/
/**********************************************************************************************************************/

/**
 * @brief The only constructor for an RAII `WritePageGuard` that creates a valid
 * guard.
 *
 * Note that only the buffer pool manager is allowed to call this constructor.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The page ID of the page we want to write to.
 * @param frame A shared pointer to the frame that holds the page we want to
 * protect.
 * @param replacer A shared pointer to the buffer pool manager's replacer.
 * @param bpm_latch A shared pointer to the buffer pool manager's latch.
 * @param disk_scheduler A shared pointer to the buffer pool manager's disk
 * scheduler.
 */
WritePageGuard::WritePageGuard(page_id_t page_id,
                               std::shared_ptr<FrameHeader> frame,
                               std::shared_ptr<ArcReplacer> replacer,
                               std::shared_ptr<std::mutex> bpm_latch,
                               std::shared_ptr<DiskScheduler> disk_scheduler)
    : page_id_(page_id), frame_(std::move(frame)),
      replacer_(std::move(replacer)), bpm_latch_(std::move(bpm_latch)),
      disk_scheduler_(std::move(disk_scheduler)) {
  is_valid_ = true;
  // Getlock();
  // frame_->rwlatch_.lock();
}

/**
 * @brief The move constructor for `WritePageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with
 * learning materials online. There are many great resources (including
 * articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard; otherwise, you might run into
 * double free problems! For both objects, you need to update _at least_ 5
 * fields each.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 */
WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  frame_ = std::move(that.frame_);
  replacer_ = std::move(that.replacer_);
  bpm_latch_ = std::move(that.bpm_latch_);
  disk_scheduler_ = std::move(that.disk_scheduler_);
  page_id_ = that.page_id_;
  is_valid_ = that.is_valid_;

  that.is_valid_ = false;
  that.page_id_ = INVALID_PAGE_ID;
  that.frame_ = nullptr;
  that.replacer_ = nullptr;
  that.bpm_latch_ = nullptr;
  that.disk_scheduler_ = nullptr;
}

/**
 * @brief The move assignment operator for `WritePageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with
 * learning materials online. There are many great resources (including
 * articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard; otherwise, you might run into
 * double free problems! For both objects, you need to update _at least_ 5
 * fields each, and for the current object, make sure you release any resources
 * it might be holding on to.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 * @return WritePageGuard& The newly valid `WritePageGuard`.
 */
auto WritePageGuard::operator=(WritePageGuard &&that) noexcept
    -> WritePageGuard & {
  if (this == &that) {
    return *this;
  }
  Drop();
  this->frame_ = std::move(that.frame_);
  this->replacer_ = std::move(that.replacer_);
  this->bpm_latch_ = std::move(that.bpm_latch_);
  this->disk_scheduler_ = std::move(that.disk_scheduler_);
  this->page_id_ = that.page_id_;
  this->is_valid_ = that.is_valid_;
  that.is_valid_ = false;
  that.page_id_ = INVALID_PAGE_ID;
  that.frame_ = nullptr;
  that.replacer_ = nullptr;
  that.bpm_latch_ = nullptr;
  that.disk_scheduler_ = nullptr;
  return *this;
}

/**
 * @brief Gets the page ID of the page this guard is protecting.
 */
auto WritePageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return page_id_;
}

/**
 * @brief Gets a `const` pointer to the page of data this guard is protecting.
 */
auto WritePageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->GetData();
}

/**
 * @brief Gets a mutable pointer to the page of data this guard is protecting.
 */
auto WritePageGuard::GetDataMut() -> char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  frame_->is_dirty_ = true;
  return frame_->GetDataMut();
}

/**
 * @brief Returns whether the page is dirty (modified but not flushed to the
 * disk).
 */
auto WritePageGuard::IsDirty() const -> bool {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->is_dirty_;
}

/**
 * @brief Flushes this page's data safely to disk.
 *
 * TODO(P1): Add implementation.
 */
/*
// 1. try 块：包裹「可能会抛出异常」的代码
try {
   // 这里放你要执行的核心逻辑
   // 这些代码有可能出现异常（比如刷盘失败、空指针调用等）
   risky_operation(); // 示例：可能抛异常的操作
}
// 2. catch 块：捕获 try 块中抛出的异常，并进行处理
catch (...) { // catch(...) 是「兜底捕获」，能捕获所有类型的异常
   // 异常发生时，会执行这里的逻辑
   // 比如：记录日志、优雅退出、保留状态等
   handle_exception(); // 示例：异常处理逻辑
}
*/
// 这里算是异步调用，通常我们调用其他操作，必须等这个操作执行完毕之后才能\执行后面的代码(线程立即阻塞)
// 但是异步调用，不需要马上阻塞线程，仍然能继续执行下面代码
//  只有future.get()（获得调用结果（报错or数据））,我们才会阻塞当前线程（若调用操作未完成会阻塞）
//  所以本质还是延缓了阻塞
void WritePageGuard::Flush() {
  // std::unique_lock<std::mutex> guard(*bpm_latch_);
  if (!is_valid_ || frame_ == nullptr || disk_scheduler_ == nullptr) {
    return;
  }

  DiskRequest disk_request;
  // 构造promise
  std::promise<bool> req_promise;
  // 构造future
  std::future<bool> req_future = req_promise.get_future();
  // 移动构造
  std::vector<DiskRequest> disk_requests;
  // promise-future
  disk_request.callback_ = std::move(req_promise);
  // 避免调用getdatamut使帧变脏
  disk_request.is_write_ = true;
  disk_request.data_ = const_cast<char *>(frame_->GetData());
  disk_request.page_id_ = page_id_;

  disk_requests.push_back(std::move(disk_request));
  disk_scheduler_->Schedule(disk_requests);
  // frame_->pin_count_.fetch_sub(1);
  if (req_future.get()) {
    frame_->is_dirty_ = false;
  }
}

/**
 * @brief Manually drops a valid `WritePageGuard`'s data. If this guard is
 * invalid, this function does nothing.
 *
 * ### Implementation
 *
 * Make sure you don't double free! Also, think **very** **VERY** carefully
 * about what resources you own and the order in which you release those
 * resources. If you get the ordering wrong, you will very likely fail one of
 * the later Gradescope tests. You may also want to take the buffer pool
 * manager's latch in a very specific scenario...
 *
 * TODO(P1): Add implementation.
 */
void WritePageGuard::Drop() {
  if (!is_valid_ || frame_ == nullptr) {
    return;
  }
  frame_->rwlatch_.unlock();
  {
    std::unique_lock<std::mutex> guard(*bpm_latch_);
    // std::lock_guard<std::mutex> guard(*bpm_latch_);
    // replacer_->RecordAccess(frame_->frame_id_, page_id_, AccessType::Lookup);
    frame_->pin_count_.fetch_sub(1, std::memory_order_acq_rel);
    // std::memory_order_seq_cst默认顺序，遵循全局唯一顺序
    // std::memory_order_acq_rel针对同一个原子变量的操作，在操作它的线程之间遵循
    // “获取（acquire）- 释放（release）” 顺序：
    // “获取（acquire）”：该操作之后的所有读写，不能重排到该操作之前；
    // “释放（release）”：该操作之前的所有读写，不能重排到该操作之后；这个比seq_cst开销更小，更严谨
    if (frame_->pin_count_.load() == 0) {
      replacer_->SetEvictable(frame_->frame_id_, true);
    }
  }
  is_valid_ = false;
  page_id_ = INVALID_PAGE_ID;
  frame_ = nullptr;
  replacer_ = nullptr;
  disk_scheduler_ = nullptr;
  bpm_latch_ = nullptr;
}

/** @brief The destructor for `WritePageGuard`. This destructor simply calls
 * `Drop()`. */
WritePageGuard::~WritePageGuard() { Drop(); }

} // namespace bustub
