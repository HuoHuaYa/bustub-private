//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include "buffer/arc_replacer.h"
#include "common/config.h"
#include "common/macros.h"

namespace bustub {

/**
 * @brief The constructor for a `FrameHeader` that initializes all fields to default values.
 *
 * See the documentation for `FrameHeader` in "buffer/buffer_pool_manager.h" for more information.
 *
 * @param frame_id The frame ID / index of the frame we are creating a header for.
 */
FrameHeader::FrameHeader(frame_id_t frame_id) : frame_id_(frame_id), data_(BUSTUB_PAGE_SIZE, 0) { Reset(); }

/**
 * @brief Get a raw const pointer to the frame's data.
 *
 * @return const char* A pointer to immutable data that the frame stores.
 */
auto FrameHeader::GetData() const -> const char * { return data_.data(); }

/**
 * @brief Get a raw mutable pointer to the frame's data.
 *
 * @return char* A pointer to mutable data that the frame stores.
 */
auto FrameHeader::GetDataMut() -> char * { return data_.data(); }

/**
 * @brief Resets a `FrameHeader`'s member fields.
 */
void FrameHeader::Reset() {
  std::fill(data_.begin(), data_.end(), 0);
  pin_count_.store(0);
  is_dirty_ = false;
}

/**
 * @brief Creates a new `BufferPoolManager` instance and initializes all fields.
 *
 * See the documentation for `BufferPoolManager` in "buffer/buffer_pool_manager.h" for more information.
 *
 * ### Implementation
 *
 * We have implemented the constructor for you in a way that makes sense with our reference solution. You are free to
 * change anything you would like here if it doesn't fit with you implementation.
 *
 * Be warned, though! If you stray too far away from our guidance, it will be much harder for us to help you. Our
 * recommendation would be to first implement the buffer pool manager using the stepping stones we have provided.
 *
 * Once you have a fully working solution (all Gradescope test cases pass), then you can try more interesting things!
 *
 * @param num_frames The size of the buffer pool.
 * @param disk_manager The disk manager.
 * @param log_manager The log manager. Please ignore this for P1.
 */
BufferPoolManager::BufferPoolManager(size_t num_frames, DiskManager *disk_manager, LogManager *log_manager)
    : num_frames_(num_frames),
      next_page_id_(0),
      bpm_latch_(std::make_shared<std::mutex>()),
      replacer_(std::make_shared<ArcReplacer>(num_frames)),
      disk_scheduler_(std::make_shared<DiskScheduler>(disk_manager)),
      log_manager_(log_manager) {
  // Not strictly necessary...
  std::scoped_lock latch(*bpm_latch_);

  // Initialize the monotonically increasing counter at 0.
  next_page_id_.store(0);

  // Allocate all of the in-memory frames up front.
  frames_.reserve(num_frames_);

  // The page table should have exactly `num_frames_` slots, corresponding to exactly `num_frames_` frames.
  page_table_.reserve(num_frames_);

  // Initialize all of the frame headers, and fill the free frame list with all possible frame IDs (since all frames are
  // initially free).
  for (size_t i = 0; i < num_frames_; i++) {
    frames_.push_back(std::make_shared<FrameHeader>(i));
    free_frames_.push_back(static_cast<int>(i));
  }
}

/**
 * @brief Destroys the `BufferPoolManager`, freeing up all memory that the buffer pool was using.
 */
BufferPoolManager::~BufferPoolManager() = default;

/**
 * @brief Returns the number of frames that this buffer pool manages.
 */
auto BufferPoolManager::Size() const -> size_t { return num_frames_; }

/**
 * @brief Allocates a new page on disk.
 *
 * ### Implementation
 *
 * You will maintain a thread-safe, monotonically increasing counter in the form of a `std::atomic<page_id_t>`.
 * See the documentation on [atomics](https://en.cppreference.com/w/cpp/atomic/atomic) for more information.
 *
 * TODO(P1): Add implementation.
 *
 * @return The page ID of the newly allocated page.
 */
// 安全的添加一个页
auto BufferPoolManager::NewPage() -> page_id_t {
  std::scoped_lock latch(*bpm_latch_);
  frame_id_t frame_id = INVALID_FRAME_ID;
  if (!free_frames_.empty()) {
    frame_id = free_frames_.front();
    free_frames_.pop_front();
  } else {
    auto t_f = replacer_->Evict();
    if (!t_f) {
      return INVALID_PAGE_ID;
    }
    frame_id = t_f.value();
    if (frame_table_.find(frame_id) != frame_talbe_.end()) frame_table_.erase(frame_id);
  }
  frame_table_[frame_id] = new_page_id;
  page_table_[new_page_id] = frame_id;
  page_id_t new_page_id = next_page_id_.fetch_add(1);
  frames_[frame_id]->Reset();
  frames_[frame_id]->pin_count_.fetch_add(1);
  frames_[frame_id]->is_dirty_ = false;
  return new_page_id;
}

/**
 * @brief Removes a page from the database, both on disk and in memory.
 *
 * If the page is pinned in the buffer pool, this function does nothing and returns `false`. Otherwise, this function
 * removes the page from both disk and memory (if it is still in the buffer pool), returning `true`.
 *
 * ### Implementation
 *
 * Think about all of the places that a page or a page's metadata could be, and use that to guide you on implementing
 * this function. You will probably want to implement this function _after_ you have implemented `CheckedReadPage` and
 * `CheckedWritePage`.
 *
 * You should call `DeallocatePage` in the disk scheduler to make the space available for new pages.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The page ID of the page we want to delete.
 * @return `false` if the page exists but could not be deleted, `true` if the page didn't exist or deletion succeeded.
 */
/**
 * @brief 从数据库中删除一个页面，同时清理该页面在**磁盘**和**内存**中的数据（双重清理）。
 *
 * 若该页面在缓冲池中处于「被固定状态」（pin计数>0，无法被驱逐），此函数将不执行任何操作并返回 `false`；
 * 反之，函数会删除该页面在磁盘上的存储，同时清理其在缓冲池内存中的数据（若该页面仍存在于缓冲池中），执行成功后返回
 * `true`。
 *
 * ### 实现说明
 * 思考一个页面或页面元数据可能存在的所有位置，以此指导你实现该函数。你最好在实现完 `CheckedReadPage`
 * 和 `CheckedWritePage` 两个函数后，再着手实现本函数。
 *
 * 你需要调用磁盘调度器（DiskScheduler）中的 `DeallocatePage` 函数，释放该页面占用的磁盘空间，以便为新页面分配存储。
 *
 * TODO(P1)：补充实现代码。
 *
 * @param page_id 想要删除的页面ID。
 * @return 若页面存在但无法被删除，返回 `false`；若页面不存在（无需删除）或删除操作成功，返回 `true`。
 */
// 清理该页面在磁盘，内存中的所有数据，如果无法清除就返回false
auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  // 先从bufferpoolmanager的成员来验证pageid是否存在,是否能够合法删除
  std::scoped_lock latch(*bpm_latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }
  if (frames_[page_table_[page_id]]->pin_count_ > 0) {
    return false;
  }
  frame_id_t target_frame_id = page_table_[page_id];
  if (target_frame_id == INVALID_FRAME_ID) {
    page_table_.erase(page_id);
    return true;
  }
  // 映射
  frame_table_.erase(target_frame_id);
  page_table_.erase(page_id);
  // frameheader
  frames_[target_frame_id]->Reset();
  // free
  free_frames_.push_back(target_frame_id);
  // scheduler
  disk_scheduler_->DeallocatePage(page_id);
  // replace
  replace_->Remove(target_frame_id);
  return true;
}

/**
 * @brief Acquires an optional write-locked guard over a page of data. The user can specify an `AccessType` if needed.
 *
 * If it is not possible to bring the page of data into memory, this function will return a `std::nullopt`.
 *
 * Page data can _only_ be accessed via page guards. Users of this `BufferPoolManager` are expected to acquire either a
 * `ReadPageGuard` or a `WritePageGuard` depending on the mode in which they would like to access the data, which
 * ensures that any access of data is thread-safe.
 *
 * There can only be 1 `WritePageGuard` reading/writing a page at a time. This allows data access to be both immutable
 * and mutable, meaning the thread that owns the `WritePageGuard` is allowed to manipulate the page's data however they
 * want. If a user wants to have multiple threads reading the page at the same time, they must acquire a `ReadPageGuard`
 * with `CheckedReadPage` instead.
 *
 * ### Implementation
 *
 * There are three main cases that you will have to implement. The first two are relatively simple: one is when there is
 * plenty of available memory, and the other is when we don't actually need to perform any additional I/O. Think about
 * what exactly these two cases entail.
 *
 * The third case is the trickiest, and it is when we do not have any _easily_ available memory at our disposal. The
 * buffer pool is tasked with finding memory that it can use to bring in a page of memory, using the replacement
 * algorithm you implemented previously to find candidate frames for eviction.
 *
 * Once the buffer pool has identified a frame for eviction, several I/O operations may be necessary to bring in the
 * page of data we want into the frame.
 *
 * There is likely going to be a lot of shared code with `CheckedReadPage`, so you may find creating helper functions
 * useful.
 *
 * These two functions are the crux of this project, so we won't give you more hints than this. Good luck!
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The ID of the page we want to write to.
 * @param access_type The type of page access.
 * @return std::optional<WritePageGuard> An optional latch guard where if there are no more free frames (out of memory)
 * returns `std::nullopt`; otherwise, returns a `WritePageGuard` ensuring exclusive and mutable access to a page's data.
 */
/**
 * @brief
 * 获取一个可选的、针对指定数据页面的写锁守卫（WritePageGuard）。若有需要，用户可以指定页面访问类型（AccessType）。
 *
 *  若无法将目标数据页面加载到内存中（如缓冲池无可用帧、内存不足），此函数将返回`std::nullopt`。
 *
 *  页面数据**仅允许**通过页面守卫（Page Guard，即ReadPageGuard或WritePageGuard）进行访问。
 *  本缓冲池管理器（BufferPoolManager）的使用者，需要根据自身对页面数据的访问模式，获取对应的读页面守卫（ReadPageGuard）或写页面守卫（WritePageGuard），
 *  这一设计能确保所有页面数据的访问操作都是线程安全的。
 *
 *  同一个页面在同一时间，只能存在一个写页面守卫（WritePageGuard）对其进行读写操作。
 *  这一机制既支持数据的不可变访问（读操作），也支持可变访问（写操作）：持有WritePageGuard的线程，可以不受限制地修改该页面的数据。
 *  若用户需要让多个线程同时读取某个页面，必须通过`CheckedReadPage`函数获取读页面守卫（ReadPageGuard），而非本函数。
 *
 * ### 实现说明
 *
 *  你需要实现三种核心场景，其中前两种场景相对简单：
 *  第一种场景是缓冲池有充足的可用内存（存在空闲帧）；
 *  第二种场景是我们实际上无需执行任何额外的I/O操作（即可获取目标页面）。
 *  请先思考这两种场景具体需要包含哪些操作逻辑。
 *
 *  第三种场景最为复杂：当缓冲池没有**易获取的可用内存**（即无空闲帧）时。
 *  此时缓冲池需要通过你之前实现的替换算法，筛选出可被驱逐的候选帧（用于存放即将加载的目标页面），以此获取可用内存。
 *
 *  当缓冲池确定了待驱逐的帧后，可能需要执行多项I/O操作，才能将目标数据页面加载到该帧中。
 *
 *  本函数与`CheckedReadPage`函数存在大量共享代码，你可以提取辅助函数来复用这些逻辑，提升代码整洁度。
 *
 *  本函数与`CheckedReadPage`是整个项目的核心所在，因此我们不会提供更多提示，祝你好运！
 *
 * TODO(P1)：补充实现逻辑。
 *
 * @param page_id  待写入操作的目标页面ID。
 * @param access_type  页面访问类型（用于标记访问行为，如Lookup/Modify等）。
 * @return  若缓冲池无可用帧（内存不足），返回`std::nullopt`；
 *          否则，返回一个写页面守卫（WritePageGuard），该守卫保证对目标页面数据的独占式、可修改访问。
 */
// 获取一个可选的，针对指定页面的writepageguard(buffer...manager只会与guard交互)
// 构造一个wpg，我需要pageid，replacer，diskscheduler，frameheader，锁
auto BufferPoolManager::CheckedWritePage(page_id_t page_id, AccessType access_type) -> std::optional<WritePageGuard> {
  frame_id_t target_frame_id = -1;
  {
    std::lock_guard<std::mutex> guard(*bpm_latch_);
    // 场景2，命中缓存
    auto page_id_pointer = page_table_.find(page_id);
    if (page_id_pointer != page_table_.end()) {
      target_frame_id = page_id_pointer->second;
      // frameheader
      std::shared_ptr<FrameHeader> frame_header = frames_[target_frame_id];
      frame_header->pin_count_.fetch_add(1);
      frame_header->is_dirty_ = true;
      // replacer
      replacer_->RecordAccess(target_frame_id, page_id, access_type);
      replacer_->SetEvictable(target_frame_id, false);

      return WritePageGuard(page_id, frame_header, replacer_, bpm_latch_, disk_scheduler_);
    }
    if (free_frames_.size()) {
      // 场景1：存在空闲帧
      target_frame_id = free_frames_.front();
      free_frames_.pop_front();
      // 映射
      page_table_[page_id] = target_frame_id;
      frame_table_[target_frame_id] = page_id;
      // frameheader
      std::shared_ptr<FrameHeader> frame_header = frames_[target_frame_id];
      frame_header->Reset();
      frame_header->pin_count_.fetch_add(1);
      frame_header->frame_id_ = target_frame_id;
      frame_header->is_dirty_ = true;
      // replacer
      replacer_->RecordAccess(target_frame_id, page_id, access_type);
      replacer_->SetEvictable(target_frame_id, false);
      return WritePageGuard(page_id, frame_header, replacer_, bpm_latch_, disk_scheduler_);
    }
    auto evicted_frame_pointer = replacer_->Evict();
    if (evicted_frame_pointer == std::nullopt) {
      return std::nullopt;
    }
    target_frame_id = *evicted_frame_pointer;
    // frameheader
    std::shared_ptr<FrameHeader> frame_header = frames_[target_frame_id];

    if (frame_header.is_dirty_) {
      WritePageGuard dirty_wpg = std::move(
          WritePageGuard(frame_table_[target_frame_id], frame_header, replacer_, bpm_latch_, disk_scheduler_));
      dirty_wpg.Flush();
      if (frame_header->is_dirty_) {
        return std::nullopt;
      }
    }
    frame_header->Reset();
    frame_header->pin_count_.fetch_add(1);
    frame_header->frame_id_ = target_frame_id;
    frame_header->is_dirty_ = true;
    // 映射
    if (frame_table_.find(target_frame_id) != frame_table_.end()) {
      page_table_.erase(frame_table_[target_frame_id]);
      frame_table_.erase(target_frame_id);
    }
    page_table_[page_id] = target_frame_id;
    frame_table_[target_frame_id] = page_id;
    // replacer
    replacer_->RecordAccess(target_frame_id, page_id, access_type);
    replacer_->SetEvictable(target_frame_id, false);
    return WritePageGuard(page_id, frame_header, replacer_, bpm_latch_, disk_scheduler_);
  }
}

/**
 * @brief Acquires an optional read-locked guard over a page of data. The user can specify an `AccessType` if needed.
 *
 * If it is not possible to bring the page of data into memory, this function will return a `std::nullopt`.
 *
 * Page data can _only_ be accessed via page guards. Users of this `BufferPoolManager` are expected to acquire either a
 * `ReadPageGuard` or a `WritePageGuard` depending on the mode in which they would like to access the data, which
 * ensures that any access of data is thread-safe.
 *
 * There can be any number of `ReadPageGuard`s reading the same page of data at a time across different threads.
 * However, all data access must be immutable. If a user wants to mutate the page's data, they must acquire a
 * `WritePageGuard` with `CheckedWritePage` instead.
 *
 * ### Implementation
 *
 * See the implementation details of `CheckedWritePage`.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return std::optional<ReadPageGuard> An optional latch guard where if there are no more free frames (out of memory)
 * returns `std::nullopt`; otherwise, returns a `ReadPageGuard` ensuring shared and read-only access to a page's data.
 */
/**
 * @brief  获取一个针对某页数据的可选读锁守卫。若有需要，用户可指定一种`AccessType`（访问类型）。
 *
 *  若无法将目标数据页加载到内存中，该函数将返回`std::nullopt`（空值）。
 *
 *  数据页的内容**仅能**通过页面守卫（page guard）进行访问。该`BufferPoolManager`（缓冲池管理器）的使用者
 *  需根据自身所需的访问模式，获取`ReadPageGuard`（读页面守卫）或`WritePageGuard`（写页面守卫），
 *  这一机制确保了所有数据访问操作都是线程安全的。
 *
 *  在不同线程中，同一数据页可以同时存在任意数量的`ReadPageGuard`进行读取操作。
 *  但所有此类数据访问都必须是不可变的（即只读操作，不允许修改数据）。如果用户想要修改该页数据，
 *  则必须通过`CheckedWritePage`函数获取`WritePageGuard`（写页面守卫）。
 *
 * ### 实现说明
 *
 *  请参考`CheckedWritePage`函数的实现细节。
 *
 * TODO(P1)：补充该函数的实现代码（P1为任务优先级/阶段标识）。
 *
 * @param  page_id  待读取数据页的唯一标识ID。
 * @param  access_type  页面访问类型（用于指定数据页的访问属性）。
 * @return  std::optional<ReadPageGuard>  一个可选的锁守卫对象：当缓冲池中没有空闲内存帧（内存不足）
 *  导致数据页无法加载时，返回`std::nullopt`；否则返回一个`ReadPageGuard`实例，该实例用于确保
 *  调用者对目标数据页拥有**共享、只读**的访问权限。
 */
auto BufferPoolManager::CheckedReadPage(page_id_t page_id, AccessType access_type) -> std::optional<ReadPageGuard> {
  frame_id_t target_frame_id = -1;
  {
    std::lock_guard<std::mutex> guard(*bpm_latch_);
    // 场景2，命中缓存
    auto page_id_pointer = page_table_.find(page_id);
    if (page_id_pointer != page_table_.end()) {
      target_frame_id = page_id_pointer->second;
      // frameheader
      std::shared_ptr<FrameHeader> frame_header = frames_[target_frame_id];
      frame_header->pin_count_.fetch_add(1);
      // frame_header.is_dirty_ = true;
      // replacer
      replacer_->RecordAccess(target_frame_id, page_id, access_type);
      replacer_->SetEvictable(target_frame_id, false);

      return ReadPageGuard(page_id, frame_header, replacer_, bpm_latch_, disk_scheduler_);
    }
    if (!free_frames_.empty()) {
      // 场景1：存在空闲帧
      target_frame_id = free_frames_.front();
      free_frames_.pop_front();
      // 映射
      page_table_[page_id] = target_frame_id;
      frame_table_[target_frame_id] = page_id;
      // frameheader
      std::shared_ptr<FrameHeader> frame_header = frames_[target_frame_id];
      frame_header->Reset();
      frame_header->pin_count_.fetch_add(1);
      frame_header->frame_id_ = target_frame_id;
      // frame_header.is_dirty_ = true;
      // replacer
      replacer_->RecordAccess(target_frame_id, page_id, access_type);
      replacer_->SetEvictable(target_frame_id, false);
      return ReadPageGuard(page_id, frame_header, replacer_, bpm_latch_, disk_scheduler_);
    }
    auto evicted_frame_pointer = replacer_->Evict();
    if (evicted_frame_pointer == std::nullopt) {
      return std::nullopt;
    }
    target_frame_id = *evicted_frame_pointer;
    // frameheader
    std::shared_ptr<FrameHeader> frame_header = frames_[target_frame_id];

    if (frame_header->is_dirty_) {
      WritePageGuard dirty_wpg = std::move(
          WritePageGuard(frame_table_[target_frame_id], frame_header, replacer_, bpm_latch_, disk_scheduler_));
      dirty_wpg.Flush();
      if (frame_header->is_dirty_) {
        return std::nullopt;
      }
    }
    frame_header->Reset();
    frame_header->pin_count_.fetch_add(1);
    frame_header->frame_id_ = target_frame_id;
    // frame_header.is_dirty_ = true;
    // 映射
    if (frame_table_.find(target_frame_id) != frame_table_.end()) {
      page_table_.erase(frame_table_[target_frame_id]);
      frame_table_.erase(target_frame_id);
    }
    page_table_[page_id] = target_frame_id;
    frame_table_[target_frame_id] = page_id;
    // replacer
    replacer_->RecordAccess(target_frame_id, page_id, access_type);
    replacer_->SetEvictable(target_frame_id, false);
    return ReadPageGuard(page_id, frame_header, replacer_, bpm_latch_, disk_scheduler_);
  }
}

/**
 * @brief A wrapper around `CheckedWritePage` that unwraps the inner value if it exists.
 *
 * If `CheckedWritePage` returns a `std::nullopt`, **this function aborts the entire process.**
 *
 * This function should **only** be used for testing and ergonomic's sake. If it is at all possible that the buffer pool
 * manager might run out of memory, then use `CheckedPageWrite` to allow you to handle that case.
 *
 * See the documentation for `CheckedPageWrite` for more information about implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return WritePageGuard A page guard ensuring exclusive and mutable access to a page's data.
 */
/**
 * @brief 这是一个对 `CheckedWritePage` 函数的包装器，若 `CheckedWritePage` 返回有效结果，则该函数会解包出内部的
 * `WritePageGuard` 对象并返回。
 *
 * 若 `CheckedWritePage` 返回 `std::nullopt`（表示获取可写页面守卫失败），**此函数会直接终止整个进程**。
 *
 * 该函数**仅应**用于测试场景或为了提升使用便捷性的场景。如果缓冲池管理器存在任何内存耗尽的可能性（可能导致获取守卫失败），
 * 则应使用 `CheckedWritePage` 函数，以便你自行处理获取失败的场景。
 *
 * 有关实现细节的更多信息，请参阅 `CheckedWritePage` 函数的文档说明。
 *
 * @param page_id 我们想要**写入**（注释笔误：原文写read，实际为write）的页面ID。
 * @param access_type 页面访问类型（读/写等）。
 * @return WritePageGuard 一个页面守卫对象，确保调用者对目标页面的数据拥有**独占且可修改**的访问权限。
 */
// 测试checkwritepageguard
auto BufferPoolManager::WritePage(page_id_t page_id, AccessType access_type) -> WritePageGuard {
  auto guard_opt = CheckedWritePage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedWritePage` failed to bring in page {}\n", page_id);
    std::abort();
  }

  return std::move(guard_opt).value();
}

/**
 * @brief A wrapper around `CheckedReadPage` that unwraps the inner value if it exists.
 *
 * If `CheckedReadPage` returns a `std::nullopt`, **this function aborts the entire process.**
 *
 * This function should **only** be used for testing and ergonomic's sake. If it is at all possible that the buffer pool
 * manager might run out of memory, then use `CheckedPageWrite` to allow you to handle that case.
 *
 * See the documentation for `CheckedPageRead` for more information about implementation.
 *
 * @param page_id The ID of the page we want to read.
 * @param access_type The type of page access.
 * @return ReadPageGuard A page guard ensuring shared and read-only access to a page's data.
 */
auto BufferPoolManager::ReadPage(page_id_t page_id, AccessType access_type) -> ReadPageGuard {
  auto guard_opt = CheckedReadPage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedReadPage` failed to bring in page {}\n", page_id);
    std::abort();
  }

  return std::move(guard_opt).value();
}

/**
 * @brief Flushes a page's data out to disk unsafely.
 *
 * This function will write out a page's data to disk if it has been modified. If the given page is not in memory, this
 * function will return `false`.
 *
 * You should not take a lock on the page in this function.
 * This means that you should carefully consider when to toggle the `is_dirty_` bit.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage` and
 * `CheckedWritePage`, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 *
 * @param page_id The page ID of the page to be flushed.
 * @return `false` if the page could not be found in the page table; otherwise, `true`.
 */
/**
 * @brief 不安全地将目标页面的数据刷写到（同步到）磁盘中。
 *
 * 若该页面被修改过（标记为脏页），此函数会将其内存数据写入磁盘；若给定的页面不存在于内存（缓冲池）中，此函数会返回
 * `false`。
 *
 * 你**不应该**在该函数中对页面加锁（指页面级别的读写锁，如 PageGuard 对应的锁）。
 * 这意味着你需要谨慎考虑何时切换（置位/复位）页面的 `is_dirty_` 标志位（脏页标记）。
 *
 * ### 实现说明
 * 你最好在完成 `CheckedReadPage` 和 `CheckedWritePage` 两个函数的实现后，再着手实现本函数，
 * 因为这会让你更容易理解该函数的实现逻辑。
 *
 * TODO(P1)：补充实现代码。
 *
 * @param page_id 需要被刷写（同步）到磁盘的页面ID。
 * @return 若在页面表（`page_table_`）中找不到该页面（即页面不在内存中），返回 `false`；其他情况返回
 * `true`（无论是否实际写入磁盘）。
 */
auto BufferPoolManager::FlushPageUnsafe(page_id_t page_id) -> bool {
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  std::shared_ptr<FrameHeader> frame_header = frames_[frame_id];
  if (frame_header == nullptr || frame_header->is_dirty_ == false) {
    return true;
  }
  std::vector<DiskRequest> requests;
  requests.emplace_back({true, frame_header->GetDataMut(), page_id});
  disk_scheduler_->Schedule(requests);
  return true;
}

/**
 * @brief Flushes a page's data out to disk safely.
 *
 * This function will write out a page's data to disk if it has been modified. If the given page is not in memory, this
 * function will return `false`.
 *
 * You should take a lock on the page in this function to ensure that a consistent state is flushed to disk.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage`,
 * `CheckedWritePage`, and `Flush` in the page guards, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 *
 * @param page_id The page ID of the page to be flushed.
 * @return `false` if the page could not be found in the page table; otherwise, `true`.
 */
/**
 * @brief 安全地将目标页面的数据刷写到（同步到）磁盘中。
 *
 * 若该页面被修改过（即标记为脏页），此函数会将其内存数据写入磁盘；若给定的页面不存在于内存（缓冲池）中，此函数会返回
 * `false`。
 *
 * 你**应该**在该函数中对页面加锁，以确保将一致性的页面状态刷写到磁盘中（避免刷写过程中页面数据被篡改）。
 *
 * ### 实现说明
 * 你最好在完成 `CheckedReadPage`、`CheckedWritePage` 这两个函数，以及页面守卫（PageGuard）中的 `Flush` 方法实现后，
 * 再着手实现本函数，因为这会让你更容易理解该函数的实现逻辑。
 *
 * TODO(P1)：补充实现代码。
 *
 * @param page_id 需要被刷写（同步）到磁盘的页面ID。
 * @return 若在页面表（`page_table_`）中找不到该页面（即页面不在内存中），返回 `false`；其他所有情况均返回
 * `true`（无论是否实际执行磁盘刷写）。
 */
auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  std::shared_ptr<FrameHeader> frame_header = frames_[frame_id];
  if (frame_header == nullptr || frame_header->is_dirty_ == false || frame_id == INVALID_FRAME_ID) {
    return true;
  }
  std::optional<WritePageGuard> guard = std::move(CheckedWritePage(page_id));
  if (guard == std::nullopt) {
    return true;
  }
  guard->Flush();
  return true;
}

/**
 * @brief Flushes all page data that is in memory to disk unsafely.
 *
 * You should not take locks on the pages in this function.
 * This means that you should carefully consider when to toggle the `is_dirty_` bit.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage`,
 * `CheckedWritePage`, and `FlushPage`, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 */
/**
 * @brief 不安全地将内存中所有页面的数据批量刷写到（同步到）磁盘中。
 *
 * 你**不应该**在该函数中对任何页面加锁（指页面级别的读写锁，如 PageGuard 对应的锁）。
 * 这意味着你需要谨慎考虑何时切换（置位/复位）页面的 `is_dirty_` 标志位（脏页标记）。
 *
 * ### 实现说明
 * 你最好在完成 `CheckedReadPage`、`CheckedWritePage` 以及 `FlushPage` 这三个函数的实现后，
 * 再着手实现本函数，因为这会让你更容易理解该函数的实现逻辑。
 *
 * TODO(P1)：补充实现代码。
 */
void BufferPoolManager::FlushAllPagesUnsafe() {
  for (auto [page_id, frame_id] : page_table_) {
    FlushPageUnsafe(page_id);
  }
}

/**
 * @brief Flushes all page data that is in memory to disk safely.
 *
 * You should take locks on the pages in this function to ensure that a consistent state is flushed to disk.
 *
 * ### Implementation
 *
 * You should probably leave implementing this function until after you have completed `CheckedReadPage`,
 * `CheckedWritePage`, and `FlushPage`, as it will likely be much easier to understand what to do.
 *
 * TODO(P1): Add implementation
 */
void BufferPoolManager::FlushAllPages() {
  for (auto [page_id, frame_id] : page_table_) {
    FlushPage(page_id);
  }
}

/**
 * @brief Retrieves the pin count of a page. If the page does not exist in memory, return `std::nullopt`.
 *
 * This function is thread safe. Callers may invoke this function in a multi-threaded environment where multiple threads
 * access the same page.
 *
 * This function is intended for testing purposes. If this function is implemented incorrectly, it will definitely cause
 * problems with the test suite and autograder.
 *
 * # Implementation
 *
 * We will use this function to test if your buffer pool manager is managing pin counts correctly. Since the
 * `pin_count_` field in `FrameHeader` is an atomic type, you do not need to take the latch on the frame that holds the
 * page we want to look at. Instead, you can simply use an atomic `load` to safely load the value stored. You will still
 * need to take the buffer pool latch, however.
 *
 * Again, if you are unfamiliar with atomic types, see the official C++ docs
 * [here](https://en.cppreference.com/w/cpp/atomic/atomic).
 *
 * TODO(P1): Add implementation
 *
 * @param page_id The page ID of the page we want to get the pin count of.
 * @return std::optional<size_t> The pin count if the page exists; otherwise, `std::nullopt`.
 */
/**
 * @brief  获取某个页面的固定计数（Pin Count）。若该页面不存在于内存中，返回 `std::nullopt`。
 *
 *  该函数是线程安全的。调用者可在多线程环境中调用此函数（即使多个线程同时访问同一个页面）。
 *
 *  此函数仅用于测试目的。若该函数实现错误，必然会导致测试套件（Test Suite）和自动评分系统（Autograder）执行失败。
 *
 * # 实现要求
 *  我们会通过此函数测试你的缓冲池管理器是否正确管理页面的固定计数。由于 `FrameHeader` 中的 `pin_count_`
 * 字段是原子类型（atomic type）， 你无需获取目标页面所在帧（frame）上的锁（latch）。相反，你可以直接使用原子类型的
 * `load` 方法，安全地加载（读取）它存储的值。 不过，你仍然需要获取缓冲池的锁（buffer pool latch）。
 *
 *  如果你对原子类型不熟悉，可以参考 C++ 官方文档：
 *  [https://en.cppreference.com/w/cpp/atomic/atomic](https://en.cppreference.com/w/cpp/atomic/atomic)
 *
 * TODO(P1)：补充实现代码。
 *
 * @param page_id  我们需要获取其固定计数的目标页面ID。
 * @return  std::optional<size_t>  若页面存在于内存中，返回该页面的固定计数；若页面不存在，返回 `std::nullopt`。
 */

auto BufferPoolManager::GetPinCount(page_id_t page_id) -> std::optional<size_t> {
  
  std::shared_lock<std::shared_mutex> lock_(*bpm_latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return std::nullopt;
  }
  frame_id_t frame_id = page_table_[page_id];
  if(frame_id == INVALID_FRAME_ID){
    return std::nullopt;
  }
  std::shared_ptr<FrameHeader> frame_header = frames_[frame_id];
  if(frame_header == nullptr){
    return std::nullopt;
  }
  return frame_header->pin_count_.load();
}

}  // namespace bustub
