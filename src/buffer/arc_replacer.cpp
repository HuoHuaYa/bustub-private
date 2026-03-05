// :bustub-keep-private:
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// arc_replacer.cpp
//
// Identification: src/buffer/arc_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/arc_replacer.h"
#include <algorithm>
#include <optional>
#include "common/config.h"

namespace bustub {

/*
 *
 * TODO(P1): Add implementation
 *
 * @brief a new ArcReplacer, with lists initialized to be empty and target size
 * to 0
 * @param num_frames the maximum number of frames the ArcReplacer will be
 * required to cache
 */
ArcReplacer::ArcReplacer(size_t num_frames) : replacer_size_(num_frames) {
  mru_.clear();
  mfu_.clear();
  mru_ghost_.clear();
  mfu_ghost_.clear();
  alive_map_.clear();
  ghost_map_.clear();
  alive_map_iter_.clear();
  ghost_map_iter_.clear();
  replacer_size_ = num_frames;
  mru_target_size_ = 0;
}
void ArcReplacer::LFerase(std::list<frame_id_t> &List, frame_id_t fid) {
  if (alive_map_iter_.find(fid) == alive_map_iter_.end()) {
    return;
  }
  auto idx = alive_map_iter_[fid];
  if (idx != List.end()) {
    List.erase(idx);
    alive_map_iter_.erase(fid);
  }
}
void ArcReplacer::LPerase(std::list<page_id_t> &List, page_id_t pid) {
  if (ghost_map_iter_.find(pid) == ghost_map_iter_.end()) {
    return;
  }
  auto idx = ghost_map_iter_[pid];
  if (idx != List.end()) {
    List.erase(idx);
    ghost_map_iter_.erase(pid);
    // ghost_map_.erase(pid);
  }
}
/**
 * TODO(P1): Add implementation
 *
 * @brief Performs the Replace operation as described by the writeup
 * that evicts from either mfu_ or mru_ into its corresponding ghost list
 * according to balancing policy.
 *
 * If you wish to refer to the original ARC paper, please note that there are
 * two changes in our implementation:
 * 1. When the size of mru_ equals the target size, we don't check
 * the last access as the paper did when deciding which list to evict from.
 * This is fine since the original decision is stated to be arbitrary.
 * 2. Entries that are not evictable are skipped. If all entries from the
 * desired side (mru_ / mfu_) are pinned, we instead try victimize the other
 * side (mfu_ / mru_), and move it to its corresponding ghost list (mfu_ghost_ /
 * mru_ghost_).
 *
 * @return frame id of the evicted frame, or std::nullopt if cannot evict
 */
/**
 * TODO(P1): 添加实现
 *
 * @brief 根据写入描述执行替换操作，该操作根据平衡策略从 mfu_ 或 mru_
 * 中淘汰页面到对应的幽灵列表。
 *
 * 如果您参考原始ARC论文，请注意我们的实现中有两个改动：
 * 1. 当 mru_
 * 的大小等于目标大小时，我们不像论文中那样检查最后访问时间来决定从哪个列表淘汰。
 *    这没有问题，因为原论文中说明该决定是随意的。
 * 2. 不可淘汰的条目会被跳过。如果目标侧（mru_ /
 * mfu_）的所有条目都被固定，我们则尝试从另一侧 （mfu_ /
 * mru_）中选择受害者，并将其移动到对应的幽灵列表（mfu_ghost_ / mru_ghost_）。
 *
 * @return 淘汰的帧的帧ID，若无法淘汰则返回 std::nullopt
 */
auto ArcReplacer::Evict() -> std::optional<frame_id_t> {
  std::lock_guard<std::mutex> lock(latch_);
  // optional<xxx>表示要么返回 xxx,要么返回空值
  if (curr_size_ == 0) {
    return std::nullopt;
  }
  // 没有可驱逐页面，返回空值
  // 被驱逐对象的增删（删除当前，加入新链表，维护map）
  auto tryevict = [&](std::list<frame_id_t> &Tlist, std::list<page_id_t> &ghost,
                      ArcStatus arcstatus) -> std::optional<frame_id_t> {
    if (Tlist.end() == Tlist.begin()) {
      return std::nullopt;
    }
    for (auto idx = Tlist.rbegin(); idx != Tlist.rend(); idx++) {
      frame_id_t fid = *idx;
      if (alive_map_.find(fid) == alive_map_.end()) {
        continue;
      }
      if (alive_map_[fid]->evictable_) {
        page_id_t pid = alive_map_[fid]->page_id_;
        curr_size_--;
        Tlist.erase(std::next(idx).base());  // 不能直接删除反向迭代器
        // 但是base改正向，会导致往前挪一位，所以现在挪回来
        ghost.push_front(pid);
        ghost_map_[pid] = alive_map_[fid];
        ghost_map_[pid]->arc_status_ = arcstatus;
        ghost_map_iter_[pid] = ghost.begin();
        alive_map_.erase(fid);
        alive_map_iter_.erase(fid);
        if (ghost.size() > replacer_size_) {
          page_id_t dead_pid = ghost.back();
          ghost.pop_back();
          ghost_map_.erase(dead_pid);
          ghost_map_iter_.erase(dead_pid);
        }
        return fid;
      }
    }
    return std::nullopt;
  };

  if (mru_.size() >= mru_target_size_) {
    auto ret = tryevict(mru_, mru_ghost_, ArcStatus::MRU_GHOST);
    if (ret == std::nullopt) {
      auto rret = tryevict(mfu_, mfu_ghost_, ArcStatus::MFU_GHOST);
      if (rret != std::nullopt) {
        return rret;
      }
    } else {
      return ret;
    }
  } else {
    auto ret = tryevict(mfu_, mfu_ghost_, ArcStatus::MFU_GHOST);
    if (ret == std::nullopt) {
      auto rret = tryevict(mru_, mru_ghost_, ArcStatus::MRU_GHOST);
      if (rret != std::nullopt) {
        return rret;
      }
    } else {
      return ret;
    }
  }
  return std::nullopt;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Record access to a frame, adjusting ARC bookkeeping accordingly
 * by bring the accessed page to the front of mfu_ if it exists in any of the
 * lists or the front of mru_ if it does not.
 *
 * Performs the operations EXCEPT REPLACE described in original paper, which is
 * handled by `Evict()`.
 *
 * Consider the following four cases, handle accordingly:
 * 1. Access hits mru_ or mfu_
 * 2/3. Access hits mru_ghost_ / mfu_ghost_
 * 4. Access misses all the lists
 *
 * This routine performs all changes to the four lists as preperation
 * for `Evict()` to simply find and evict a victim into ghost lists.
 *
 * Note that frame_id is used as identifier for alive pages and
 * page_id is used as identifier for the ghost pages, since page_id is
 * the unique identifier to the page after it's dead.
 * Using page_id for alive pages should be the same since it's one to one
 * mapping, but using frame_id is slightly more intuitive.
 *
 * @param frame_id id of frame that received a new access.
 * @param page_id id of page that is mapped to the frame.
 * @param access_type type of access that was received. This parameter is only
 * needed for leaderboard tests.
 */
void ArcReplacer::RecordAccess(frame_id_t frame_id, page_id_t page_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> lock(latch_);
  // 更新，增删自己（链表，status），更新p ,map status
  if (alive_map_.find(frame_id) == alive_map_.end() && ghost_map_.find(page_id) == ghost_map_.end()) {
    // 从未别命中，加入mru
    if (mru_.size() + mru_ghost_.size() == replacer_size_) {
      if (!mru_ghost_.empty()) {
        page_id_t pid = mru_ghost_.back();
        ghost_map_.erase(pid);
        ghost_map_iter_.erase(pid);
        mru_ghost_.pop_back();
      }
    } else {
      if (mru_.size() + mru_ghost_.size() + mfu_ghost_.size() == 2 * replacer_size_) {
        if (!mfu_ghost_.empty()) {
          page_id_t pid = mfu_ghost_.back();
          ghost_map_.erase(pid);
          ghost_map_iter_.erase(pid);
          mfu_ghost_.pop_back();
        }
      }
    }
    alive_map_[frame_id] = std::make_shared<FrameStatus>(page_id, frame_id, false, ArcStatus::MRU);
    mru_.push_front(frame_id);
    alive_map_iter_[frame_id] = mru_.begin();
  } else if (alive_map_.find(frame_id) != alive_map_.end() && alive_map_[frame_id]->arc_status_ == ArcStatus::MRU) {
    // 在mru命中
    alive_map_[frame_id]->arc_status_ = ArcStatus::MFU;
    // auto idx = std::find(mru_.begin(), mru_.end(), frame_id);
    // mru_.erase(idx);
    LFerase(mru_, frame_id);
    mfu_.push_front(frame_id);
    alive_map_iter_[frame_id] = mfu_.begin();
  } else if (ghost_map_.find(page_id) != ghost_map_.end() && ghost_map_[page_id]->arc_status_ == ArcStatus::MRU_GHOST) {
    // mru_ghost 命中
    if (mru_ghost_.size() >= mfu_ghost_.size()) {
      mru_target_size_++;
    } else if (!mru_ghost_.empty()) {
      mru_target_size_ += mfu_ghost_.size() / mru_ghost_.size();
    }
    // alive
    mru_target_size_ = std::min(mru_target_size_, replacer_size_);
    alive_map_[frame_id] = std::make_shared<FrameStatus>(page_id, frame_id, false, ArcStatus::MFU);
    mfu_.push_front(frame_id);
    alive_map_iter_[frame_id] = mfu_.begin();
    // ghost
    LPerase(mru_ghost_, page_id);
    ghost_map_.erase(page_id);
    // ghost_map_iter_.erase(page_id);
    // auto idx = std::find(mru_ghost_.begin(), mru_ghost_.end(), page_id);
    // mru_ghost_.erase(idx);
  } else if (ghost_map_.find(page_id) != ghost_map_.end() && ghost_map_[page_id]->arc_status_ == ArcStatus::MFU_GHOST) {
    if (mfu_ghost_.size() >= mru_ghost_.size()) {
      mru_target_size_--;
    } else if (!mfu_ghost_.empty()) {
      // 因为是size_t，因此没有负数，所以这里如果减成负数会溢出
      mru_target_size_ -= std::min(mru_target_size_, mru_ghost_.size() / mfu_ghost_.size());
    }

    // alive
    mru_target_size_ = std::max(mru_target_size_, static_cast<size_t>(0));
    alive_map_[frame_id] = std::make_shared<FrameStatus>(page_id, frame_id, false, ArcStatus::MFU);
    mfu_.push_front(frame_id);
    alive_map_iter_[frame_id] = mfu_.begin();
    // auto idx = std::find(mfu_ghost_.begin(), mfu_ghost_.end(), page_id);
    // mfu_ghost_.erase(idx);
    // ghost
    LPerase(mfu_ghost_, page_id);
    ghost_map_.erase(page_id);
  } else if (alive_map_.find(frame_id) != alive_map_.end() && alive_map_[frame_id]->arc_status_ == ArcStatus::MFU) {
    // auto idx = std::find(mfu_.begin(), mfu_.end(), frame_id);
    // mfu_.erase(idx);
    LFerase(mfu_, frame_id);
    mfu_.push_front(frame_id);
    alive_map_iter_[frame_id] = mfu_.begin();
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Toggle whether a frame is evictable or non-evictable. This function
 * also controls replacer's size. Note that size is equal to number of evictable
 * entries.
 *
 * If a frame was previously evictable and is to be set to non-evictable, then
 * size should decrement. If a frame was previously non-evictable and is to be
 * set to evictable, then size should increment.
 *
 * If frame id is invalid, throw an exception or abort the process.
 *
 * For other scenarios, this function should terminate without modifying
 * anything.
 *
 * @param frame_id id of frame whose 'evictable' status will be modified
 * @param set_evictable whether the given frame is evictable or not
 */
void ArcReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  if (set_evictable && !alive_map_[frame_id]->evictable_) {
    curr_size_++;
  } else if (!set_evictable && alive_map_[frame_id]->evictable_) {
    curr_size_--;
  } else if (alive_map_.find(frame_id) == alive_map_.end()) {
    throw std::invalid_argument("frame does not exist in alive_map_");
  }
  alive_map_[frame_id]->evictable_ = set_evictable;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Remove an evictable frame from replacer.
 * This function should also decrement replacer's size if removal is successful.
 *
 * Note that this is different from evicting a frame, which always remove the
 * frame decided by the ARC algorithm.
 *
 * If Remove is called on a non-evictable frame, throw an exception or abort the
 * process.
 *
 * If specified frame is not found, directly return from this function.
 *
 * @param frame_id id of frame to be removed
 */
void ArcReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  if (alive_map_.find(frame_id) == alive_map_.end()) {
  } else if (!alive_map_[frame_id]->evictable_) {
    throw std::runtime_error("frame_id is not evictable");
  } else {
    if (alive_map_[frame_id]->arc_status_ == ArcStatus::MRU) {
      LFerase(mru_, frame_id);
      // alive_map_iter_.erase(frame_id);
    } else {
      LFerase(mfu_, frame_id);
      // alive_map_iter_.erase(frame_id);
    }
    alive_map_.erase(frame_id);
    if (curr_size_ > 0) {
      curr_size_--;
    }
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto ArcReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(latch_);
  return curr_size_;
}
}  // namespace bustub
