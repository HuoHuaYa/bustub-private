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
 * @brief a new ArcReplacer, with lists initialized to be empty and target size to 0
 * @param num_frames the maximum number of frames the ArcReplacer will be required to cache
 */
ArcReplacer::ArcReplacer(size_t num_frames) : replacer_size_(num_frames) {
  mru_.clear();
  mfu_.clear();
  mru_ghost_.clear();
  mfu_ghost_.clear();
  replacer_size_ = num_frames;
  mru_target_size_ = static_cast<size_t>(num_frames * 0.5);
}
void ArcReplacer::LFerase(std::list<frame_id_t> &List, frame_id_t fid) {
  auto idx = std::find(List.begin(), List.end(), fid);
  if (idx != List.end()) {
    List.erase(idx);
    // alive_map_.erase(fid);
  }
}
void ArcReplacer::LPerase(std::list<page_id_t> &List, page_id_t pid) {
  auto idx = std::find(List.begin(), List.end(), pid);
  if (idx != List.end()) {
    List.erase(idx);
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
 * 2. Entries that are not evictable are skipped. If all entries from the desired side
 * (mru_ / mfu_) are pinned, we instead try victimize the other side (mfu_ / mru_),
 * and move it to its corresponding ghost list (mfu_ghost_ / mru_ghost_).
 *
 * @return frame id of the evicted frame, or std::nullopt if cannot evict
 */
auto ArcReplacer::Evict() -> std::optional<frame_id_t> {
  // optional<xxx>表示要么返回 xxx,要么返回空值
  if (curr_size_ == 0) {
    return std::nullopt;
  }
  // 没有可驱逐页面，返回空值
  // 被驱逐对象的增删（删除当前，加入新链表，维护map）
  auto tryevict = [&](std::list<frame_id_t> &Tlist, std::list<page_id_t> &ghost,
                      ArcStatus arcstatus) -> std::optional<frame_id_t> {
    auto idx = Tlist.begin();
    while (idx != Tlist.end()) {
      frame_id_t fid = *idx;
      if (alive_map_.find(fid) != alive_map_.end()) {
        if (alive_map_[fid]->evictable_) {
          page_id_t pid = alive_map_[fid]->page_id_;
          curr_size_--;
          idx = Tlist.erase(idx);
          ghost.push_front(alive_map_[fid]->page_id_);
          ghost_map_[pid] = alive_map_[fid];
          ghost_map_[pid]->arc_status_ = arcstatus;
          alive_map_.erase(fid);
          return fid;
        }
      } else {
        idx++;
      }
    }
    return std::nullopt;
  };

  if (mru_.size() >= mru_target_size_) {
    auto ret = tryevict(mru_, mru_ghost_, ArcStatus::MRU_GHOST);
    if (ret == std::nullopt) {
      auto Rret = tryevict(mfu_, mfu_ghost_, ArcStatus::MFU_GHOST);
      if (Rret != std::nullopt) {
        return Rret;
      }
    } else {
      return ret;
    }
  } else {
    auto ret = tryevict(mfu_, mfu_ghost_, ArcStatus::MFU_GHOST);
    if (ret == std::nullopt) {
      auto Rret = tryevict(mru_, mru_ghost_, ArcStatus::MRU_GHOST);
      if (Rret != std::nullopt) {
        return Rret;
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
 * by bring the accessed page to the front of mfu_ if it exists in any of the lists
 * or the front of mru_ if it does not.
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
 * Using page_id for alive pages should be the same since it's one to one mapping,
 * but using frame_id is slightly more intuitive.
 *
 * @param frame_id id of frame that received a new access.
 * @param page_id id of page that is mapped to the frame.
 * @param access_type type of access that was received. This parameter is only needed for
 * leaderboard tests.
 */
void ArcReplacer::RecordAccess(frame_id_t frame_id, page_id_t page_id, [[maybe_unused]] AccessType access_type) {
  // 更新，增删自己（链表，status），更新p ,map status
  if (alive_map_.find(frame_id) == alive_map_.end() && ghost_map_.find(page_id) == ghost_map_.end()) {
    // 从未别命中，加入mru
    if (mru_.size() + mru_ghost_.size() == replacer_size_) {
      if (mru_ghost_.size()) {
        page_id_t pid = mru_ghost_.back();
        ghost_map_.erase(pid);
        mru_ghost_.pop_back();
      }
    } else {
      if (mru_.size() + mru_ghost_.size() + mfu_ghost_.size() == 2 * replacer_size_) {
        if (mfu_ghost_.size()) {
          page_id_t pid = mfu_ghost_.back();
          ghost_map_.erase(pid);
          mfu_ghost_.pop_back();
        }
      }
    }
    alive_map_[frame_id] = std::make_shared<FrameStatus>(page_id, frame_id, false, ArcStatus::MRU);
    mru_.push_front(frame_id);
  } else if (alive_map_.find(frame_id) != alive_map_.end() && alive_map_[frame_id]->arc_status_ == ArcStatus::MRU) {
    alive_map_[frame_id]->arc_status_ = ArcStatus::MFU;
    // auto idx = std::find(mru_.begin(), mru_.end(), frame_id);
    // mru_.erase(idx);
    LFerase(mru_, frame_id);
    mfu_.push_front(frame_id);
  } else if (ghost_map_.find(page_id) != ghost_map_.end() && ghost_map_[page_id]->arc_status_ == ArcStatus::MRU_GHOST) {
    if (mru_ghost_.size() >= mfu_ghost_.size()) {
      mru_target_size_++;
    } else if (mru_ghost_.size() > 0) {
      mru_target_size_ += mfu_ghost_.size() / mru_ghost_.size();
    }
    mru_target_size_ = std::min(mru_target_size_, replacer_size_);
    alive_map_[frame_id] = std::make_shared<FrameStatus>(page_id, frame_id, false, ArcStatus::MFU);

    mfu_.push_front(frame_id);
    ghost_map_.erase(page_id);
    // auto idx = std::find(mru_ghost_.begin(), mru_ghost_.end(), page_id);
    // mru_ghost_.erase(idx);
    LPerase(mru_ghost_, page_id);
  } else if (ghost_map_.find(page_id) != ghost_map_.end() && ghost_map_[page_id]->arc_status_ == ArcStatus::MFU_GHOST) {
    if (mfu_ghost_.size() >= mru_ghost_.size()) {
      mru_target_size_--;
    } else if (mfu_ghost_.size() > 0) {
      mru_target_size_ -= mru_ghost_.size() / mfu_ghost_.size();
    }
    mru_target_size_ = std::max(mru_target_size_, (size_t)0);
    alive_map_[frame_id] = std::make_shared<FrameStatus>(page_id, frame_id, false, ArcStatus::MFU);
    mfu_.push_front(frame_id);
    ghost_map_.erase(page_id);
    // auto idx = std::find(mfu_ghost_.begin(), mfu_ghost_.end(), page_id);
    // mfu_ghost_.erase(idx);
    LPerase(mfu_ghost_, page_id);
  } else if (alive_map_.find(frame_id) != alive_map_.end() && alive_map_[frame_id]->arc_status_ == ArcStatus::MFU) {
    // auto idx = std::find(mfu_.begin(), mfu_.end(), frame_id);
    // mfu_.erase(idx);
    LFerase(mfu_, frame_id);
    mfu_.push_front(frame_id);
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Toggle whether a frame is evictable or non-evictable. This function also
 * controls replacer's size. Note that size is equal to number of evictable entries.
 *
 * If a frame was previously evictable and is to be set to non-evictable, then size should
 * decrement. If a frame was previously non-evictable and is to be set to evictable,
 * then size should increment.
 *
 * If frame id is invalid, throw an exception or abort the process.
 *
 * For other scenarios, this function should terminate without modifying anything.
 *
 * @param frame_id id of frame whose 'evictable' status will be modified
 * @param set_evictable whether the given frame is evictable or not
 */
void ArcReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  if (alive_map_.find(frame_id) == alive_map_.end()) {
    throw std::invalid_argument("frame does not exist in alive_map_");
  } else if (set_evictable && !alive_map_[frame_id]->evictable_) {
    curr_size_++;
    alive_map_[frame_id]->evictable_ = true;
  } else if (!set_evictable && alive_map_[frame_id]->evictable_) {
    curr_size_--;
    alive_map_[frame_id]->evictable_ = false;
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Remove an evictable frame from replacer.
 * This function should also decrement replacer's size if removal is successful.
 *
 * Note that this is different from evicting a frame, which always remove the frame
 * decided by the ARC algorithm.
 *
 * If Remove is called on a non-evictable frame, throw an exception or abort the
 * process.
 *
 * If specified frame is not found, directly return from this function.
 *
 * @param frame_id id of frame to be removed
 */
void ArcReplacer::Remove(frame_id_t frame_id) {
  if (alive_map_.find(frame_id) == alive_map_.end()) {
    return;
  } else if (alive_map_[frame_id]->evictable_ == false) {
    throw std::runtime_error("frame_id is not evictable");
  } else {
    if (alive_map_[frame_id]->arc_status_ == ArcStatus::MRU) {
      LFerase(mru_, frame_id);
    } else {
      LFerase(mfu_, frame_id);
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
auto ArcReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
