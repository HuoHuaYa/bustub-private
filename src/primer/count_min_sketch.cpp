//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// count_min_sketch.cpp
//
// Identification: src/primer/count_min_sketch.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/count_min_sketch.h"

#include <stdexcept>
#include <string>

namespace bustub {

/**
 * Constructor for the count-min sketch.
 *
 * @param width The width of the sketch matrix.
 * @param depth The depth of the sketch matrix.
 * @throws std::invalid_argument if width or depth are zero.
 */
template <typename KeyType>
CountMinSketch<KeyType>::CountMinSketch(uint32_t width, uint32_t depth) : width_(width), depth_(depth) {
  /** @TODO(student) Implement this function! */
  if (width == 0 || depth == 0) {
    throw std::invalid_argument("构造长宽存在0");
  }
  // counts_.resize(depth, std::vector<std::atomic<size_t>>(width, 0));这个写法是拷贝一个vector<atomic...>
  // 但是atomic不支持拷贝所以不行吧

  counts_.reserve(depth);  // 只能是reserve，因为resize是直接赋予，而reverse是先预定，后面等待手动赋予
  // 而atomic不能拷贝，因此不能直接赋予
  for (size_t i = 0; i < depth; ++i) {
    counts_.emplace_back(width);
  }  // 直接构造 width 个默认初始化的 atomic

  /** @fall2025 PLEASE DO NOT MODIFY THE FOLLOWING */
  // Initialize seeded hash functions
  hash_functions_.reserve(depth_);
  for (size_t i = 0; i < depth_; i++) {
    hash_functions_.push_back(this->HashFunction(i));
  }
}

template <typename KeyType>
CountMinSketch<KeyType>::CountMinSketch(CountMinSketch &&other) noexcept : width_(other.width_), depth_(other.depth_) {
  /** @TODO(student) Implement this function! */
  hash_functions_ = std::move(other.hash_functions_);
  counts_ = std::move(other.counts_);
}

template <typename KeyType>
auto CountMinSketch<KeyType>::operator=(CountMinSketch &&other) noexcept -> CountMinSketch & {
  /** @TODO(student) Implement this function! */
  if (this != &other) {
    depth_ = other.depth_;
    width_ = other.width_;
    hash_functions_ = std::move(other.hash_functions_);
    counts_ = std::move(other.counts_);
  }
  return *this;
}

template <typename KeyType>
void CountMinSketch<KeyType>::Insert(const KeyType &item) {
  /** @TODO(student) Implement this function! */
  for (size_t i = 0; i < depth_; i++) {
    size_t rowi_index = hash_functions_[i](item);
    counts_[i][rowi_index].fetch_add(1, std::memory_order_relaxed);
    // 对counts_[i][col_idx]做原子自增（线程安全，内存顺序用relaxed足够）
  }
}

template <typename KeyType>
void CountMinSketch<KeyType>::Merge(const CountMinSketch<KeyType> &other) {
  if (width_ != other.width_ || depth_ != other.depth_) {
    throw std::invalid_argument("Incompatible CountMinSketch dimensions for merge.");
  }
  for (size_t i = 0; i < depth_; i++) {
    for (size_t j = 0; j < width_; j++) {
      counts_[i][j].fetch_add(other.counts_[i][j].load(), std::memory_order_relaxed);
    }
  }
  /** @TODO(student) Implement this function! */
}

template <typename KeyType>
auto CountMinSketch<KeyType>::Count(const KeyType &item) const -> uint32_t {
  uint32_t min_val = __UINT32_MAX__;
  for (size_t i = 0; i < depth_; i++) {
    min_val = std::min(min_val, static_cast<uint32_t>(counts_[i][hash_functions_[i](item)].load()));
  }
  return min_val;
}

template <typename KeyType>
void CountMinSketch<KeyType>::Clear() {
  /** @TODO(student) Implement this function! */
  for (size_t i = 0; i < depth_; i++) {
    for (size_t j = 0; j < width_; j++) {
      counts_[i][j].store(0, std::memory_order_relaxed);
    }
  }
}

template <typename KeyType>
auto CountMinSketch<KeyType>::TopK(uint16_t k, const std::vector<KeyType> &candidates)
    -> std::vector<std::pair<KeyType, uint32_t>> {
  /** @TODO(student) Implement this function! */
  std::vector<std::pair<KeyType, uint32_t>> can_val;
  can_val.reserve(candidates.size());
  for (auto &members : candidates) {
    can_val.emplace_back(members, Count(members));
  }
  std::sort(
      can_val.begin(), can_val.end(),
      [](const std::pair<KeyType, uint32_t> &a, const std::pair<KeyType, uint32_t> &b) { return a.second > b.second; });
  size_t numbers = std::min(static_cast<size_t>(k), candidates.size());
  return std::vector<std::pair<KeyType, uint32_t>>(can_val.begin(), can_val.begin() + numbers);
}

// Explicit instantiations for all types used in tests
template class CountMinSketch<std::string>;
template class CountMinSketch<int64_t>;  // For int64_t tests
template class CountMinSketch<int>;      // This covers both int and int32_t
}  // namespace bustub
