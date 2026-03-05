#pragma once

#include <memory>
#include <cstddef>
#include <utility>
#include <vector>

#include "storage/table/tuple.h"

namespace bustub {

/**
 * Page to hold the intermediate data for external merge sort and hash join.
 * Supports variable-length tuples.
 */
class IntermediateResultPage {
public:
  // 初始化一个全新的空页
  void Init() {
    num_tuples_ = 0;
    // 初始时，空闲指针指向页面的绝对末尾
    next_free_ptr_ = BUSTUB_PAGE_SIZE;
  }

  // 获取页面中存储的 Tuple 数量
  auto GetNumTuples() const -> uint32_t { return num_tuples_; }

  // 核心功能声明
  auto InsertTuple(const Tuple &tuple) -> bool {
    // 获取 Tuple 自身的原始数据大小
    uint32_t data_size = tuple.GetLength();
    // 加进来的tuple要多少空间
    uint32_t serialized_size = data_size + sizeof(uint32_t);

    // 计算页面剩余空间是否足够
    // header 大小 (8字节) + 现有的所有 slot 大小 + 即将新增的 1 个 slot 大小
    uint32_t used_header_space = 8 + (num_tuples_ + 1) * sizeof(Slot);
    if (used_header_space + serialized_size > next_free_ptr_) {
      return false; // 空间不够，插入失败
    }

    next_free_ptr_ -= serialized_size;

    // 将Tuple序列化到页面的指定位置
    // 直接利用 tuple.cpp 提供的 SerializeTo 函数，不需要自己写 memcpy！
    char *target_ptr = reinterpret_cast<char *>(this) + next_free_ptr_;
    tuple.SerializeTo(target_ptr);

    // 记录槽位Slot
    Slot *slot_array =
        reinterpret_cast<Slot *>(reinterpret_cast<char *>(this) + 8);
    slot_array[num_tuples_].offset_ = next_free_ptr_;
    slot_array[num_tuples_].size_ = serialized_size; // 记录总的物理占用大小

    num_tuples_++;
    return true;
  }
  auto GetTuple(uint32_t tuple_index, Tuple *tuple) const -> void {
    // 越界检查
    BUSTUB_ASSERT(tuple_index < num_tuples_, "Tuple index out of range");

    // 找到对应的 Slot，优雅的找到Slot的数组指针
    const Slot *slot_array = reinterpret_cast<const Slot *>(
        reinterpret_cast<const char *>(this) + 8);
    const Slot &slot = slot_array[tuple_index];

    // 找到tuple绝对地址
    const char *tuple_data_ptr =
        reinterpret_cast<const char *>(this) + slot.offset_;

    // 直接调用 tuple.cpp 里提供的 DeserializeFrom，它会自动读取前 4 个字节作为
    // size，并拷贝后续数据！
    tuple->DeserializeFrom(tuple_data_ptr);
  }

private:
  // 必须严格控制 Header 的大小
  uint32_t num_tuples_; // 已经插入了多少个 Tuple
  uint32_t
      next_free_ptr_; // 数据区域是从后往前长的，这个指针记录数据的"最低水位线"

  // 槽位结构：记录每个 Tuple 在页内的起始位置和大小
  struct Slot {
    uint32_t offset_;
    uint32_t size_;
  };

  // 指向Header之后的空间，用来存放Slot和真实数据
  // char page_data_[0];
};

} // namespace bustub
