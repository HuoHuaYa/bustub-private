//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include <vector>
#include "common/macros.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  // Spawn the background thread
  background_thread_.emplace([&] { StartWorkerThread(); });
  // 这个是给内部optional空着的background_thread_内部构造一个thread对象
  // 而lambda：独立可调用的对象，内部包裹start（），作为参数传给thread，用于构造thread对象
  // 因为thread不支持拷贝，所以通过emplace这个方式内部构造
  // 同时不能直接调用StartWorkerThread()，编译器会自动添加一个this指针参数
  // DiskScheduler::StartWorkerThread(DiskScheduler *this)
  // thread无法给这个指针参数，所以他必须要可以直接调用，没有隐含未绑定参数的可调用对象
  // 如果start内部有参数你正常传就行([&]{star(x);})
}
DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {  // 检测 background_thread_是否持有有效的 thread 对象
    background_thread_->join();
    // 1. 析构线程（用于触发对象销毁的已有线程，独立于后台线程）执行此join函数
    // 2. 析构线程进入阻塞状态，等待 background_thread_
    // 包裹的【后台业务线程】完成所有任务
    // 3. 后台业务线程完成后，join()
    // 自动回收该后台业务线程的资源，析构线程解除阻塞
    // 4. 一个thread只能执行一次join()
  }
  // diskmanager是指针不用析构，request_queue自己有request析构函数，optional会调用thread的析构函数所以也不用
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Schedules a request for the DiskManager to execute.
 *
 * @param requests The requests to be scheduled.
 */
void DiskScheduler::Schedule(std::vector<DiskRequest> &requests) {
  for (auto &request : requests) {
    request_queue_.Put(std::make_optional(std::move(request)));
  }  // request包含promise不可拷贝，所以要移动
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Background worker thread function that processes scheduled requests.
 *
 * The background thread needs to process requests while the DiskScheduler
 * exists, i.e., this function should not return until ~DiskScheduler() is
 * called. At that point you need to make sure that the function does return.
 */
void DiskScheduler::StartWorkerThread() {
  while (true) {
    std::optional<DiskRequest> request = request_queue_.Get();
    // optional：对象容器，可以近似看作指针（本质不是指针，但是引用时和指针一样调用）
    if (request == std::nullopt) {
      break;
    }
    try {
      if (request->is_write_) {
        disk_manager_->WritePage(request->page_id_, request->data_);
      } else {
        disk_manager_->ReadPage(request->page_id_, request->data_);
      }
      request->callback_.set_value(true);
    } catch (...) {
      request->callback_.set_exception(std::current_exception());
    }
  }
}
// 磁盘处理可能会报错，所以我们必须做报错的准备，promise就是对策
}  // namespace bustub
