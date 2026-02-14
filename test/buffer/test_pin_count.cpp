#include <iostream>
#include <memory>
#include <mutex>
#include "buffer/arc_replacer.h"
#include "buffer/buffer_pool_manager.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/disk/disk_scheduler.h"

// #include "gtest/gtest.h"
using namespace bustub;
int main() {
  const size_t FRAMES = 10;
  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(FRAMES, disk_manager.get());

  // Get a new write page and edit it. We will retrieve it later
  const auto mutable_page_id = bpm->NewPage();
  auto mutable_guard = bpm->WritePage(mutable_page_id);
  strcpy(mutable_guard.GetDataMut(), "data");  // NOLINT
  mutable_guard.Drop();

  {
    // Fill up the BPM again.
    std::vector<WritePageGuard> guards;
    for (size_t i = 0; i < FRAMES; i++) {
      auto new_pid = bpm->NewPage();
      guards.push_back(bpm->WritePage(new_pid));
      bpm->GetPinCount(new_pid);
    }
  }

  // Fetching the flushed page should result in seeing the changed value.
  std::cout << "Y1\n";
  auto immutable_guard = bpm->ReadPage(mutable_page_id);
  std::cout << immutable_guard.GetData() << '\n';
  std::cout << std::strcmp("data", immutable_guard.GetData()) << '\n';

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  std::cout << 1 << '\n';
}