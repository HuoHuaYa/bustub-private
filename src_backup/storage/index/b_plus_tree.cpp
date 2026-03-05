//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree.cpp
//
// Identification: src/storage/index/b_plus_tree.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/index/b_plus_tree.h"

#include "buffer/traced_buffer_pool_manager.h"
#include "storage/index/b_plus_tree_debug.h"
namespace bustub {

FULL_INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id,
                          BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size,
                          int internal_max_size)
    : bpm_(std::make_shared<TracedBufferPoolManager>(buffer_pool_manager)),
      index_name_(std::move(name)), comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size), internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->WritePage(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/**
 * @brief Helper function to decide whether current b+tree is empty
 * @return Returns true if this B+ tree has no keys and values.
 */
// 判断树是否为空
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  auto root_page_id = const_cast<BPLUSTREE_TYPE *>(this)->GetRootPageId();
  return root_page_id == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/**
 * @brief Return the only value that associated with input key
 *
 * This method is used for point query
 *
 * @param key input key
 * @param[out] result vector that stores the only value that associated with
 * input key, if the value exists
 * @return : true means key exists
 */
// 返回与输入键有关的唯一的值，用于点查询
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key,
                              std::vector<ValueType> *result) -> bool {
  // UNIMPLEMENTED("TODO(P2): Add implementation.");
  // Declaration of context instance. Using the Context is not necessary but
  // advised.
  Context ctx;
  const LeafPage *leaf_page = FindLeafForRead(key, &ctx);
  // 判断叶页是否无效
  if (leaf_page == nullptr) {
    return false;
  }
  // KeyIndex 通过二分获得正确索引
  int index = leaf_page->KeyIndex(key, comparator_);
  // 再次检查索引是否对应键值
  if (index >= 0 && index < leaf_page->GetSize() &&
      comparator_(leaf_page->KeyAt(index), key) == 0) {
    // 检查墓碑机制
    if (!leaf_page->IsTombstone(index)) {
      result->emplace_back(leaf_page->ValueAt(index));
      return true;
    }
  }
  return false;
}
// 实现一个寻找叶子节点页面的函数--
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafForRead(const KeyType &key, Context *context) const
    -> const LeafPage * {
  // 因为getrootpageid不能保证是一个只读的函数，拥有更改内容的风险。不符合上面const定义
  // 这里会context。emplace，所以就算没有成功，也需要pop_back
  page_id_t root_page_id = GetRootPageIdForRead(context);

  if (root_page_id == INVALID_PAGE_ID) {
    context->read_set_.clear();
    return nullptr;
  }
  // 通过header page获得root page id
  // ，也就是说在我为rootpage上锁的时候，我需要保证header page 不变
  // 所以在给他上锁之前，不能够轻易删除headerpageguard
  auto guard = bpm_->ReadPage(root_page_id);
  auto page = guard.As<BPlusTreePage>();
  if (!context->read_set_.empty()) {
    context->read_set_.pop_back();
  }

  // 判断叶子
  while (!page->IsLeafPage()) {
    // 通过internal中的lookup找到正确的子页面
    auto internal_page = guard.template As<InternalPage>();
    auto child_page_id = internal_page->Lookup(key, comparator_);
    auto child_guard = bpm_->ReadPage(child_page_id);
    auto child_page = child_guard.template As<BPlusTreePage>();
    // 给子页面上锁
    guard = std::move(child_guard);
    page = child_page;
  }
  context->read_set_.emplace_back(std::move(guard));
  return context->read_set_.back().As<LeafPage>();
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageIdForRead(Context *ctx) const -> page_id_t {
  auto header_page_guard = bpm_->ReadPage(header_page_id_);
  auto head_page = header_page_guard.As<BPlusTreeHeaderPage>();
  if (ctx != nullptr) {
    ctx->root_page_id_ = head_page->root_page_id_;
    ctx->read_set_.emplace_back(std::move(header_page_guard));
  }
  return head_page->root_page_id_;
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/**
 * @brief Insert constant key & value pair into b+ tree
 * 添加常量的键与值
 * if current tree is empty, start new tree, update root page id and insert
 * entry; otherwise, insert into leaf page.
 *  树是空的就要开一棵新树，更新条目否则就插入叶页
 * @param key the key to insert
 * @param value the value associated with key
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false; otherwise, return true.
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value)
    -> bool {
  // 乐观锁
  // 经典掏header_page
  {
    auto head_guard = bpm_->ReadPage(header_page_id_);
    auto header_page = head_guard.template As<BPlusTreeHeaderPage>();
    page_id_t root_id = header_page->root_page_id_;
    if (root_id != INVALID_PAGE_ID) {
      auto current_guard = bpm_->ReadPage(root_id);
      head_guard.Drop();
      auto current_page = current_guard.template As<BPlusTreePage>();
      if (current_page->IsLeafPage()) {
        if (current_page->GetSize() < current_page->GetMaxSize() - 1) {
          current_guard.Drop();
          auto leaf_write_guard = bpm_->WritePage(root_id);
          auto leaf_page = leaf_write_guard.AsMut<LeafPage>();
          // 二次校验，避免并发修改
          if (leaf_page->GetSize() < leaf_page->GetMaxSize() - 1) {
            if (leaf_page->Insert(key, value, comparator_)) {
              return true;
            }
          }
        }
      } else {
        while (true) {
          auto internal_page = current_guard.As<InternalPage>();
          page_id_t next_page_id = internal_page->Lookup(key, comparator_);
          auto next_guard = bpm_->ReadPage(next_page_id);
          auto next_page = next_guard.As<BPlusTreePage>();
          if (next_page->IsLeafPage()) {
            if (next_page->GetSize() < next_page->GetMaxSize() - 1) {
              next_guard.Drop();
              auto leaf_write_guard = bpm_->WritePage(next_page_id);
              auto leaf_page = leaf_write_guard.AsMut<LeafPage>();
              current_guard.Drop();
              if (leaf_page->GetSize() < leaf_page->GetMaxSize() - 1) {
                if (leaf_page->Insert(key, value, comparator_)) {
                  return true;
                }
              }
            } else {
              next_guard.Drop();
              current_guard.Drop();
            }
            break;
          }
          // Crabbing
          current_guard = std::move(next_guard);
        }
      }
    } else {
      // 空树，释放 header
      head_guard.Drop();
    }
  }
  // UNIMPLEMENTED("TODO(P2): Add implementation.");
  // Declaration of context instance. Using the Context is not necessary but
  // advised.
  Context ctx;
  auto leaf_guard = FindLeafForWrite(key, &ctx);
  // 空树就要从0开始初始化他的根节点了
  if (ctx.root_page_id_ == INVALID_PAGE_ID) {
    page_id_t new_page_id = bpm_->NewPage();
    // 可能因为缓存池满了，导致没有成功申请到page
    if (new_page_id == INVALID_PAGE_ID) {
      ctx.Clear();
      return false;
    }
    // 初始化新的页面
    auto new_root_guard = bpm_->WritePage(new_page_id);
    // newpage  writepage  都会是pin+1因此这里要-1
    auto new_root_page = new_root_guard.AsMut<LeafPage>();
    new_root_page->Init(leaf_max_size_);
    new_root_page->Insert(key, value, comparator_);
    // 更新header中的rootpageid
    auto head_page = ctx.header_page_->template AsMut<BPlusTreeHeaderPage>();
    head_page->root_page_id_ = new_page_id;
    ctx.Clear();
    return true;
  }
  auto leaf_page = leaf_guard.template AsMut<LeafPage>();
  if (leaf_page->Insert(key, value, comparator_)) {
    ctx.Clear();
    return true;
  }
  // 两种失败：已经有了这个key，size == maxsize
  // 验证是不是第一种
  int index = leaf_page->KeyIndex(key, comparator_);
  if (index < leaf_page->GetSize() &&
      comparator_(leaf_page->KeyAt(index), key) == 0) {
    if (!leaf_page->IsTombstone(index)) {
      ctx.Clear();
      return false;
    }
  }
  // 页面满载，需要分裂,开一个新的页面
  page_id_t new_page_id = bpm_->NewPage();
  if (new_page_id == INVALID_PAGE_ID) {
    ctx.Clear();
    return false;
  }
  // leaf_page->Compact();
  // 初始化新页
  auto new_page_guard = bpm_->WritePage(new_page_id);
  auto new_page = new_page_guard.AsMut<LeafPage>();
  new_page->Init(leaf_max_size_);
  // 分裂，更新兄弟页面
  leaf_page->Split(new_page, key, value, comparator_);
  new_page->SetNextPageId(leaf_page->GetNextPageId());
  leaf_page->SetNextPageId(new_page_id);
  // 给父亲节点提供儿子的key
  KeyType split_key = new_page->KeyAt(0);
  // 无法通过page获取page id ，所以这里要传page id
  InsertIntoParent(leaf_guard.GetPageId(), split_key, new_page_id, &ctx);
  ctx.Clear();
  return true;
}
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafForWrite(const KeyType &key, Context *ctx)
    -> WritePageGuard {
  auto head_guard = bpm_->WritePage(header_page_id_);
  auto head_page = head_guard.AsMut<BPlusTreeHeaderPage>();
  ctx->root_page_id_ = head_page->root_page_id_;
  ctx->header_page_ = std::move(head_guard);
  // 判断是否为空树
  if (ctx->root_page_id_ == INVALID_PAGE_ID) {
    return {};
  }
  auto guard = bpm_->WritePage(ctx->root_page_id_);
  auto page = guard.AsMut<BPlusTreePage>();
  // 如果根节点就是叶子节点，head的锁就永远都不会解开了
  bool root_is_safe = false;
  if (page->IsLeafPage()) {
    root_is_safe = (page->GetSize() < (page->GetMaxSize() - 1));
  } else {
    root_is_safe = page->GetSize() < page->GetMaxSize();
  }
  if (root_is_safe) {
    ctx->header_page_ = std::nullopt;
  }
  // 从上往下搜，走螃蟹步QWQ
  while (!page->IsLeafPage()) {
    ctx->write_set_.emplace_back(std::move(guard));
    // 获取相应子节点位置    upper - 1 找到的是一定key的位置
    auto internal_node = ctx->write_set_.back().template AsMut<InternalPage>();
    page_id_t next_page_id = internal_node->Lookup(key, comparator_);
    // 更新
    guard = bpm_->WritePage(next_page_id);
    page = guard.AsMut<BPlusTreePage>();

    bool is_safe = false;
    if (page->IsLeafPage()) {
      is_safe = (page->GetSize() < (page->GetMaxSize() - 1));
    } else {
      is_safe = page->GetSize() < (page->GetMaxSize());
    }
    if (is_safe) {
      // 螃蟹步的重点：我删的是父亲节点，当前节点安全祖先全都不需要分裂
      // 同时因为rootnode不会分裂，不需要headguard保护rootid了
      ctx->header_page_ = std::nullopt;
      ctx->write_set_.clear();
    }
  }
  return guard;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(page_id_t old_page_id, const KeyType &key,
                                      page_id_t new_page_id, Context *ctx) {
  page_id_t current_old_page_id = old_page_id;
  KeyType current_split_key = key;
  page_id_t current_new_page_id = new_page_id;
  std::vector<WritePageGuard> guards_to_keep_alive;
  while (true) {
    // 如果走到这一步，要么叶子节点是安全的要么就是根都已经分裂了，已经没有父亲了
    // 不可能是前者，因为在insert中直接返回true结束了
    // 现在就是创造一个新的根节点
    if (ctx->write_set_.empty()) {
      // 经典四板斧
      page_id_t new_root_id = bpm_->NewPage();
      // bpm_的问题
      if (new_root_id == INVALID_PAGE_ID) {
        return;
      }
      auto new_root_guard = bpm_->WritePage(new_root_id);
      auto new_root_page = new_root_guard.template AsMut<InternalPage>();
      new_root_page->Init(internal_max_size_);

      // 给newroot赋值
      new_root_page->PopulateNewRoot(current_old_page_id, current_split_key,
                                     current_new_page_id);
      // DEBUG
      // std::cout << "DEBUG: New Root Created (PageID=" << new_root_id << ")"
      //           << std::endl;
      // std::cout << "GetSize() " << ' ' << new_root_page->GetSize() << '\n';
      // std::cout << "  -> Left Child (Index 0) ID: " <<
      // new_root_page->ValueAt(0)
      //           << " (Should be " << current_old_page_id << ")" << std::endl;
      // std::cout << "  -> Right Child (Index 1) ID: "
      //           << new_root_page->ValueAt(1) << " (Should be "
      //           << current_new_page_id << ")" << std::endl;
      // std::cout << "  -> Split Key: " << new_root_page->KeyAt(1) <<
      // std::endl; 记得给headerpage相关都更新一下
      auto header_page = ctx->header_page_->AsMut<BPlusTreeHeaderPage>();
      header_page->root_page_id_ = new_root_id;
      return;
    }

    // 处理正常的internal page了
    // 从ctx拿出来
    auto parent_guard = std::move(ctx->write_set_.back());
    ctx->write_set_.pop_back();
    auto parent_page = parent_guard.template AsMut<InternalPage>();

    // insert
    parent_page->InsertNodeAfter(current_old_page_id, current_split_key,
                                 current_new_page_id);
    // 检测当前parent page是否安全，安全就直接结束
    if (parent_page->GetSize() <= parent_page->GetMaxSize()) {
      guards_to_keep_alive.push_back(std::move(parent_guard));
      return;
    }
    // 证明需要开始分裂
    // 经典四板斧
    page_id_t new_internal_page_id = bpm_->NewPage();
    if (new_internal_page_id == INVALID_PAGE_ID) {
      return;
    }
    auto new_internal_guard = bpm_->WritePage(new_internal_page_id);
    auto new_internal_page = new_internal_guard.template AsMut<InternalPage>();
    new_internal_page->Init(internal_max_size_);
    // 开始分裂
    parent_page->Split(new_internal_page, &current_split_key);
    // 给下一个父亲提供参数
    current_old_page_id = parent_guard.GetPageId();
    current_new_page_id = new_internal_guard.GetPageId();
    // 保护parent_guard,以及parent的新兄弟
    guards_to_keep_alive.push_back(std::move(parent_guard));
    guards_to_keep_alive.push_back(std::move(new_internal_guard));
  }
}
/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/**
 * @brief Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target,
 * then delete entry from leaf page. Remember to deal with redistribute or
 * merge if necessary.
 *
 * @param key input key
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key) {
  // Declaration of context instance.
  if (IsEmpty()) {
    return;
  }
  // 乐观锁
  bool optimistic_success = false;
  // 经典掏header_page
  {
    auto head_guard = bpm_->ReadPage(header_page_id_);
    auto header_page = head_guard.template As<BPlusTreeHeaderPage>();
    page_id_t root_id = header_page->root_page_id_;
    // 如果这个树有空的直接删
    if (root_id != INVALID_PAGE_ID) {
      auto root_guard = bpm_->ReadPage(root_id);
      auto root_page = root_guard.template As<BPlusTreePage>();
      if (root_page->IsLeafPage()) {
        root_guard.Drop();
        auto root_write_guard = bpm_->WritePage(root_id);
        auto root_leaf = root_write_guard.template AsMut<LeafPage>();
        if (root_leaf->GetSize() > 1) {
          if (root_leaf->RemoveAndDeleteRecord(key, comparator_)) {
            optimistic_success = true;
          }
        }
      } else {
        // 是internal
        root_guard.Drop();
        // 换读锁
        auto current_guard = bpm_->ReadPage(root_id);
        head_guard.Drop();
        while (true) {
          auto internal_page = current_guard.template As<InternalPage>();
          page_id_t next_id = internal_page->Lookup(key, comparator_);
          auto next_guard = bpm_->ReadPage(next_id);
          auto next_page = next_guard.template As<BPlusTreePage>();
          // qa：onenote
          if (next_page->IsLeafPage()) {
            next_guard.Drop();
            // 一定要有父亲guard的保护
            auto leaf_guard = bpm_->WritePage(next_id);
            auto leaf_page = leaf_guard.template AsMut<LeafPage>();
            // 注意这个currentguard，他是和悲观锁的不同之处
            current_guard.Drop();
            if (leaf_page->GetSize() > leaf_page->GetMinSize()) {
              if (leaf_page->RemoveAndDeleteRecord(key, comparator_)) {
                optimistic_success = true;
              }
            }
            break;
          }
          current_guard = std::move(next_guard);
        }
      }
    } else {
      // 空树无负担删head
      head_guard.Drop();
    }
  }
  if (optimistic_success) {
    return;
  }
  // 悲观锁
  Context ctx;
  auto leaf_guard = FindLeafForRemove(key, &ctx);
  auto leaf_page = leaf_guard.template AsMut<LeafPage>();

  bool is_deleted = leaf_page->RemoveAndDeleteRecord(key, comparator_);
  if (!is_deleted) {
    ctx.Clear();
    return;
  }
  WritePageGuard current_guard = std::move(leaf_guard);
  auto current_page = current_guard.template AsMut<BPlusTreePage>();
  page_id_t current_page_id = current_guard.GetPageId();
  // 判断page是否安全
  auto get_min_size = [](BPlusTreePage *page) -> int {
    if (page->IsLeafPage()) {
      return page->GetMaxSize() / 2;
    }
    return (page->GetMaxSize() + 1) / 2;
  };
  auto get_effective_size = [](BPlusTreePage *page) -> int {
    return page->GetSize();
  };
  // 一直向上找，直到根或者不在需要分裂
  while (current_page_id != ctx.root_page_id_ &&
         get_effective_size(current_page) < get_min_size(current_page)) {
    // 经典连招，找出父亲节点所有信息，和找一个准备使用的兄弟
    auto parent_guard = std::move(ctx.write_set_.back());
    ctx.write_set_.pop_back();
    auto parent_page = parent_guard.template AsMut<InternalPage>();
    // 找兄弟：索引，pageid，guard，page
    int my_index = parent_page->ValueIndex(current_page_id);
    int sibling_index = my_index > 0 ? my_index - 1 : my_index + 1;
    //
    assert(sibling_index < parent_page->GetSize() &&
           "ERROR: Sibling index out of bounds! Tree is corrupted!");
    //
    page_id_t sibling_page_id = parent_page->ValueAt(sibling_index);
    auto sibling_guard = bpm_->WritePage(sibling_page_id);
    auto sibling_page = sibling_guard.template AsMut<BPlusTreePage>();
    // 开始借
    // 先判断兄弟有没有米

    if (get_effective_size(sibling_page) > get_min_size(sibling_page)) {
      // 兄弟有米，考虑leaf还是internal
      if (current_page->IsLeafPage()) {
        // 将他们的类型转换成为叶子
        auto *leaf = reinterpret_cast<LeafPage *>(current_page);
        auto *sibling_leaf = reinterpret_cast<LeafPage *>(sibling_page);

        while (get_effective_size(leaf) < get_min_size(leaf) &&
               get_effective_size(sibling_leaf) > get_min_size(sibling_leaf)) {
          // 唯一需要的保护：防止一直搬运墓碑把叶子节点的 tombstones_ 数组撑爆
          // 满了就踢掉最老的一个，绝对不调用 Compact() 清场
          while (LEAF_PAGE_TOMB_CNT > 0 &&
                 leaf->GetTombstonesNum() >= LEAF_PAGE_TOMB_CNT) {
            leaf->RemoveOldestTombstone();
          }

          if (sibling_index < my_index) {
            // 兄弟在左边，借最后一个放到我的前面
            sibling_leaf->MoveLastToFrontOf(leaf);
            parent_page->SetKeyAt(my_index, leaf->KeyAt(0));
          } else {
            // 兄弟在右边，借第一个放到我的后面
            sibling_leaf->MoveFirstToEndOf(leaf);
            parent_page->SetKeyAt(sibling_index, sibling_leaf->KeyAt(0));
          }
        }
      } else {
        // 他们的类型是内部节点，转成内部节点：
        auto *internal = reinterpret_cast<InternalPage *>(current_page);
        auto *sibling_internal = reinterpret_cast<InternalPage *>(sibling_page);
        if (sibling_index < my_index) {
          // 兄弟在左边:更新兄弟大小，更新父亲条目的key值
          int last_index = (sibling_internal->GetSize()) - 1;
          // pushfront会改leaf的大小
          internal->PushFront(parent_page->KeyAt(my_index),
                              sibling_internal->ValueAt(last_index));
          parent_page->SetKeyAt(my_index, sibling_internal->KeyAt(last_index));
          sibling_internal->SetSize(sibling_internal->GetSize() - 1);
        } else {
          // 兄弟在右边
          internal->PushBack(parent_page->KeyAt(sibling_index),
                             sibling_internal->ValueAt(0));
          // 兄弟在左边不用，因为最后一个键值对被送走，改size就相当于删了
          parent_page->SetKeyAt(sibling_index, sibling_internal->KeyAt(1));
          sibling_internal->Remove(0);
        }
      }
      // 兄弟刚好有米借我，我就不用找我爹了
      // break;
    }

    if (get_effective_size(sibling_page) <= get_min_size(sibling_page)) {
      // 兄弟也没米，我和兄弟一起凑点钱
      // 统一合并给左节点
      BPlusTreePage *left_page;
      BPlusTreePage *right_page;
      page_id_t right_page_id;
      int right_parent_index;
      if (sibling_index < my_index) {
        // 我是右儿子
        left_page = sibling_page;
        right_page = current_page;
        right_page_id = current_page_id;
        right_parent_index = my_index;
      } else {
        // 左儿子是me
        left_page = current_page;
        right_page = sibling_page;
        right_page_id = sibling_page_id;
        right_parent_index = sibling_index;
      }
      if (current_page->IsLeafPage()) {
        auto *left_leaf = static_cast<LeafPage *>(left_page);
        auto *right_leaf = static_cast<LeafPage *>(right_page);
        right_leaf->MoveAllTo(left_leaf);
      } else {
        auto *left_internal = static_cast<InternalPage *>(left_page);
        auto *right_internal = static_cast<InternalPage *>(right_page);
        // 因为key[0]无效，因此我们可以从父亲拿键值用
        right_internal->MoveAllTo(left_internal,
                                  parent_page->KeyAt(right_parent_index));
      }
      // 物理删除右节点
      if (sibling_index < my_index) {
        current_guard.Drop();
      } else {
        sibling_guard.Drop();
      }
      bpm_->DeletePage(right_page_id);
      // 从父亲那里删掉
      parent_page->Remove(right_parent_index);

      current_guard = std::move(parent_guard);
      // 更新一下current
      current_page = parent_page;
      current_page_id = current_guard.GetPageId();
    }
  }
  // 正常来说到这里就结束了，但是如果一直延伸到根了，就会断掉，这个时候就重置root
  if (current_page_id == ctx.root_page_id_) {
    // root是叶子,并且被删空了
    if (current_page->IsLeafPage() && current_page->GetSize() == 0) {
      // 全部清空
      auto header_page =
          ctx.header_page_->template AsMut<BPlusTreeHeaderPage>();
      header_page->root_page_id_ = INVALID_PAGE_ID;
      current_guard.Drop();
      bpm_->DeletePage(current_page_id);
    }
    // root是内部节点，只剩下最后一个节点
    else if (!current_page->IsLeafPage() && current_page->GetSize() == 1) {
      auto *root_internal = static_cast<InternalPage *>(current_page);
      // 换他的儿子上来，替代他
      page_id_t new_root_id = root_internal->ValueAt(0);
      auto header_page =
          ctx.header_page_->template AsMut<BPlusTreeHeaderPage>();
      header_page->root_page_id_ = new_root_id;

      // deletepage之前要drop掉锁
      current_guard.Drop();
      bpm_->DeletePage(current_page_id);
    }
  }
  ctx.Clear();
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafForRemove(const KeyType &key, Context *ctx)
    -> WritePageGuard {
  auto head_guard = bpm_->WritePage(header_page_id_);
  auto head_page = head_guard.AsMut<BPlusTreeHeaderPage>();
  ctx->root_page_id_ = head_page->root_page_id_;
  ctx->header_page_ = std::move(head_guard);
  // 判断是否为空树
  if (ctx->root_page_id_ == INVALID_PAGE_ID) {
    return {};
  }
  auto guard = bpm_->WritePage(ctx->root_page_id_);
  auto page = guard.AsMut<BPlusTreePage>();
  // 如果根节点就是叶子节点，head的锁就永远都不会解开了
  bool root_is_safe = false;
  if (page->IsLeafPage()) {
    root_is_safe = page->GetSize() > 1;
  } else {
    root_is_safe = page->GetSize() > 2;
  }
  if (root_is_safe) {
    ctx->header_page_ = std::nullopt;
  }
  // 从上往下搜，走螃蟹步QWQ
  while (!page->IsLeafPage()) {
    ctx->write_set_.emplace_back(std::move(guard));
    // 获取相应子节点位置    upper - 1 找到的是一定key的位置
    auto internal_node = ctx->write_set_.back().AsMut<InternalPage>();
    page_id_t next_page_id = internal_node->Lookup(key, comparator_);
    // 更新
    guard = bpm_->WritePage(next_page_id);
    page = guard.AsMut<BPlusTreePage>();

    bool is_safe = false;
    if (page->IsLeafPage()) {
      // auto *leaf_node = static_cast<LeafPage *>(page);
      is_safe = page->GetSize() > (page->GetMaxSize() / 2);
    } else {
      is_safe = page->GetSize() > ((page->GetMaxSize() + 1) / 2);
    }
    if (is_safe) {
      // 螃蟹步的重点：我删的是父亲节点，当前节点安全祖先全都不需要分裂
      // 同时因为rootnode不会分裂，不需要headguard保护rootid了
      ctx->header_page_ = std::nullopt;
      ctx->write_set_.clear();
    }
  }
  return guard;
}
/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/**
 * @brief Input parameter is void, find the leftmost leaf page first, then
 * construct index iterator
 *
 * You may want to implement this while implementing Task #3.
 *
 * @return : index iterator
 */
// 没有参数就是最左边的叶子节点
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return End();
  }
  // 经典操作，获取header
  auto head_guard = bpm_->ReadPage(header_page_id_);
  auto header_page = head_guard.template As<BPlusTreeHeaderPage>();
  page_id_t root_id = header_page->root_page_id_;
  if (root_id == INVALID_PAGE_ID) {
    head_guard.Drop();
    return INDEXITERATOR_TYPE(bpm_.get(), INVALID_PAGE_ID, -1, ReadPageGuard{});
  }
  // 必须先拿guard再drop
  auto current_guard = bpm_->ReadPage(root_id);
  head_guard.Drop();
  // 拿root
  auto current_page = current_guard.template As<BPlusTreePage>();
  while (!current_page->IsLeafPage()) {
    auto internal_page = current_guard.template As<InternalPage>();
    // std::cout<<"now inter_page"<<' '<<current_guard.GetPageId()<<'
    // '<<internal_page->KeyAt(1)<<'\n';
    page_id_t next_page_id = internal_page->ValueAt(0);
    auto next_guard = bpm_->ReadPage(next_page_id);
    // current_guard.Drop();
    current_guard = std::move(next_guard);
    current_page = current_guard.template As<BPlusTreePage>();
  }
  //
  // auto leaf_page = current_guard.template As<LeafPage>();
  // std::cout<<"Value 0"<<' '<<leaf_page->ValueAt(0)<<'\n';
  // std:: << "DEBUG_TRAVERSE: Reached Leaf Page " << current_guard.GetPageId()
  //           << " | Size: " << leaf_page->GetSize() << " | Content: [ ";
  // for(intcout i=0; i<leaf_page->GetSize(); i++) std::cout <<
  // leaf_page->KeyAt(i) <<' '<<leaf_page->ValueAt(i)<< "\n"; std::cout << "]"
  // << std::endl;
  //
  // 编译器get后获得的是TraceBufferPoolManager，而他是BufferPoolManager的子类，我们需要动手强转
  page_id_t page_id = current_guard.GetPageId();
  return INDEXITERATOR_TYPE((bpm_.get()), page_id, 0, std::move(current_guard));
}

/**
 * @brief Input parameter is low key, find the leaf page that contains the
 * input key first, then construct index iterator
 * @return : index iterator
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  // std::cout<<"HAS BEGIN------------------------\n";
  if (IsEmpty()) {
    return End();
  }
  // 经典操作，获取header
  auto head_guard = bpm_->ReadPage(header_page_id_);
  auto header_page = head_guard.template As<BPlusTreeHeaderPage>();
  page_id_t root_id = header_page->root_page_id_;
  if (root_id == INVALID_PAGE_ID) {
    head_guard.Drop();
    return INDEXITERATOR_TYPE(bpm_.get(), INVALID_PAGE_ID, -1, ReadPageGuard{});
  }
  auto current_guard = bpm_->ReadPage(root_id);
  head_guard.Drop();
  // 拿root
  auto current_page = current_guard.template As<BPlusTreePage>();
  while (!current_page->IsLeafPage()) {
    auto internal_page = current_guard.template As<InternalPage>();
    page_id_t next_page_id = internal_page->Lookup(key, comparator_);
    auto next_guard = bpm_->ReadPage(next_page_id);
    current_guard.Drop();
    current_guard = std::move(next_guard);
    current_page = current_guard.template As<BPlusTreePage>();
  }
  auto leaf_page = current_guard.template As<LeafPage>();

  int index = leaf_page->KeyIndex(key, comparator_);
  page_id_t page_id = current_guard.GetPageId();
  return INDEXITERATOR_TYPE((bpm_.get()), page_id, index,
                            std::move(current_guard));
}

/**
 * @brief Input parameter is void, construct an index iterator representing
 * the end of the key/value pair in the leaf node
 * @return : index iterator
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  return INDEXITERATOR_TYPE();
}

/**
 * @return Page id of the root of this tree
 *
 * You may want to implement this while implementing Task #3.
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  auto guard = bpm_->ReadPage(header_page_id_);
  auto head_page = guard.As<BPlusTreeHeaderPage>();
  return head_page->root_page_id_;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, 3>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, 2>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, 1>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, -1>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace bustub
