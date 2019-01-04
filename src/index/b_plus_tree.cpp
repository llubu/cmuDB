/**
 * b_plus_tree.cpp
 */
#include <iostream>
#include <string>
#include <deque>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "index/b_plus_tree.h"
#include "page/header_page.h"
#include "page/b_plus_tree_page.h"
#include "page/b_plus_tree_internal_page.h"
#include "page/b_plus_tree_leaf_page.h"

namespace cmudb
{

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(const std::string &name,
                          BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator,
                          page_id_t root_page_id)
    : index_name_(name),
      root_page_id_(root_page_id),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const
{
  return root_page_id_ == INVALID_PAGE_ID;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key,
                              std::vector<ValueType> &result,
                              Transaction *transaction)
{
  ValueType val;
  bool flag = false;
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page = FindLeafPage(key);

  if (leaf_page->Lookup(key, val, comparator_))
  {
    result.push_back(val);
    flag = true;
  }

  Page *page = reinterpret_cast<Page *>(leaf_page);
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);

  return flag;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value,
                            Transaction *transaction)
{
  if (IsEmpty())
  {
    StartNewTree(key, value);
    return true;
  }
  return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value)
{
  page_id_t page_id;
  Page *page = buffer_pool_manager_->NewPage(page_id);

  if (page == nullptr)
    throw std::bad_alloc();

  root_page_id_ = page_id;
  UpdateRootPageId();

  B_PLUS_TREE_LEAF_PAGE_TYPE *root = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page);

  root->Init(root_page_id_);
  root->Insert(key, value, comparator_);

  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value,
                                    Transaction *transaction)
{
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page = FindLeafPage(key, SearchType::Insert);
  Page *page = reinterpret_cast<Page *>(leaf_page);

  // Check if user insert duplicate keys
  ValueType val;
  if (leaf_page->Lookup(key, val, comparator_))
  {
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return false;
  }

  int new_size = leaf_page->Insert(key, value, comparator_);
  if (new_size > leaf_page->GetMaxSize())
  {
    int key_position = (leaf_page->GetMaxSize() + 1) / 2;
    InsertIntoParent(leaf_page,
                     leaf_page->KeyAt(key_position),
                     Split(leaf_page));
  }
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node)
{
  page_id_t new_page_id;
  Page *new_page = buffer_pool_manager_->NewPage(new_page_id);
  assert(new_page != nullptr);

  N *new_node = reinterpret_cast<N *>(new_page);
  new_node->Init(new_page_id, node->GetParentPageId());
  node->MoveHalfTo(new_node, buffer_pool_manager_);

  return new_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node,
                                      const KeyType &key,
                                      BPlusTreePage *new_node,
                                      Transaction *transaction)
{
  Page *page;
  B_PLUS_TREE_PARENT_PAGE_TYPE *parent;
  page_id_t parent_id = old_node->GetParentPageId();

  if (old_node->IsRootPage())
  {
    page = buffer_pool_manager_->NewPage(parent_id);
    assert(page != nullptr);

    root_page_id_ = parent_id;
    UpdateRootPageId();

    // Create new root
    parent = reinterpret_cast<B_PLUS_TREE_PARENT_PAGE_TYPE *>(page);
    parent->Init(parent_id);
    parent->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    buffer_pool_manager_->UnpinPage(parent_id, true);

    // Link old_node and new_node to new root
    page = reinterpret_cast<Page *>(new_node);
    new_node->SetParentPageId(parent_id);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);

    page = reinterpret_cast<Page *>(old_node);
    old_node->SetParentPageId(parent_id);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  }
  else
  {
    page = buffer_pool_manager_->FetchPage(parent_id);
    parent = reinterpret_cast<B_PLUS_TREE_PARENT_PAGE_TYPE *>(page);
    parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());

    if (parent->GetSize() > parent->GetMaxSize())
    {
      int key_position = (parent->GetMaxSize() + 1) / 2;
      InsertIntoParent(parent,
                       parent->KeyAt(key_position),
                       Split(parent));
    }

    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);

    page = reinterpret_cast<Page *>(old_node);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);

    page = reinterpret_cast<Page *>(new_node);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction)
{
  if (IsEmpty())
    return;

  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page = FindLeafPage(key, SearchType::Delete);
  int new_size = leaf_page->RemoveAndDeleteRecord(key, comparator_);

  if (new_size < leaf_page->GetMinSize())
    CoalesceOrRedistribute(leaf_page);

  Page *page = reinterpret_cast<Page *>(leaf_page);
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction)
{
  assert(node->GetSize() < node->GetMinSize());

  if (node->IsRootPage())
    return AdjustRoot(node);

  B_PLUS_TREE_PARENT_PAGE_TYPE *parent;
  Page *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  parent = reinterpret_cast<B_PLUS_TREE_PARENT_PAGE_TYPE *>(parent_page);

  assert(parent->GetSize() > 1);

  int node_id_in_parent = parent->ValueIndex(node->GetPageId());
  int sibling_id_in_parent = node_id_in_parent + 1;
  if (sibling_id_in_parent >= parent->GetSize())
    sibling_id_in_parent = node_id_in_parent - 1;

  Page *sibling_page = buffer_pool_manager_->FetchPage(
      parent->ValueAt(sibling_id_in_parent));

  assert(sibling_page != nullptr);

  N *sibling = reinterpret_cast<N *>(sibling_page);

  if (sibling->GetSize() + node->GetSize() > node->GetMaxSize())
  {
    if (sibling_id_in_parent > node_id_in_parent)
      Redistribute(sibling, node, 0);
    else
      Redistribute(sibling, node, 1);

    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);
    Page *page = reinterpret_cast<Page *>(node);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);

    return false;
  }
  if (sibling_id_in_parent < node_id_in_parent)
  {
    Coalesce(sibling, node, parent, INVALID_TXN_ID);
    return true;
  }
  else
  {
    Coalesce(node, sibling, parent, INVALID_TXN_ID);
    return false;
  }

} // namespace cmudb

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node" (in the left of node)
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(
    N *&neighbor_node, N *&node,
    B_PLUS_TREE_PARENT_PAGE_TYPE *&parent,
    int index, Transaction *transaction)
{
  node->MoveAllTo(neighbor_node, parent->ValueIndex(node->GetPageId()), buffer_pool_manager_);
  // Delete node
  Page *page = reinterpret_cast<Page *>(node);
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  buffer_pool_manager_->DeletePage(node->GetPageId());

  page = reinterpret_cast<Page *>(neighbor_node);
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);

  if (parent->GetSize() < parent->GetMinSize())
    return CoalesceOrRedistribute(parent);
  else
  {
    page = reinterpret_cast<Page *>(parent);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    return false;
  }
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index)
{
  if (index == 0)
    neighbor_node->MoveFirstToEndOf(node, buffer_pool_manager_);
  else
    neighbor_node->MoveLastToFrontOf(node, 0, buffer_pool_manager_);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node)
{
  if (old_root_node->GetSize() == 1)
  {
    if (old_root_node->IsLeafPage())
    {
      Page *page = reinterpret_cast<Page *>(old_root_node);
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);

      return false;
    }
    page_id_t new_root_id = reinterpret_cast<B_PLUS_TREE_PARENT_PAGE_TYPE *>(old_root_node)->ValueAt(0);
    root_page_id_ = new_root_id;
    UpdateRootPageId();

    // Update new root
    Page *page = buffer_pool_manager_->FetchPage(new_root_id);
    BPlusTreePage *new_root_node = reinterpret_cast<BPlusTreePage *>(page);
    new_root_node->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);

    // Delete old root
    page = reinterpret_cast<Page *>(old_root_node);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
    return true;
  }
  else if (old_root_node->GetSize() == 0)
  {
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId();

    // Delete old root
    Page *page;
    page = reinterpret_cast<Page *>(old_root_node);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    buffer_pool_manager_->DeletePage(page->GetPageId());

    return true;
  }
  else
  {
    Page *page = reinterpret_cast<Page *>(old_root_node);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    return false;
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin()
{
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page = FindLeafPage(KeyType(), SearchType::Find, true);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf_page, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key)
{
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page = FindLeafPage(key, SearchType::Find);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf_page, leaf_page->KeyIndex(key, comparator_));
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page, if wLatchAll == true, acquire WLatch on every page
 * in the path from root to that leaf, 
 * If release == true, release all WLatch from root to that node and return nullptr.
 */
INDEX_TEMPLATE_ARGUMENTS
B_PLUS_TREE_LEAF_PAGE_TYPE *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key,
                                                         SearchType option,
                                                         bool leftMost)
{
  Page *current_page = buffer_pool_manager_->FetchPage(root_page_id_);
  BPlusTreePage *current_node = reinterpret_cast<BPlusTreePage *>(current_page);

  while (!current_node->IsLeafPage())
  {
    page_id_t next_id;
    B_PLUS_TREE_PARENT_PAGE_TYPE *node;
    node = reinterpret_cast<B_PLUS_TREE_PARENT_PAGE_TYPE *>(current_page);

    if (leftMost)
      next_id = node->ValueAt(0);
    else
      next_id = node->Lookup(key, comparator_);

    buffer_pool_manager_->UnpinPage(current_page->GetPageId(), false);

    current_page = buffer_pool_manager_->FetchPage(next_id);
    current_node = reinterpret_cast<BPlusTreePage *>(current_page);
  }

  B_PLUS_TREE_LEAF_PAGE_TYPE *node;
  node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(current_page);
  return node;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record)
{
  HeaderPage *header_page = static_cast<HeaderPage *>(
      buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record)
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  else
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for debug only
 * print out whole b+tree sturcture, rank by rank
 */
INDEX_TEMPLATE_ARGUMENTS
std::string BPLUSTREE_TYPE::ToString(bool verbose) { return "Empty tree"; }

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name,
                                    Transaction *transaction)
{
  int64_t key;
  std::ifstream input(file_name);
  while (input)
  {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name,
                                    Transaction *transaction)
{
  int64_t key;
  std::ifstream input(file_name);
  while (input)
  {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace cmudb
