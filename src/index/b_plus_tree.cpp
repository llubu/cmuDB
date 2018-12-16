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
    : index_name_(name), root_page_id_(root_page_id),
      buffer_pool_manager_(buffer_pool_manager), comparator_(comparator) {}

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
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page = FindLeafPage(key);
  Page *page_ptr = reinterpret_cast<Page *>(leaf_page);

  bool flag = false;
  ValueType val;

  if (leaf_page->Lookup(key, val, comparator_))
  {
    result.push_back(val);
    flag = true;
  }
  page_ptr->RUnlatch();
  buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), false);
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
  Page *page = buffer_pool_manager_->NewPage(root_page_id_);

  if (!page)
  {
    // thow out of memory exception
    return;
  }
  page->WLatch();
  UpdateRootPageId();
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page);
  leaf_page->Init(root_page_id_);
  leaf_page->Insert(key, value, comparator_);
  page->WUnlatch();
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
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return false;
  }

  int new_size = leaf_page->Insert(key, value, comparator_);
  if (new_size > leaf_page->GetMaxSize())
  {
    InsertIntoParent(leaf_page,
                     leaf_page->KeyAt(leaf_page->GetMinSize()),
                     Split(leaf_page));
  }
  page->WUnlatch();
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

  N *new_node = reinterpret_cast<N *>(new_page);
  new_page->WLatch();
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
  BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent;
  page_id_t parent_id = old_node->GetParentPageId();
  Page *page;

  if (old_node->IsRootPage())
  {
    page = buffer_pool_manager_->NewPage(parent_id);
    page->WLatch();
    parent = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page);
    parent->Init(parent_id);
    parent->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    root_page_id_ = parent_id;
    UpdateRootPageId();
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(parent_id, true);

    page = reinterpret_cast<Page *>(new_node);
    new_node->SetParentPageId(parent_id);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);

    page = reinterpret_cast<Page *>(old_node);
    old_node->SetParentPageId(parent_id);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  }
  else
  {
    page = buffer_pool_manager_->FetchPage(parent_id);
    parent = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(page);

    // Insert into parent
    parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());

    if (parent->GetSize() > parent->GetMaxSize())
    {
      InsertIntoParent(parent,
                       parent->KeyAt(parent->GetMinSize()),
                       Split(parent));
    }
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);

    page = reinterpret_cast<Page *>(old_node);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    page = reinterpret_cast<Page *>(old_node);
    page->WUnlatch();
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
  Page *page = reinterpret_cast<Page *>(leaf_page);
  int new_size = leaf_page->RemoveAndDeleteRecord(key, comparator_);

  if (new_size < leaf_page->GetMinSize())
  {
    CoalesceOrRedistribute(leaf_page);
  }
  page->WUnlatch();
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
  BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent_node;
  Page *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  parent_node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(parent_page);

  int node_id = parent_node->ValueIndex(node->GetPageId());
  int sibling_id = node_id + 1;
  if (sibling_id >= parent_node->GetSize())
    sibling_id = node_id - 1;

  Page *sibling_page = buffer_pool_manager_->FetchPage(sibling_id);
  sibling_page->WLatch();
  N *sibling = reinterpret_cast<N *>(sibling_page);
  if (sibling->GetSize() + node->GetSize() > node->GetMaxSize())
  {
    if (sibling_id > node_id)
      Redistribute(sibling, node, 0);
    else
      Redistribute(sibling, node, 1);
    return false;
  }
  if (sibling_id < node_id)
    Coalesce(sibling, node, parent_node, INVALID_TXN_ID);
  else
    Coalesce(node, sibling, parent_node, INVALID_TXN_ID);
  return true;
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
    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *&parent,
    int index, Transaction *transaction)
{
  node->MoveAllTo(neighbor_node, 0, buffer_pool_manager_);
  Page *page = reinterpret_cast<Page *>(node);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  page = reinterpret_cast<Page *>(neighbor_node);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  buffer_pool_manager_->DeletePage(node->GetPageId());

  if (parent->IsRootPage())
  {
    return AdjustRoot(parent);
  }
  else
  {
    return CoalesceOrRedistribute(parent);
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
      return false;
    page_id_t new_root_id = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(old_root_node)->ValueAt(0);
    root_page_id_ = new_root_id;

    Page *page = buffer_pool_manager_->FetchPage(new_root_id);
    page->WLatch();
    BPlusTreePage *new_root_node = reinterpret_cast<BPlusTreePage *>(page);
    new_root_node->SetParentPageId(INVALID_PAGE_ID);
    page->WLatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);

    page = reinterpret_cast<Page *>(old_root_node);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
    UpdateRootPageId();
    return true;
  }
  else if (old_root_node->GetSize() == 0)
  {
    Page *page = reinterpret_cast<Page *>(old_root_node);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId();
    return true;
  }
  Page *page = reinterpret_cast<Page *>(old_root_node);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  return false;
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
 *  Check if a page is safe or not.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsSafe(BPlusTreePage *node, SearchType option)
{
  if (option == SearchType::Find)
    return true;
  if (option == SearchType::Insert)
    return node->GetSize() < node->GetMaxSize();
  // SearchType::Delete
  return node->GetSize() > node->GetMinSize();
}

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
  std::deque<Page *> parent_pages;
  Page *current_page = buffer_pool_manager_->FetchPage(root_page_id_);
  BPlusTreePage *current_node = reinterpret_cast<BPlusTreePage *>(current_page);
  while (!current_node->IsLeafPage())
  {
    if (option == SearchType::Find)
      current_page->RLatch();
    else
      current_page->WLatch();

    if (IsSafe(current_node, option))
    {
      while (!parent_pages.empty())
      {
        Page *p = parent_pages[0];
        parent_pages.pop_front();
        if (option == SearchType::Find)
          p->RUnlatch();
        else
          p->WUnlatch();

        buffer_pool_manager_->UnpinPage(p->GetPageId(), false);
      }
    }
    parent_pages.push_back(current_page);
    // find children
    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *node;
    node = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(current_page);
    page_id_t next_id;
    if (leftMost)
      next_id = node->ValueAt(0);
    else
      next_id = node->Lookup(key, comparator_);

    current_page = buffer_pool_manager_->FetchPage(next_id);
    current_node = reinterpret_cast<BPlusTreePage *>(current_page);
  }
  if (option == SearchType::Find)
    current_page->RLatch();
  else
    current_page->WLatch();
  if (IsSafe(current_node, option))
  {
    while (!parent_pages.empty())
    {
      Page *p = parent_pages[0];
      parent_pages.pop_front();
      if (option == SearchType::Find)
        p->RUnlatch();
      else
        p->WUnlatch();

      buffer_pool_manager_->UnpinPage(p->GetPageId(), false);
    }
  }
  for (auto p : parent_pages)
    buffer_pool_manager_->UnpinPage(p->GetPageId(), false);
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
