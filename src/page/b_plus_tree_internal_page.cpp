/**
 * b_plus_tree_internal_page.cpp
 */
#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "page/b_plus_tree_internal_page.h"

namespace cmudb
{
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id,
                                          page_id_t parent_id)
{
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  // HEADER_PAGE_SIZE + max_size * sizeof(MappingType) < PAGE_SIZE
  int max_size = (PAGE_SIZE - 20) / sizeof(MappingType) - 1;
  SetMaxSize(max_size);
  array = new MappingType[max_size + 1];
  SetSize(0);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const
{
  assert(index >= 0 && index < GetMaxSize());
  return array[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key)
{
  assert(index >= 0 && index < GetMaxSize());
  array[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const
{
  for (int i = 0; i < GetSize(); ++i)
  {
    if (array[i].second == value)
      return i;
  }
  return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const
{
  assert(index >= 0 && index < GetMaxSize());
  return array[index].second;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType
B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key,
                                       const KeyComparator &comparator) const
{
  for (int i = 1; i < GetSize(); ++i)
  {
    if (comparator(key, array[i].first) < 0)
      return array[i - 1].second;
  }
  return array[GetSize() - 1].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value)
{
  SetSize(2);
  array[0] = MappingType(KeyType(), old_value);
  array[1] = MappingType(new_key, new_value);
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value)
{
  for (int i = 0; i < GetSize(); ++i)
  {
    if (array[i].second == old_value)
    {
      IncreaseSize(1);
      for (int j = i + 2; j < GetSize(); ++j)
      {
        array[j] = array[j - 1];
      }
      array[i + 1] = MappingType(new_key, new_value);
      break;
    }
  }
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * NOTE: current_page >> recipient_page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager)
{
  recipient->CopyHalfFrom(array + GetMinSize(), GetSize() - GetMinSize(), buffer_pool_manager);
  SetSize(GetMinSize());
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyHalfFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager)
{
  SetSize(size);
  for (int i = 0; i < size; i++)
  {
    array[i] = items[i];
    page_id_t page_id = array[i].second;
    Page *page = buffer_pool_manager->FetchPage(page_id);
    BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page);

    page->WLatch();
    node->SetParentPageId(GetPageId());
    page->WUnlatch();
    buffer_pool_manager->UnpinPage(page_id, true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index)
{
  assert(index >= 0 && index < GetSize());
  IncreaseSize(-1);
  for (int i = index; i < GetSize(); ++i)
    array[i] = array[i + 1];
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild()
{
  assert(GetSize() == 1);
  IncreaseSize(-1);
  return array[0].second;
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 * NOTE: recipient_page << current_page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(
    BPlusTreeInternalPage *recipient, int index_in_parent,
    BufferPoolManager *buffer_pool_manager)
{
  recipient->CopyAllFrom(array, GetSize(), buffer_pool_manager);
  SetSize(0);

  BPlusTreeInternalPage *parent;
  page_id_t parent_id = GetParentPageId();
  Page *page = buffer_pool_manager->FetchPage(parent_id);
  parent = reinterpret_cast<BPlusTreeInternalPage *>(page);

  parent->Remove(index_in_parent);
  buffer_pool_manager->UnpinPage(parent_id, true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyAllFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager)
{
  for (int i = 0; i < size; i++)
  {
    IncreaseSize(1);
    array[GetSize() - 1] = items[i];
    page_id_t page_id = items[i].second;
    Page *page = buffer_pool_manager->FetchPage(page_id);
    BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page);

    page->WLatch();
    node->SetParentPageId(GetPageId());
    page->WUnlatch();
    buffer_pool_manager->UnpinPage(page->GetPageId(), true);
  }
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient"
 * page, then update relavent key & value pair in its parent page.
 * NOTE: recipient_page << current_page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager)
{
  recipient->CopyLastFrom(array[0], buffer_pool_manager);
  Remove(0);

  BPlusTreeInternalPage *parent;
  page_id_t parent_id = GetParentPageId();
  Page *page = buffer_pool_manager->FetchPage(parent_id);

  parent = reinterpret_cast<BPlusTreeInternalPage *>(page);
  int index_in_parent = parent->ValueIndex(GetPageId());
  parent->SetKeyAt(index_in_parent, array[0].first);
  page->WUnlatch();
  buffer_pool_manager->UnpinPage(parent_id, true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(
    const MappingType &pair, BufferPoolManager *buffer_pool_manager)
{
  IncreaseSize(1);
  array[GetSize() - 1] = pair;
  page_id_t page_id = pair.second;
  Page *page = buffer_pool_manager->FetchPage(page_id);
  BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page);

  page->WLatch();
  node->SetParentPageId(GetPageId());
  page->WUnlatch();
  buffer_pool_manager->UnpinPage(page_id, true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient"
 * page, then update relavent key & value pair in its parent page.
 * NOTE: current_page >> recipient_page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(
    BPlusTreeInternalPage *recipient, int parent_index,
    BufferPoolManager *buffer_pool_manager)
{
  recipient->CopyFirstFrom(array[GetSize() - 1], parent_index, buffer_pool_manager);
  Remove(GetSize() - 1);

  BPlusTreeInternalPage *parent;
  page_id_t parent_id = GetParentPageId();
  Page *page = buffer_pool_manager->FetchPage(parent_id);

  parent = reinterpret_cast<BPlusTreeInternalPage *>(page);
  int index_in_parent = parent->ValueIndex(recipient->GetPageId());
  parent->SetKeyAt(index_in_parent, recipient->array[0].first);
  page->WUnlatch();
  buffer_pool_manager->UnpinPage(parent_id, true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(
    const MappingType &pair, int parent_index,
    BufferPoolManager *buffer_pool_manager)
{
  IncreaseSize(1);
  for (int i = GetSize() - 1; i > 0; i--)
    array[i] = array[i - 1];
  array[0] = pair;

  page_id_t page_id = pair.second;
  Page *page = buffer_pool_manager->FetchPage(page_id);
  BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page);

  page->WLatch();
  node->SetParentPageId(GetPageId());
  page->WUnlatch();
  buffer_pool_manager->UnpinPage(page_id, true);
}

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::QueueUpChildren(
    std::queue<BPlusTreePage *> *queue,
    BufferPoolManager *buffer_pool_manager)
{
  for (int i = 0; i < GetSize(); i++)
  {
    auto *page = buffer_pool_manager->FetchPage(array[i].second);
    if (page == nullptr)
      throw Exception(EXCEPTION_TYPE_INDEX,
                      "all page are pinned while printing");
    BPlusTreePage *node =
        reinterpret_cast<BPlusTreePage *>(page->GetData());
    queue->push(node);
  }
}

INDEX_TEMPLATE_ARGUMENTS
std::string B_PLUS_TREE_INTERNAL_PAGE_TYPE::ToString(bool verbose) const
{
  if (GetSize() == 0)
  {
    return "";
  }
  std::ostringstream os;
  if (verbose)
  {
    os << "[pageId: " << GetPageId() << " parentId: " << GetParentPageId()
       << "]<" << GetSize() << "> ";
  }

  int entry = verbose ? 0 : 1;
  int end = GetSize();
  bool first = true;
  while (entry < end)
  {
    if (first)
    {
      first = false;
    }
    else
    {
      os << " ";
    }
    os << std::dec << array[entry].first.ToString();
    if (verbose)
    {
      os << "(" << array[entry].second << ")";
    }
    ++entry;
  }
  return os.str();
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t,
                                     GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t,
                                     GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t,
                                     GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t,
                                     GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t,
                                     GenericComparator<64>>;
} // namespace cmudb
