/**
 * index_iterator.cpp
 */
#include <cassert>

#include "index/index_iterator.h"

namespace cmudb
{

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(
    BufferPoolManager *buffer_pool_manager,
    B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page,
    int offset)
    : buffer_pool_manager_(buffer_pool_manager), leaf_page_(leaf_page), offset_(offset)
{
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator()
{
    Page *current_page = reinterpret_cast<Page *>(leaf_page_);
    current_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(current_page->GetPageId(), false);
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd()
{
    if (leaf_page_->GetNextPageId() == INVALID_PAGE_ID)
    {
        if (offset_ >= leaf_page_->GetSize())
            return true;
    }
    return false;
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*()
{
    return leaf_page_->GetItem(offset_);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++()
{
    offset_++;
    if (isEnd())
        return *this;
    if (offset_ == leaf_page_->GetSize())
    {
        Page *current_page = reinterpret_cast<Page *>(leaf_page_);
        Page *next_page = buffer_pool_manager_->FetchPage(leaf_page_->GetNextPageId());

        next_page->RLatch();
        offset_ = 0;
        current_page->RUnlatch();
        buffer_pool_manager_->UnpinPage(current_page->GetPageId(), false);
        leaf_page_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(next_page);
        return *this;
    }
    return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;
template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;
template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;
template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace cmudb
