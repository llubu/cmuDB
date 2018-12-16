#include "buffer/buffer_pool_manager.h"

namespace cmudb
{

/*
 * BufferPoolManager Constructor
 * WARNING: Do Not Edit This Function
 */
BufferPoolManager::BufferPoolManager(size_t pool_size,
                                     const std::string &db_file)
    : pool_size_(pool_size), disk_manager_{db_file}
{
  // a consecutive memory space for buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHash<page_id_t, Page *>(100);
  replacer_ = new LRUReplacer<Page *>;
  free_list_ = new std::list<Page *>;

  // put all the pages into free list
  for (size_t i = 0; i < pool_size_; ++i)
  {
    free_list_->push_back(&pages_[i]);
  }
}

/*
 * BufferPoolManager Deconstructor
 * WARNING: Do Not Edit This Function
 */
BufferPoolManager::~BufferPoolManager()
{
  FlushAllPages();
  delete[] pages_;
  delete page_table_;
  delete replacer_;
  delete free_list_;
}

/**
 * 1. search hash table.
 *  1.1 if exist, pin the page and return immediately
 *  1.2 if no exist, find a replacement entry from either free list or lru
 *      replacer. (NOTE: always find from free list first)
 * 2. If the entry chosen for replacement is dirty, write it back to disk.
 * 3. Delete the entry for the old page from the hash table and insert an entry
 * for the new page.
 * 4. Update page metadata, read page content from disk file and return page
 * pointer
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id)
{
  std::unique_lock<std::mutex> lock(latch_);
  // temporary page
  Page *tmp_page = nullptr;
  if (page_table_->Find(page_id, tmp_page))
  {
    tmp_page->pin_count_++;
    return tmp_page;
  }
  // find replacement entry
  if (free_list_->size())
  {
    tmp_page = free_list_->back();
    free_list_->pop_back();
  }
  else if (!(replacer_->Victim(tmp_page)))
  {
    return nullptr;
  }
  if (tmp_page->is_dirty_)
  {
    FlushPage(tmp_page->GetPageId());
  }
  page_table_->Remove(page_id);
  tmp_page = new Page();
  tmp_page->page_id_ = page_id;
  tmp_page->pin_count_++;

  tmp_page->WLatch();
  disk_manager_.ReadPage(tmp_page->GetPageId(), tmp_page->GetData());
  tmp_page->WUnlatch();

  return tmp_page;
}

/*
 * Implementation of unpin page
 * if pin_count>0, decrement it and if it becomes zero, put it back to replacer
 * if pin_count<=0 before this call, return false.
 * is_dirty: set the dirty flag of this page
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty)
{
  std::lock_guard<std::mutex> lock(latch_);
  Page *tmp_page = nullptr;
  if (page_table_->Find(page_id, tmp_page))
  {
    if (tmp_page->pin_count_ > 0)
    {
      tmp_page->pin_count_--;
      if (tmp_page->GetPinCount() == 0)
      {
        replacer_->Insert(tmp_page);
      }
      tmp_page->is_dirty_ |= is_dirty;
      return true;
    }
    else
    {
      return false;
    }
  }
  return false;
}

/*
 * Used to flush a particular page of the buffer pool to disk. Should call the
 * write_page method of the disk manager
 * if page is not found in page table, return false
 * NOTE: make sure page_id != INVALID_PAGE_ID
 */
bool BufferPoolManager::FlushPage(page_id_t page_id)
{
  if (page_id == INVALID_PAGE_ID)
    return false;

  Page *tmp_page = nullptr;
  if (page_table_->Find(page_id, tmp_page))
  {
    tmp_page->WLatch();
    disk_manager_.WritePage(tmp_page->GetPageId(), tmp_page->GetData());
    tmp_page->is_dirty_ = false;
    tmp_page->WUnlatch();
    return true;
  }
  return false;
}

/*
 * Used to flush all dirty pages in the buffer pool manager
 */
void BufferPoolManager::FlushAllPages()
{
  for (size_t i = 0; i < pool_size_; ++i)
    if (pages_[i].is_dirty_)
      FlushPage(pages_[i].GetPageId());
}

/**
 * User should call this method for deleting a page. This routine will call disk
 * manager to deallocate the page.
 * First, if page is found within page table, buffer pool manager should be
 * reponsible for removing this entry out of page table, reseting page metadata
 * and adding back to free list. Second, call disk manager's DeallocatePage()
 * method to delete from disk file.
 * If the page is found within page table, but pin_count != 0, return false
 */
bool BufferPoolManager::DeletePage(page_id_t page_id)
{
  std::lock_guard<std::mutex> lock(latch_);

  Page *tmp_page = nullptr;
  if (page_table_->Find(page_id, tmp_page))
  {
    if (tmp_page->GetPinCount())
      return false;

    page_table_->Remove(page_id);
    replacer_->Erase(tmp_page);
  }
  disk_manager_.DeallocatePage(page_id);
  return true;
}

/**
 * User should call this method if needs to create a new page. This routine
 * will call disk manager to allocate a page.
 * Buffer pool manager should be responsible to choose a victim page either from
 * free list or lru replacer(NOTE: always choose from free list first), update
 * new page's metadata, zero out memory and add corresponding entry into page
 * table.
 * return nullptr is all the pages in pool are pinned
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id)
{
  std::lock_guard<std::mutex> lock(latch_);
  // temporary page
  Page *tmp_page = nullptr;
  // find a free entry in free_list
  if (free_list_->size())
  {
    tmp_page = free_list_->back();
    free_list_->pop_back();
  }
  else if (!(replacer_->Victim(tmp_page)))
  {
    return nullptr;
  }

  if (tmp_page->is_dirty_)
    FlushPage(tmp_page->GetPageId());

  // update new page's metadata
  tmp_page = new Page;
  page_id = disk_manager_.AllocatePage();
  tmp_page->page_id_ = page_id;
  tmp_page->pin_count_++;

  page_table_->Insert(page_id, tmp_page);
  return tmp_page;
}

} // namespace cmudb
