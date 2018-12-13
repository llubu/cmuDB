/*
 * buffer_pool_manager.h
 *
 * Functionality: The simplified Buffer Manager interface allows a client to
 * new/delete pages on disk, to read a disk page into the buffer pool and pin
 * it, also to unpin a page in the buffer pool.
 */

#pragma once
#include <list>
#include <mutex>
#include <memory>

#include "buffer/lru_replacer.h"
#include "disk/disk_manager.h"
#include "hash/extendible_hash.h"
#include "page/page.h"

namespace cmudb
{
class BufferPoolManager
{
public:
  BufferPoolManager(size_t pool_size, const std::string &db_file);

  ~BufferPoolManager();

  Page *FetchPage(page_id_t page_id);

  bool UnpinPage(page_id_t page_id, bool is_dirty);

  bool FlushPage(page_id_t page_id);

  void FlushAllPages();

  Page *NewPage(page_id_t &page_id);

  bool DeletePage(page_id_t page_id);

private:
  size_t pool_size_;
  // array of pages
  Page *pages_;
  DiskManager disk_manager_;
  // to keep track of page id and its memory location
  HashTable<page_id_t, Page *> *page_table_;
  // to collect unpinned pages for replacement
  Replacer<Page *> *replacer_;
  // to collect free pages for replacement
  std::list<Page *> *free_list_;
  // to protect shared data structure, you may need it for synchronization
  // between replacer and page table
  std::mutex latch_;
};
} // namespace cmudb
