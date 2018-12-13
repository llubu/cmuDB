/**
 * disk_manager.h
 *
 * Disk manager takes care of the allocation and deallocation of pages within a
 * database. It also performs read and write of pages to and from disk, and
 * provides a logical file layer within the context of a database management
 * system.
 */

#pragma once
#include <atomic>
#include <fstream>
#include <string>

#include "common/config.h"

namespace cmudb
{

class DiskManager
{
public:
  DiskManager(const std::string &db_file);
  ~DiskManager();

  void WritePage(page_id_t page_id, const char *page_data);
  void ReadPage(page_id_t page_id, char *page_data);

  page_id_t AllocatePage();
  void DeallocatePage(page_id_t page_id);

private:
  int GetFileSize();
  std::fstream db_io_;
  std::string file_name_;
  std::atomic<page_id_t> next_page_id_;
};

} // namespace cmudb
