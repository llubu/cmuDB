/**
 * disk_manager.cpp
 */
#include <cstring>
#include <iostream>
#include <sys/stat.h>

#include "common/logger.h"
#include "disk/disk_manager.h"

namespace cmudb
{

/**
 * Constructor: open/create a single database file
 * @input db_file: database file name
 */
DiskManager::DiskManager(const std::string &db_file)
    : file_name_(db_file), next_page_id_(0)
{
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open())
  {
    db_io_.clear();
    // create a new file
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  }
}

DiskManager::~DiskManager() { db_io_.close(); }

/**
 * Write the contents of the specified page into disk file
 */
void DiskManager::WritePage(page_id_t page_id, const char *page_data)
{
  size_t offset = page_id * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad())
  {
    LOG_DEBUG("I/O error while writing");
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}

/**
 * Read the contents of the specified page into the given memory area
 */
void DiskManager::ReadPage(page_id_t page_id, char *page_data)
{
  int offset = page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize())
  {
    LOG_DEBUG("I/O error while reading");
    // std::cerr << "I/O error while reading" << std::endl;
  }
  else
  {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE)
    {
      LOG_DEBUG("Read less than a page");
      // std::cerr << "Read less than a page" << std::endl;
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

/**
 * Allocate new page (operations like create index/table)
 * For now just keep an increasing counter
 */
page_id_t DiskManager::AllocatePage() { return next_page_id_++; }

/**
 * Deallocate page (operations like drop index/table)
 * Need bitmap in header page for tracking pages
 */
void DiskManager::DeallocatePage(__attribute__((unused)) page_id_t page_id)
{
  return;
}

/**
 * Private helper function to get disk file size
 */
int DiskManager::GetFileSize()
{
  struct stat stat_buf;
  int rc = stat(file_name_.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

} // namespace cmudb
