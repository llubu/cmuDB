/**
 * lru_replacer.h
 *
 * Functionality: The buffer pool manager must maintain a LRU list to collect
 * all the pages that are unpinned and ready to be swapped. The simplest way to
 * implement LRU is a FIFO queue, but remember to dequeue or enqueue pages when
 * a page changes from unpinned to pinned, or vice-versa.
 */

#pragma once

#include "buffer/replacer.h"
#include "hash/extendible_hash.h"
#include <set>
#include <mutex>

namespace cmudb
{

template <typename T>
class LRUReplacer : public Replacer<T>
{
public:
  // do not change public interface
  LRUReplacer();

  ~LRUReplacer();

  void Insert(const T &value);

  bool Victim(T &value);

  bool Erase(const T &value);

  size_t Size();

private:
  // add your member variables here

  // hash_key, <time_stamp, object>
  HashTable<size_t, std::pair<size_t, T>> *hash_map_;
  // Store pair <timestamp, hash_key(object),>
  std::set<std::pair<size_t, size_t>> *LRU_;

  size_t time_count_;

  std::mutex latch_;
};

} // namespace cmudb
