/*
 * extendible_hash.h : implementation of in-memory hash table using extendible
 * hashing
 *
 * Functionality: The buffer pool manager must maintain a page table to be able
 * to quickly map a PageId to its corresponding memory location; or alternately
 * report that the PageId does not match any currently-buffered page.
 */

#pragma once

#include <cstdlib>
#include <vector>
#include <string>
#include <iostream>
#include <mutex>

#include "hash/hash_table.h"

namespace cmudb
{

template <typename K, typename V>
class ExtendibleHash : public HashTable<K, V>
{
public:
  // constructor
  ExtendibleHash(size_t size);
  //destructor
  ~ExtendibleHash();

  // helper function to generate hash addressing
  size_t HashKey(const K &key);
  // helper function to get global & local depth
  int GetGlobalDepth() const;
  int GetLocalDepth(int bucket_id) const;
  int GetNumBuckets() const;
  // lookup and modifier
  bool Find(const K &key, V &value) override;
  bool Remove(const K &key) override;
  void Insert(const K &key, const V &value) override;

private:
  // add your own member variables here

  struct Bucket
  {
    bool isCreated;
    size_t local_depth_;
    // list of elements in the current bucket.
    std::vector<std::pair<size_t, V>> elements_;

    Bucket()
    {
      local_depth_ = 0;
      elements_.clear();
    }
  };

  // list of buckets
  std::vector<Bucket *> buckets_;

  size_t global_depth_;
  size_t num_bucket_;
  size_t bucket_size_;
  size_t max_num_buckets_;
  std::mutex latch_;

  // helper functions
  void split(size_t id);
  size_t getFirstNBits(size_t value, size_t n);
};

} // namespace cmudb
