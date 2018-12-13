#include <list>

#include "hash/extendible_hash.h"
#include "page/page.h"

namespace cmudb
{

/*
 * constructor
 * array_size: fixed array size for each bucket
 */
template <typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash(size_t size)
{
  bucket_size_ = size;

  // Initalization
  num_bucket_ = 1;
  global_depth_ = 0;
  max_num_buckets_ = 1;

  buckets_.resize(max_num_buckets_);
  buckets_[0] = new Bucket();
}

template <typename K, typename V>
ExtendibleHash<K, V>::~ExtendibleHash()
{
  buckets_.clear();
}

/*
 * helper function to calculate the hashing address of input key
 */
template <typename K, typename V>
size_t ExtendibleHash<K, V>::HashKey(const K &key)
{
  size_t hash_key = std::hash<K>{}(key);

  size_t num_bit = global_depth_;
  // offset of the key in buckets array.
  size_t id = getFirstNBits(hash_key, num_bit);

  while (buckets_[id] == nullptr)
  {
    num_bit--;
    id = getFirstNBits(hash_key, num_bit);
  }

  return id;
}

/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetGlobalDepth() const
{
  return global_depth_;
}

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const
{
  return buckets_[bucket_id]->local_depth_;
}

/*
 * helper function to return current number of bucket in hash table
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const
{
  return num_bucket_;
}

/*
 * lookup function to find value associate with input key
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K &key, V &value)
{
  size_t id = HashKey(key);
  size_t hash_key = std::hash<K>{}(key);

  for (auto i : buckets_[id]->elements_)
  {
    if (i.first == hash_key)
    {
      value = i.second;
      return true;
    }
  }

  return false;
}

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K &key)
{
  // thread safety
  std::lock_guard<std::mutex> lock(latch_);

  size_t id = HashKey(key);
  size_t hash_key = std::hash<K>{}(key);

  // temporary vector
  std::vector<std::pair<size_t, V>> tmp;
  bool isDelete = 0;

  for (auto i : buckets_[id]->elements_)
  {
    if (i.first == hash_key)
    {
      isDelete = 1;
      continue;
    }
    tmp.push_back(i);
  }

  buckets_[id]->elements_ = tmp;
  return isDelete;
}

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
template <typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K &key, const V &value)
{
  // thread safety
  std::lock_guard<std::mutex> lock(latch_);

  size_t id = HashKey(key);
  size_t hash_key = std::hash<K>{}(key);

  buckets_[id]->elements_.push_back(std::pair<size_t, V>(hash_key, value));
  if (buckets_[id]->elements_.size() > bucket_size_)
  {
    split(id);
  }
}

// private functions

/*
 * get last N bits
 */
template <typename K, typename V>
size_t ExtendibleHash<K, V>::getFirstNBits(size_t value, size_t n)
{
  return value & ((1 << n) - 1);
}

/*
 * Split buckets[id] into buckets[id] and buckets[nxt]
 */
template <typename K, typename V>
void ExtendibleHash<K, V>::split(size_t id)
{
  num_bucket_++;
  size_t depth = buckets_[id]->local_depth_;
  std::vector<std::pair<size_t, V>> Total = buckets_[id]->elements_;

  //Extend the size of bucket array
  if (depth == global_depth_)
  {
    max_num_buckets_ <<= 1;
    buckets_.resize(max_num_buckets_);
  }

  size_t new_id = id | (1 << depth);
  buckets_[new_id] = new Bucket();

  // update local and global depth
  depth++;
  global_depth_ = std::max(global_depth_, depth);
  buckets_[id]->local_depth_ = depth;
  buckets_[new_id]->local_depth_ = depth;

  buckets_[id]->elements_.clear();

  // split each element into id or new_id bucket
  for (auto i : Total)
  {
    size_t offset = getFirstNBits(i.first, depth);
    buckets_[offset]->elements_.push_back(i);
  }

  if (buckets_[id]->elements_.size() > bucket_size_)
  {
    split(id);
  }
  if (buckets_[new_id]->elements_.size() > bucket_size_)
  {
    split(new_id);
  }
}

template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, std::list<Page *>::iterator>;

template class ExtendibleHash<size_t, std::pair<size_t, Page *>>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
template class ExtendibleHash<size_t, std::pair<size_t, int>>;
} // namespace cmudb
