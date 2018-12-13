/**
 * LRU implementation using extendible_hash and std::set
 */
#include "buffer/lru_replacer.h"
#include "page/page.h"
#include "common/logger.h"

namespace cmudb
{

template <typename T>
LRUReplacer<T>::LRUReplacer()
{
  hash_map_ = new ExtendibleHash<size_t, std::pair<size_t, T>>(5);
  LRU_ = new std::set<std::pair<size_t, size_t>>;
  time_count_ = 0;
}

template <typename T>
LRUReplacer<T>::~LRUReplacer()
{
  delete hash_map_;
  delete LRU_;
}

/*
 * Insert value into LRU
 */
template <typename T>
void LRUReplacer<T>::Insert(const T &value)
{
  std::lock_guard<std::mutex> lock(latch_);

  time_count_++;

  size_t hash_key = std::hash<T>{}(value);

  // timestamp - object
  std::pair<size_t, T> object;

  if (hash_map_->Find(hash_key, object))
  {
    LRU_->erase(LRU_->find(std::pair<size_t, size_t>(object.first, hash_key)));
    hash_map_->Remove(hash_key);
  }

  LRU_->insert(std::pair<size_t, size_t>(time_count_, hash_key));

  object.first = time_count_;
  object.second = value;
  hash_map_->Insert(hash_key, object);
}
/* If LRU is non-empty, pop the head member from LRU to argument "value", and
 * return true. If LRU is empty, return false
 */
template <typename T>
bool LRUReplacer<T>::Victim(T &value)
{
  std::lock_guard<std::mutex> lock(latch_);

  if (!LRU_->size())
    return false;

  // timestamp, hash_key
  std::pair<size_t, size_t> victim = *LRU_->begin();
  LRU_->erase(LRU_->begin());

  // timestamp - object
  std::pair<size_t, T> object;

  if (hash_map_->Find(victim.second, object))
  {
    hash_map_->Remove(victim.second);
  }
  else
  {
    LOG_ERROR("Error finding object in LRU_replacer::Victim()");
  }

  value = object.second;

  return true;
}

/*
 * Remove value from LRU. If removal is successful, return true, otherwise
 * return false
 */
template <typename T>
bool LRUReplacer<T>::Erase(const T &value)
{
  std::lock_guard<std::mutex> lock(latch_);

  size_t hash_key = std::hash<T>{}(value);

  // timestamp - object
  std::pair<size_t, T> object;

  if (hash_map_->Find(hash_key, object))
  {
    hash_map_->Remove(hash_key);
    LRU_->erase(LRU_->find(std::pair<size_t, size_t>(object.first, hash_key)));
    return true;
  }
  return false;
}

template <typename T>
size_t LRUReplacer<T>::Size()
{
  std::lock_guard<std::mutex> lock(latch_);

  return LRU_->size();
}

template class LRUReplacer<Page *>;
// test only
template class LRUReplacer<int>;

} // namespace cmudb
