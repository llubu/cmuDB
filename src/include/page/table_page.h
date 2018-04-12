/**
 * table_page.h
 *
 * Slotted page format:
 *  ---------------------------------------
 * | HEADER | ... FREE SPACES ... | TUPLES |
 *  ---------------------------------------
 *                                 ^
 *                         free space pointer
 *
 *  Header format (size in byte):
 *  ---------------------------------------------------------------------
 * | PageId (4) | PrevPageId (4) | NextPageId (4) | FreeSpacePointer (4) |
 *  ---------------------------------------------------------------------
 *  --------------------------------------------------------------
 * | TupleCount (4) | Tuple_1 offset (4) | Tuple_1 size (4) | ... |
 *  --------------------------------------------------------------
 *
 */

#pragma once

#include <cstring>

#include "common/rid.h"
#include "concurrency/lock_manager.h"
#include "page/page.h"
#include "table/tuple.h"

namespace cmudb {

class TablePage : public Page {
public:
  /**
   * Header related
   */
  void Init(page_id_t page_id, size_t page_size,
            page_id_t prev_page_id = INVALID_PAGE_ID,
            page_id_t next_page_id = INVALID_PAGE_ID);
  page_id_t GetPageId();

  page_id_t GetPrevPageId();
  page_id_t GetNextPageId();
  void SetPrevPageId(page_id_t prev_page_id);
  void SetNextPageId(page_id_t next_page_id);

  /**
   * Tuple related
   */
  bool InsertTuple(const Tuple &tuple, RID &rid, Transaction *txn,
                   LockManager *lock_manager); // return rid if success
  bool MarkDelete(const RID &rid, Transaction *txn,
                  LockManager *lock_manager); // delete
  bool UpdateTuple(const Tuple &new_tuple, Tuple &old_tuple, const RID &rid,
                   Transaction *txn, LockManager *lock_manager);

  // commit time
  void ApplyDelete(const RID &rid, Transaction *txn);    // when commit success
  void RollbackDelete(const RID &rid, Transaction *txn); // when commit abort

  // return tuple (with data pointing to heap) if success
  bool GetTuple(const RID &rid, Tuple &tuple, Transaction *txn,
                LockManager *lock_manager);

  /**
   * Tuple iterator
   */
  bool GetFirstTupleRid(RID &first_rid);
  bool GetNextTupleRid(const RID &cur_rid, RID &next_rid);

private:
  /**
   * helper functions
   */
  int32_t GetTupleOffset(int slot_num);
  int32_t GetTupleSize(int slot_num);
  void SetTupleOffset(int slot_num, int32_t offset);
  void SetTupleSize(int slot_num, int32_t offset);
  int32_t GetFreeSpacePointer(); // offset of the beginning of free space
  void SetFreeSpacePointer(int32_t free_space_pointer);
  int32_t GetTupleCount(); // Note that this tuple count may be larger than # of
                           // actual tuples because some slots may be empty
  void SetTupleCount(int32_t tuple_count);
  int32_t GetFreeSpaceSize();
};
} // namespace cmudb
