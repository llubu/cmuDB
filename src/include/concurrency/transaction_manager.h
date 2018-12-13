/**
 * transaction_manager.h
 *
 */

#pragma once
#include "concurrency/lock_manager.h"

namespace cmudb
{
class TransactionManager
{
public:
  TransactionManager(LockManager *lock_manager) : lock_manager_(lock_manager) {}

  void Commit(Transaction *txn);
  void Abort(Transaction *txn);

private:
  LockManager *lock_manager_;
};

} // namespace cmudb
