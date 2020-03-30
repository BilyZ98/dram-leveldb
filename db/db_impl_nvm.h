#ifndef STORAGE_LEVELDB_DB_IMPL_NVM_H
#define STORAGE_LEVELDB_DB_IMPL_NVM_H

#include <string>
#include <deque>
#include <atomic>
#include <set>

#include "leveldb/db.h"
#include "leveldb/env.h"


#include "db/dbformat.h"


#include "port/port.h"
//#include "port/port_stdcxx.h"
#include "port/thread_annotations.h"


namespace leveldb{

class MemTable;
class RamTable;
class VersionNVM;
class VersionEditNVM;


class DBImplNVM: public DB{
public:

  DBImplNVM(const Options& options, const std::string & dbname);

  DBImplNVM(const DBImplNVM&) = delete;
  DBImplNVM& operator=(const DBImplNVM&) = delete;

  ~DBImplNVM() override;

  // Implementations of the DB interface
  Status Put(const WriteOptions&, const Slice& key,
              const Slice& value) override;

  Status Get(const ReadOptions& options, const Slice& key,
              std::string* value) override;


  Status Delete(const WriteOptions&, const Slice& key) override;
  Status Write(const WriteOptions& options, WriteBatch* updates) override;

  Iterator* NewIterator(const ReadOptions&) override;


  const Snapshot* GetSnapshot() override;
  void ReleaseSnapshot(const Snapshot* snapshot) override;
  bool GetProperty(const Slice& property, std::string* value) override;
  void GetApproximateSizes(const Range* range, int n, uint64_t* sizes) override;
  void CompactRange(const Slice* begin, const Slice* end) override;


  // Extra methods (for testing) that are not in the public DB interface

  // Compact any files in the named level that overlap [*begin,*end]
  void TEST_CompactRange(int level, const Slice* begin, const Slice* end);


  // Force current memtable contents to be compacted.
  Status TEST_CompactMemTable();
private:

  friend class DB;
  struct CompactionState;

  struct Writer;

  // Information for a manual compaction
  struct ManualCompaction {
    int level;
    bool done;
    const InternalKey* begin;  // null means beginning of key range
    const InternalKey* end;    // null means end of key range
    InternalKey tmp_storage;   // Used to keep track of compaction progress
  };

  // Compact the in-memory write buffer to disk.  Switches to a new
  // log-file/memtable and writes a new descriptor iff successful.
  // Errors are recorded in bg_error_.
  void CompactMemTable() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  Status WriteLevel0Table(const RamTable* mem, VersionEditNVM* edit, VersionNVM* base)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  WriteBatch* BuildBatchGroup(Writer** last_writer);

  //Status WriteLevel0Table(MemTable* mem, VersionEdit* edit, VersionNVM* base)
  //EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status MakeRoomForWrite(bool force /* compact even if there is room? */)
  EXCLUSIVE_LOCKS_REQUIRED(mutex_);


  void MaybeScheduleCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  static void BGWork(void* db);
  void BackgroundCall();
  void BackgroundCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);


  Status DoCompactionWork(CompactionState* compact)
    EXCLUSIVE_LOCKS_REQUIRED(mutex_);


  // Constant after construction
  Env* const env_;

  std::string dbname_;

  const Options options_;  // options_.comparator == &internal_comparator_
  

  port::Mutex mutex_;
  port::CondVar background_work_finished_signal_ GUARDED_BY(mutex_);

  MemTable* mem_;
  MemTable* imm_ GUARDED_BY(mutex_);  // Memtable being compacted

  std::atomic<bool> has_imm_;         // So bg thread can detect non-null imm_

  // Queue of writers.
  std::deque<Writer*> writers_ GUARDED_BY(mutex_);
  WriteBatch* tmp_batch_ GUARDED_BY(mutex_); 


  //SnapshotList snapshots_ GUARDED_BY(mutex_);

  // Set of table files to protect from deletion because they are
  // part of ongoing compactions.
  std::set<uint64_t> pending_outputs_ GUARDED_BY(mutex_);

  // Has a background compaction been scheduled or is running?
  bool background_compaction_scheduled_ GUARDED_BY(mutex_);


  // Have we encountered a background error in paranoid mode?
  Status bg_error_ GUARDED_BY(mutex_);

  const InternalKeyComparator internal_comparator_;

  VersionNVM* version_ GUARDED_BY(mutex_);

  ManualCompaction* manual_compaction_ GUARDED_BY(mutex_);


  const Comparator* user_comparator() const {
    return internal_comparator_.user_comparator();
  }



  //CompactionStats stats_[config::kNumLevels] GUARDED_BY(mutex_);  

  Status InstallCompactionResults(CompactionState*);

  void RecordBackgroundError(const Status&);

};


}


#endif