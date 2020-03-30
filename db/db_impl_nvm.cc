

#include <string>

#include <iostream>

#include "db/memtable.h"
#include "db/db_impl_nvm.h"
#include "db/dbformat.h"
#include "db/write_batch_internal.h"
#include "db/ram_table.h"
#include "db/version_edit_nvm.h"

#include "leveldb/write_batch.h"
#include "leveldb/status.h"
#include "leveldb/table_builder_nvm.h"

#include "leveldb/env.h"

#include "port/port.h"


#include "db/version_NVM.h"

#include "util/mutexlock.h"


namespace leveldb{


struct DBImplNVM::CompactionState{

CompactionState(CompactionNVM* c)
:compaction(c)
{

}

std::vector<RamTable*> outputs;

CompactionNVM* const compaction;

RamTable* current_output(){ return outputs[outputs.size() - 1];}
//TableBuilderNVM* builder;

};

DBImplNVM::DBImplNVM(const Options& raw_options, const std::string & dbname)
    :   env_(raw_options.env),
        background_work_finished_signal_(&mutex_),
        mem_(nullptr),
        imm_(nullptr),
        has_imm_(false),
        background_compaction_scheduled_(false),
        dbname_(dbname),
        internal_comparator_(raw_options.comparator),
        tmp_batch_(new WriteBatch),
        manual_compaction_(nullptr),
        version_(new VersionNVM(&raw_options, &internal_comparator_))
        {}


DBImplNVM::~DBImplNVM(){
  mutex_.Lock();
  while (background_compaction_scheduled_) {
    background_work_finished_signal_.Wait();
  }

  mutex_.Unlock();

  version_->Clear();

  if (mem_ != nullptr) mem_->Unref();
  if (imm_ != nullptr) imm_->Unref();

  delete tmp_batch_;
}

//direvtly copy from db_impl.h
struct DBImplNVM::Writer{
    explicit Writer(port::Mutex* mu)
    : batch(nullptr), sync(false), done(false), cv(mu) {}

    Status status;
    WriteBatch* batch;
    bool sync;
    bool done;
    port::CondVar cv;
};


Status DBImplNVM::Put(const WriteOptions& o, const Slice& key, const Slice &val){
    return DB::Put(o, key, val);
}

Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice &val) {
    WriteBatch batch;
    batch.Put(key, val);
    return Write(opt, &batch);
}


// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-null batch
WriteBatch* DBImplNVM::BuildBatchGroup(Writer** last_writer) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  Writer* first = writers_.front();
  WriteBatch* result = first->batch;
  assert(result != nullptr);

  size_t size = WriteBatchInternal::ByteSize(first->batch);

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = 1 << 20;
  if (size <= (128 << 10)) {
    max_size = size + (128 << 10);
  }

  *last_writer = first;
  std::deque<Writer*>::iterator iter = writers_.begin();
  ++iter;  // Advance past "first"
  for (; iter != writers_.end(); ++iter) {
    Writer* w = *iter;
    if (w->sync && !first->sync) {
      // Do not include a sync write into a batch handled by a non-sync write.
      break;
    }

    if (w->batch != nullptr) {
      size += WriteBatchInternal::ByteSize(w->batch);
      if (size > max_size) {
        // Do not make batch too big
        break;
      }

      // Append to *result
      if (result == first->batch) {
        // Switch to temporary batch instead of disturbing caller's batch
        result = tmp_batch_;
        assert(WriteBatchInternal::Count(result) == 0);
        WriteBatchInternal::Append(result, first->batch);
      }
      WriteBatchInternal::Append(result, w->batch);
    }
    *last_writer = w;
  }
  return result;
}


Status DBImplNVM::Write(const WriteOptions& options, WriteBatch* updates) {
  Writer w(&mutex_);
  w.batch = updates;
  w.sync = options.sync;
  w.done = false;

  MutexLock l(&mutex_);
  writers_.push_back(&w);
  while (!w.done && &w != writers_.front()) {
    w.cv.Wait();
  }
  if (w.done) {
    return w.status;
  }

  // May temporarily unlock and wait.
  Status status = MakeRoomForWrite(updates == nullptr);
  uint64_t last_sequence = version_->LastSequence();
  Writer* last_writer = &w;
  if (status.ok() && updates != nullptr) {  // nullptr batch is for compactions
    WriteBatch* write_batch = BuildBatchGroup(&last_writer);
    WriteBatchInternal::SetSequence(write_batch, last_sequence + 1);
    last_sequence += WriteBatchInternal::Count(write_batch);

    // Add to log and apply to memtable.  We can release the lock
    // during this phase since &w is currently responsible for logging
    // and protects against concurrent loggers and concurrent writes
    // into mem_.
    {
      mutex_.Unlock();
      //status = log_->AddRecord(WriteBatchInternal::Contents(write_batch));

      if (status.ok()) {
        status = WriteBatchInternal::InsertInto(write_batch, mem_);
      }
      mutex_.Lock();

    }
    if (write_batch == tmp_batch_) tmp_batch_->Clear();

    version_->SetLastSequence(last_sequence);
  }

  while (true) {
    Writer* ready = writers_.front();
    writers_.pop_front();
    if (ready != &w) {
      ready->status = status;
      ready->done = true;
      ready->cv.Signal();
    }
    if (ready == last_writer) break;
  }

  // Notify new head of write queue
  if (!writers_.empty()) {
    writers_.front()->cv.Signal();
  }

  return status; 
}


Status DBImplNVM::MakeRoomForWrite(bool force) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  bool allow_delay = !force;
  Status s;
  while (true) {
    if (!bg_error_.ok()) {
      // Yield previous error
      s = bg_error_;
      break;
    } else if (!force &&
               (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size)) {
      // There is room in current memtable
      break;
    } else if (imm_ != nullptr) {
      // We have filled up the current memtable, but the previous
      // one is still being compacted, so we wait.
      //Log(options_.info_log, "Current memtable full; waiting...\n");
      background_work_finished_signal_.Wait();
    } else if (version_->NumLevelTables(0) >= config::kL0_StopWritesTrigger) {
      // There are too many level-0 files.
      //Log(options_.info_log, "Too many L0 files; waiting...\n");
      background_work_finished_signal_.Wait();
    } else {
      // Attempt to switch to a new memtable and trigger compaction of old

      imm_ = mem_;
      has_imm_.store(true, std::memory_order_release);
      mem_ = new MemTable(internal_comparator_);
      mem_->Ref();
      force = false;  // Do not force another compaction if have room
      MaybeScheduleCompaction();

/*
      for(int level = 0; level < config::kNumLevels; level++) {
        std::cout<< "level:" << level << "number:"<< version_->NumLevelTables(level) <<std::endl;
      }
      std::cout<< "------------------------------------------"<<std::endl;
*/
    }
  }
  return s;
}

void DBImplNVM::MaybeScheduleCompaction() {
  mutex_.AssertHeld();
  if (background_compaction_scheduled_) {
    // Already scheduled
  } else if (!bg_error_.ok()) {
    // Already got an error; no more changes
  } else if (imm_ == nullptr && manual_compaction_ == nullptr &&
             !version_->NeedsCompaction()) {
    // No work to be done
  } else {
    background_compaction_scheduled_ = true;
    env_->Schedule(&DBImplNVM::BGWork, this);
  }
}

void DBImplNVM::BGWork(void* db) {
  reinterpret_cast<DBImplNVM*>(db)->BackgroundCall();
}

void DBImplNVM::BackgroundCall() {
  MutexLock l(&mutex_);
  assert(background_compaction_scheduled_);
  if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else {
    BackgroundCompaction();
  }

  background_compaction_scheduled_ = false;

  // Previous compaction may have produced too many files in a level,
  // so reschedule another compaction if needed.
  MaybeScheduleCompaction();
  background_work_finished_signal_.SignalAll();
}

void DBImplNVM::BackgroundCompaction()
{
  mutex_.AssertHeld();
  if (imm_ != nullptr) {
    CompactMemTable();
    return;
  }

  CompactionNVM* c;
  
  bool is_manual = (manual_compaction_ != nullptr);
  InternalKey manual_end;
  if (is_manual) {
    
    ManualCompaction* m = manual_compaction_;
    c = version_->CompactRange(m->level, m->begin, m->end);
    m->done = (c == nullptr);
    if (c != nullptr) {
      manual_end = c->input(0, c->num_input_tables(0) - 1)->GetLargestInternalKey();
    }
    /*
    Log(options_.info_log,
        "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
        m->level, (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
        (m->end ? m->end->DebugString().c_str() : "(end)"),
        (m->done ? "(end)" : manual_end.DebugString().c_str()));
    */
    
  } else {
    c = version_->PickCompaction();
  }

  Status status;
  if (c == nullptr) {
    // Nothing to do
  } else if (!is_manual && c->IsTrivialMove()) {
    // Move file to next level
      // Move file to next level
    assert(c->num_input_tables(0) == 1);
    const RamTable* f = c->input(0, 0);
    c->edit()->RemoveTable(c->level(), f);
    c->edit()->AddTable(c->level() + 1, f);
    VersionNVM** new_version;
    status = version_->Apply(c->edit(), new_version, &mutex_);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    delete version_;
    version_ = nullptr;
    version_ = *new_version;


  } else {
    CompactionState* compact = new CompactionState(c);
    status = DoCompactionWork(compact);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    //CleanupCompaction(compact);
    //c->ReleaseInputs();
    //RemoveObsoleteFiles();
  }
  delete c;

  if (status.ok()) {
    // Done
  } else {
    //Log(options_.info_log, "Compaction error: %s", status.ToString().c_str());
  }

  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    if (!status.ok()) {
      m->done = true;
    }
    if (!m->done) {
      // We only compacted part of the requested range.  Update *m
      // to the range that is left to be compacted.
      m->tmp_storage = manual_end;
      m->begin = &m->tmp_storage;
    }
    manual_compaction_ = nullptr;
  }

}


void DBImplNVM::RecordBackgroundError(const Status& s) {
  mutex_.AssertHeld();
  if (bg_error_.ok()) {
    bg_error_ = s;
    background_work_finished_signal_.SignalAll();
  }
}

static void CheckIterator(Iterator* iter) {
  iter->SeekToFirst();
  while(iter->Valid()) {
    InternalKey ikey;
    ikey.DecodeFrom(iter->key());
    Slice user_key = ikey.user_key();
    Slice val = iter->value();

    assert(val.compare(user_key) == 0);

    iter->Next();
  }
}


void DBImplNVM::CompactMemTable(){
  mutex_.AssertHeld();
  assert(imm_ != nullptr);

  Iterator* imm_iter = imm_->NewIterator();
  RamTable * l0_table = new RamTable(imm_iter, &internal_comparator_);


  int new_table_number = version_->NewTableNumber();
  l0_table->SetTableNumber(new_table_number);
  delete imm_iter;

  VersionEditNVM edit;
  Status s = WriteLevel0Table(l0_table, &edit, version_);


  VersionNVM* new_version = nullptr;
  if(s.ok())
  {
    s = version_->Apply(&edit, &new_version, &mutex_);
    version_ = new_version;

  }

  if(s.ok())
  {

    //imm_->Unref();
    //delete imm_;
    imm_ = nullptr;
    has_imm_.store(false, std::memory_order_release);


  }
  else
  {
    abort();
  }
  

  //version_->
}

Status DBImplNVM::WriteLevel0Table(const RamTable* r, VersionEditNVM* edit,
                                  VersionNVM* base)
{
  mutex_.AssertHeld();
  //Iterator* imm_iter = mem->NewIterator();
  //RamTable * l0_table = new RamTable(imm_iter, &internal_comparator_);
  //l0_table->SetTableNumber(version_->NewTableNumber());
  //delete imm_iter;

  int level = 0;

  edit->AddTable(level, r);

  return Status::OK();
}                                  



void DBImplNVM::CompactRange(const Slice* begin, const Slice* end) {
  int max_level_with_files = 1;
  {
    MutexLock l(&mutex_);
    VersionNVM* base = version_;
    for (int level = 1; level < config::kNumLevels; level++) {
      if (base->OverlapInLevel(level, begin, end)) {
        max_level_with_files = level;
      }
    }
  }
  TEST_CompactMemTable();  // TODO(sanjay): Skip if memtable does not overlap
  for (int level = 0; level < max_level_with_files; level++) {
    TEST_CompactRange(level, begin, end);
  }
}

void DBImplNVM::TEST_CompactRange(int level, const Slice* begin,
                               const Slice* end) {
  assert(level >= 0);
  assert(level + 1 < config::kNumLevels);

  InternalKey begin_storage, end_storage;

  ManualCompaction manual;
  manual.level = level;
  manual.done = false;
  if (begin == nullptr) {
    manual.begin = nullptr;
  } else {
    begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
    manual.begin = &begin_storage;
  }
  if (end == nullptr) {
    manual.end = nullptr;
  } else {
    end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
    manual.end = &end_storage;
  }

  MutexLock l(&mutex_);
  while (!manual.done  &&
         bg_error_.ok()) {
    if (manual_compaction_ == nullptr) {  // Idle
      manual_compaction_ = &manual;
      MaybeScheduleCompaction();
    } else {  // Running either my compaction or another compaction.
      background_work_finished_signal_.Wait();
    }
  }
  if (manual_compaction_ == &manual) {
    // Cancel my manual compaction since we aborted early for some reason.
    manual_compaction_ = nullptr;
  }
}



Status DBImplNVM::DoCompactionWork(CompactionState* compact)
{
  const uint64_t start_micros = env_->NowMicros();
  int64_t imm_micros = 0;  // Micros spent doing imm_ compactions

  assert(version_->NumLevelTables(compact->compaction->level()) > 0);
  //assert(compact->builder == nullptr);
  //assert(compact->outTable == nullptr);

  Iterator* input = version_->MakeInputIterator(compact->compaction);


  mutex_.Unlock();

  input->SeekToFirst();
  Status status;
  ParsedInternalKey ikey;
  std::string current_user_key;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;

  int count = 0;

  while(input->Valid())
  {
    // Prioritize immutable compaction work
    if (has_imm_.load(std::memory_order_relaxed)) 
    {
      const uint64_t imm_start = env_->NowMicros();
      mutex_.Lock();
      if (imm_ != nullptr) {
        CompactMemTable();
        // Wake up MakeRoomForWrite() if necessary.
        background_work_finished_signal_.SignalAll();
      }
      mutex_.Unlock();
      imm_micros += (env_->NowMicros() - imm_start);
    }

    Slice key = input->key();

    bool drop = false;

    if(!ParseInternalKey(key, &ikey))
    {
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    }
    else
    {
      if(!has_current_user_key || 
      user_comparator()->Compare(ikey.user_key, Slice(current_user_key)) != 
        0)
      {
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());
        has_current_user_key = true;
        last_sequence_for_key = kMaxSequenceNumber;
      }

      if(ikey.type == kTypeDeletion &&
         compact->compaction->IsBaseLevelForKey(ikey.user_key))
      {
        drop = true;
      }

      last_sequence_for_key = ikey.sequence;
    }

    if(!drop) 
    {
      //
      
      if(compact->outputs.size() == 0 || compact->current_output()->Full()  )
      {
        RamTable* ram_table = new RamTable(&internal_comparator_);
        ram_table->SetTableNumber(version_->NewTableNumber());
        compact->outputs.push_back(ram_table);
      }

      RamTable* current_ram_table = compact->current_output();
      InternalKey internal_key;
      internal_key.DecodeFrom(key);
      current_ram_table->Append(internal_key, input->value());

    }

    InternalKey input_ikey;
    Slice val= input->value();;
    input_ikey.DecodeFrom(input->key());
    if(internal_comparator_.user_comparator()->Compare(input_ikey.user_key(), val) != 0) {
      int a = 5;
    }    

    input->Next();


  }



  delete input;
  input = nullptr;


  mutex_.Lock();

  /*
  std::string compaction_info = compact->compaction->CompactionInfo();
  std::cout << compaction_info << std::endl;
  */
  status = InstallCompactionResults(compact);

  if (!status.ok()) 
  {
    RecordBackgroundError(status);
  }
  
  // delete picked ramtables in compaction
  compact->compaction->DeleteObsoleteTables();
  return status;
  
}


Status DBImplNVM::InstallCompactionResults(CompactionState * compact)
{
  mutex_.AssertHeld();

  compact->compaction->AddInputDeletions(compact->compaction->edit());
  const int level = compact->compaction->level();
  for(int i =0; i < compact->outputs.size(); i++)
  {
    const RamTable* ramtable = compact->outputs[i];
    compact->compaction->edit()->AddTable(level+1, ramtable);
  }
  
  VersionNVM* new_version = nullptr;

  Status s = version_->Apply(compact->compaction->edit(),&new_version, &mutex_);
  if(s.ok())
  {
    delete version_;
    version_ = nullptr;
    version_ = new_version;
  }

  return s;
}

Status DBImplNVM::TEST_CompactMemTable()
{
  // nullptr batch means just wait for earlier writes to be done
  WriteBatch* w = nullptr;
  Status s = Write(WriteOptions(), w);
  if (s.ok()) {
    // Wait until the compaction completes
    MutexLock l(&mutex_);
    while (imm_ != nullptr && bg_error_.ok()) {
      background_work_finished_signal_.Wait();
    }
    if (imm_ != nullptr) {
      s = bg_error_; 
    }
  }
  return s;
}

Status DB::Open(const Options& options, const std::string& dbname, DB** dbptr)
{
  *dbptr = nullptr;
  DBImplNVM* dbimplnvm = new DBImplNVM(options, dbname);

  dbimplnvm->mutex_.Lock();

  if(dbimplnvm->mem_ == nullptr)
  {
    dbimplnvm->mem_ = new MemTable(dbimplnvm->internal_comparator_);
    dbimplnvm->mem_->Ref();
  }


  dbimplnvm->mutex_.Unlock();

  assert(dbimplnvm->mem_ != nullptr);
  *dbptr = dbimplnvm;

  return Status::OK(); 

}



DB::~DB()
{

}


const Snapshot* DBImplNVM::GetSnapshot() 
{
  return nullptr;
}
void DBImplNVM::ReleaseSnapshot(const Snapshot* snapshot) 
{

}
bool DBImplNVM::GetProperty(const Slice& property, std::string* value) 
{
  value->clear();
  MutexLock l(&mutex_);
  Slice in  = property;
  Slice prefix("leveldb.");
  if(!in.starts_with(prefix)) return false;
  in.remove_prefix(prefix.size());

  if(in.starts_with("num-tables-at-level")) {
    in.remove_prefix(strlen("num-tables-at-level"));

    int level = atoi(in.data());
    if(level < config::kNumLevels) {
      int num_tables = version_->NumLevelTables(level);
      char buf[100];
      snprintf(buf, sizeof(buf), "%d", num_tables);
      *value = buf;
      return true;
    } else {
      return false;
    }



  }
  return false;
}
void DBImplNVM::GetApproximateSizes(const Range* range, int n, uint64_t* sizes) 
{

}


Status DBImplNVM::Get(const ReadOptions& options, const Slice& key,
            std::string* value)
{
  mutex_.AssertHeld();
  Status s;
  MutexLock l(&mutex_);
  SequenceNumber seq;
  seq = version_->LastSequence();

  MemTable* mem = mem_;
  MemTable* imm = imm_;
  mem->Ref();
  if(imm != nullptr) imm->Ref();

  {
    while(background_compaction_scheduled_) {
      background_work_finished_signal_.Wait();
    }

    mutex_.Unlock();
    //mutex_.Lock();
    LookupKey lkey(key, seq);
    if(mem->Get(lkey, value, &s))
    {

    }
    else if(imm != nullptr && imm->Get(lkey, value, &s))
    {
      
    }
    else
    {
      s = version_->Get(options, lkey, value);
    }
    mutex_.Lock();
    
    
  }
  mem->Unref();
  if(imm != nullptr) imm->Unref();
  return s;
  
}


Status DBImplNVM::Delete(const WriteOptions& options, const Slice& key)
{
  return DB::Delete(options, key);
}
namespace {
struct IterState {
  port::Mutex* const mu;
  VersionNVM* const version GUARDED_BY(mu);
  MemTable* const mem GUARDED_BY(mu);
  MemTable* const imm GUARDED_BY(mu);

  IterState(port::Mutex* mutex, MemTable* mem, MemTable* imm, VersionNVM* version)
      : mu(mutex), version(version), mem(mem), imm(imm) {}
};

static void CleanupIteratorState(void* arg1, void* arg2) {
  IterState* state = reinterpret_cast<IterState*>(arg1);
  state->mu->Lock();
  state->mem->Unref();
  if (state->imm != nullptr) state->imm->Unref();
  //state->version->Unref();
  state->mu->Unlock();
  delete state;
}

}  // anonymous namespace

Iterator* DBImplNVM::NewIterator(const ReadOptions& r)
{
  mutex_.Lock();
  std::vector<Iterator*> list;
  list.push_back(mem_->NewIterator());
  mem_->Ref();
  if(imm_ != nullptr) {
    list.push_back(imm_->NewIterator());
    imm_->Ref();
  }
  version_->AddIterators(&list);
  Iterator* internal_iter = 
          new MergingIterator(&internal_comparator_, &list[0], list.size());


  IterState* cleanup = new IterState(&mutex_, mem_, imm_, version_);
  internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, nullptr);

  mutex_.Unlock();
  return internal_iter;
}

Status DB::Delete(const WriteOptions& opt, const Slice& key) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(opt, &batch);
}

}