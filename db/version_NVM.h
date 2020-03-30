#ifndef STORAGE_LEVELDB_DB_VERSION_NVM_H
#define STORAGE_LEVELDB_DB_VERSION_NVM_H

#include <vector>

#include "db/dbformat.h"
#include "db/ram_table.h"
#include "db/version_edit_nvm.h"


namespace leveldb{

class RamTable;
class CompactionNVM;
class Iterator;
//class VersionEditNVM;
// versionNVM stores the whole sstables that store in nvm
// and it provides functionality of get, compaction sstables


int FindTable(const InternalKeyComparator& icmp,const std::vector<RamTable*>&  table_list, const Slice& target);
class VersionNVM{

public:

    VersionNVM(const Options *options, const InternalKeyComparator*);

    VersionNVM(const VersionNVM&);

    ~VersionNVM();

    void Clear();

    //Status Get(const ReadOptions&, const LookupKey& key, std::string* val,GetStats* stats);

    void ForEachOverlapping(Slice user_key, Slice internal_key, void* arg,
                                 bool (*func)(void*, int, const RamTable*));

    /*
    void Ref();
    void UnRef();
    */

    //int NumTables(int level) const{}

    bool OverlapInLevel(int level, const Slice* smallest_user_key, const Slice* largest_user_key);

    int NumLevelTables(int level) const {return tables_[level].size(); }

    //use to set sequence number for each write batch
    void SetLastSequence(uint64_t seq) {
        assert(seq >= last_sequence_);
        last_sequence_ = seq;
    }

    uint64_t LastSequence() const{  return last_sequence_; }


    Iterator* MakeInputIterator(CompactionNVM* compaction);

    CompactionNVM* PickCompaction();

    CompactionNVM* CompactRange(int level, const InternalKey* begin,
                                     const InternalKey* end);

    void GetOverlappingInputs(
    int level,
    const InternalKey* begin,  // nullptr means before all keys
    const InternalKey* end,    // nullptr means after all keys
    std::vector<const RamTable*>* inputs);

    bool NeedsCompaction() { 
        return compaction_score_ >= 1 || table_to_compact != nullptr;
    }

    Status Apply(VersionEditNVM* edit, VersionNVM** v,port::Mutex* mu);

    // this can only be called once
    // to specify the ompaction score
    void Finalize();

    Status Get(const ReadOptions& options, const LookupKey& k,
                std::string* value);

    int NewTableNumber() { return next_table_number_++; }

    void AddIterators(std::vector<Iterator*> * list);

    Iterator* GetIteratorByTableNumber(int tablenumber);

private:
    friend class CompactionNVM;

    class BuilderNVM;

    void GetRange(const std::vector<const RamTable*>& inputs, InternalKey* smallest,
                InternalKey* largest);

    void GetRange2(const std::vector<const RamTable*>& inputs1,
                 const std::vector<const RamTable*>& inputs2,
                 InternalKey* smallest, InternalKey* largest);
    
    void SetupOtherInputs(CompactionNVM* c);

    void Finalize(VersionNVM* v);

    const InternalKeyComparator icmp_;

    int next_table_number_;

    //int refs_;

    RamTable* table_to_compact;
    int table_to_compact_level;

    // all sstables
    std::vector<const RamTable*> tables_[config::kNumLevels];

    //int compaction_level_;

    uint64_t last_sequence_;

    // Level that should be compacted next and its compaction score.
    // Score < 1 means compaction is not strictly needed.  These fields
    // are initialized by Finalize().
    double compaction_score_;
    int compaction_level_;
    
    const Options* const options_;

    // Per-level key at which the next compaction at that level should start.
    // Either an empty string, or a valid InternalKey.
    std::string compact_pointer_[config::kNumLevels];

    Env* const env_;
};


class CompactionNVM{
public:

    //CompactionNVM();

    ~CompactionNVM();

    bool IsBaseLevelForKey(const Slice& user_key);

    bool IsTrivialMove();

    //VersionEditNVM * edit() { return &edit_;}

    int level() const {return level_;}

    //int num_input_tables(int which) const { return inputs_[which].size(); }

    VersionEditNVM* edit() { return &edit_;}

    // Add all inputs to this compaction as delete operations to *edit.
    void AddInputDeletions(VersionEditNVM* edit);

    void DeleteObsoleteTables();

    const RamTable* input(int which, int i){ return inputs_[which][i]; }
    
    // "which" must be either 0 or 1
    int num_input_tables(int which) const { return inputs_[which].size(); }

    std::string CompactionInfo() ;
private:

    friend class VersionNVM;

    CompactionNVM(const Options* options, int level);

        
    VersionNVM* input_version_;
    VersionEditNVM edit_;
    int level_;
    std::vector<const RamTable *> inputs_[2];

    size_t level_ptrs_[config::kNumLevels];

//VersionNVM* edit_;  
};


// Iterator for level that is not in 0
// concatenate all iterators of sstables together
// key is the ley 
// value is the value 
// first we need to find the sstable
// then we need to find the k/v pair in the table

class LevelConcatIterator: public Iterator
{
public:

    LevelConcatIterator(const InternalKeyComparator& icmp, 
    const std::vector<const RamTable*> *table_list);

    bool Valid() const override;

    // first locate the sstable
    // then using iterator to find the k/v pair

    // first call current iterator's next()
    // if invalid 
    // then we move the index into next sstable
    // we assume that we at least have one sstble search
    /*
        Param: 
            target slice, user key 
    */

    void Seek(const Slice& target) override;

    void SeekToFirst() override;

    void SeekToLast() override;

    void Next() override;
 

    void Prev() override ;

    Slice key() const override;

    Slice value() const override;

    Status status() const override;



private:
    const InternalKeyComparator icmp_;

    // table_list_ vector is sorted 
    const std::vector<const RamTable*>* const table_list_;
    std::vector<Iterator*> ram_table_iterators;      
    int index_;

    //this can only be called when the object is initialized;
    void  CreateTableIterators() 
    {
        for(int i=0; i<table_list_->size(); i++)
        {
            ram_table_iterators.push_back((*table_list_)[i]->NewIterator());
        }
    }


};

class MergingIterator: public Iterator
{

public:
    MergingIterator(const InternalKeyComparator* icomp, Iterator** list, int n);

    bool Valid() const override;
    void Seek(const Slice& target) override;

    void SeekToFirst() override;

    void SeekToLast() override;

    Slice key() const override;

    Slice value() const  override;

    void Next() override;

    void Prev() override;

    Status status() const override;



private:

enum Direction{kForward, kReverse};



Iterator** child_;
int n_;
const InternalKeyComparator* icmp_;
Iterator* current_;


void FindSmallest();


};



}

#endif