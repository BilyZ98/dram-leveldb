#ifndef STORAGE_LEVELDB_INCLUDE_RAM_TABLE_H
#define STORAGE_LEVELDB_INCLUDE_RAM_TABLE_H

#include "db/dbformat.h"
#include "leveldb/export.h"
//#include "leveldb/iterator.h"


namespace leveldb{

class Iterator;
struct Options;

class RamTableIterator;

// A RamTable is just like a Table except the content it store
// is on the ram or nvm. A RamTable is mutable and persistent
// so when we modify the content of the RamTable we need sync
class LEVELDB_EXPORT RamTable{
public:
    //RamTable(const RamTable&) = delete;
    //RamTable& operator=(const RamTable&) = delete;

    RamTable(const InternalKeyComparator* comparator);

    // iterator of memtable to add entry of memtable 
    // to RamTable;
    RamTable(Iterator* iter, const InternalKeyComparator* comparator) ;

    ~RamTable();

    Iterator* NewIterator() const;

    // search value for a key
    // if found deleted or valid return true, deleted set status 
    // not found
    // if not found return false
    Status  Get(const Slice& key, void* arg, 
        void(*handle_result)(void*, const Slice&, const Slice&)) const ;



    //RamTable(int table_size);

    int GetTableNumber() const { return table_number_; }

    // should be set only when construct the object
    void SetTableNumber(int table_number){ table_number_ = table_number; }

    InternalKey GetSmallestInternalKey() const;

    InternalKey GetLargestInternalKey() const;


    // append an entry at the end of the table
    // REQUERIES: the key should be larger than the key at
    // the end of the table 
    bool Append(const InternalKey& ikey, const Slice& value);

    bool Full();



private:

friend class RamTableIterator;

//Memtable * const mtable_;

const InternalKeyComparator* comparator_;

// entry Node for k/v pair
struct Node;

struct RamRep;

Node* node_arr_;
// pos_ to indicate the last position of the 
// valid entry
int pos_;

InternalKey smallest_key_;
InternalKey largest_key_;

// table_size_ should be bigger than a memtable size
// and there should be some blank space for new node to be inserted.
int table_size_;

//int mem_entry_size_;

static const int Times_Factor_ = 1;

int NumMemTableEntry(Iterator* iter);

void InsertNodeAtPos(int pos, const Node& node);

// used to discriminate which table should be placed
// at first when two sstable have the same smallest
// key in the same level while compaction
// this number should be given by versionNVM
int table_number_;

/*
explicit RamTable(RamRep* ram_rep, Memtable* mtable): ram_rep_(ram_rep),
                                                     mtable_(mtable){}

Status InternalGet(const ReadOptions&, const Slice& key, void* arg,
                    void (*handle_result)(void* arg, const Slice& k,
                                            const Slice& v));
*/
//void ReadMeta(const Footer& footer);

//RamRep* const ram_rep_;



};

class RamTableIterator: public Iterator
{

public:
    RamTableIterator(const RamTable* ram_table);
    

    ~RamTableIterator() override = default;

    //if not found search key, might set pos = -1
    bool Valid() const override ;

    // slice k is the internal user key
    // because the key store in the ramtable is internal key
    // so we transform userkey to internal key
    // binary search the key in the node_arr_ array 
    // of RamTable that is the first key that is 
    // larger than the search key 
    void Seek(const Slice& k)   override;

    void SeekToFirst() override;

    void SeekToLast() override;

    void Next() override;

    void Prev() override;
    
    Slice key() const override;

    Slice value() const override;

    Status status() const override;
private:

int pos_;
//int table_size_;
//RamTable::Node* node_arr_;
const RamTable * ram_table_;
//bool valid_;
};


}


#endif