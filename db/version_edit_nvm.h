#ifndef STORAGE_LEVELDB_DB_VERSION_EDIT_NVM_H
#define STORAGE_LEVELDB_DB_VERSION_EDIT_NVM_H

#include <string>
#include <vector>
#include "db/dbformat.h"


namespace leveldb{

class VersionNVM;
class RamTable;
//class VersionSetNVM;

/*
struct TableMetaData{
    TableMetaData() : refs(0), table_size(0) {}
    int refs;
    uint64_t number;
    uint64_t file_size; 
    InternalKey smallest;
    InternalKey largest;

};
*/

class VersionEditNVM{
public:
    VersionEditNVM() { Clear();}
    ~VersionEditNVM() = default;

    void Clear();
    void SetComparatorName(const Slice& name) 
    {
      has_comparator_ = true;
      comparator_ = name.ToString();
    }

    void SetCompactPointer(int level, const InternalKey& key)
    {
      compact_pointers_.push_back(std::make_pair(level, key));
    }

    void RemoveTable(int level, const RamTable* r);

    void AddTable(int level, const RamTable* r);


private:
friend class VersionNVM;

std::string comparator_;
bool has_comparator_;
bool has_next_table_number_;

std::vector<std::pair<int, InternalKey>> compact_pointers_;
std::vector<std::pair<int, const RamTable*>> new_tables_;
std::vector<std::pair<int, const RamTable*>> deleted_tables_;

};
}

#endif