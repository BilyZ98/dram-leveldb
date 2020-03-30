#ifndef STORAGE_LEVELDB_INCLUDE_TABLE_BUILDER_NVM_H_
#define STORAGE_LEVELDB_INCLUDE_TABLE_BUILDER_NVM_H_

#include "leveldb/export.h"
#include "leveldb/status.h"

namespace leveldb{

class RamTable;

// table
class LEVELDB_EXPORT TableBuilderNVM{

public:

    TableBuilderNVM();

    TableBuilderNVM(const TableBuilderNVM&) = delete;
    TableBuilderNVM& operator=(const TableBuilderNVM&) = delete;

    // REQUIRES: Either Finish() or Abandon() has been called.
    ~TableBuilderNVM();


    void Add(const Slice& key, const Slice& value);

    Status Finish();

    void Abandon();

private:
    //RamTable* ram_table_;

};

};



#endif