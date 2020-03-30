#include "db/version_edit_nvm.h"
#include "db/ram_table.h"


namespace leveldb{

void VersionEditNVM::Clear()
{

}


void VersionEditNVM::RemoveTable(int level, const RamTable* r)
{
    deleted_tables_.push_back(std::make_pair(level,  r));
}

void VersionEditNVM::AddTable(int level, const RamTable* r)
{
    new_tables_.push_back(std::make_pair(level, r));
}



}

