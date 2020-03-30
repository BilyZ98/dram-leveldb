
#include <string>
#include <iostream>
#include <algorithm>
#include <set>
#include <vector>

#include "db/version_NVM.h"
#include "db/version_edit_nvm.h"
#include "db/memtable.h"
#include "db/dbformat.h"
#include "db/ram_table.h"


#include "leveldb/iterator.h"
#include "leveldb/env.h"
#include "table/merger.h"
#include "port/port.h"

namespace leveldb{

static int MaxNumTablesForLevel(int level)
{
    int level_max_size = config::kL0_CompactionTrigger;

    while(level >0)
    {
        level_max_size*=10;
        level--;
    }
    return level_max_size;

}

int FindTable(const InternalKeyComparator& icmp,const std::vector<const RamTable*>&  table_list, const Slice& target)
{
    int left = 0;
    int right = table_list.size();
    while(left < right)
    {
        int mid = (left + right) / 2;
        const RamTable* ram_table = table_list[mid];
        // maybe icmp.user_comparator()->
        if(icmp.Compare(ram_table->GetLargestInternalKey().Encode(), target) < 0)
        {
            left = mid + 1;
        }
        else
        {
            right = mid;
        }
        

    }
    return right;
}

/*
void VersionNVM::Ref(){
    refs_++;
}

void VersionNVM::UnRef(){
    assert(refs_ >=1);    
}

*/


CompactionNVM::CompactionNVM(const Options* options, int level)
    : level_(level),
      input_version_(nullptr)
      
{
    for(int i =0; i< config::kNumLevels; i++)
    {
        level_ptrs_[i] = 0;
    }
}


void CompactionNVM::AddInputDeletions(VersionEditNVM* edit)
{
  for (int which = 0; which < 2; which++) {
    for (size_t i = 0; i < inputs_[which].size(); i++) {
      //edit->RemoveFile(level_ + which, inputs_[which][i]->number);
      edit->RemoveTable(level_ + which, inputs_[which][i]);
    }
  }
}

void CompactionNVM::DeleteObsoleteTables()
{
    for(int which=0; which < 2; which++)
    {
        for(std::vector<const RamTable*>::iterator it = inputs_[which].begin(); 
                it !=inputs_[which].end(); it++)
        {
            delete *it;
        }
    }
}

/*
Status VersionNVM::Get(const ReadOptions& options, const LookupKey& k,
                    std::string* value, GetStats* stats)
{

}
*/


static bool NewestFirst(const RamTable* a, const RamTable* b) {
  return a->GetTableNumber() > b->GetTableNumber();
}

void VersionNVM::ForEachOverlapping(Slice user_key, Slice internal_key, void* arg,
                                 bool (*func)(void*, int,const  RamTable*))
{
    const Comparator* ucmp = icmp_.user_comparator();
    
    // search level-0in order from newest to oldest
    std::vector<const RamTable*> tmp;
    tmp.reserve(tables_[0].size());
    for(int i =0 ; i < tables_[0].size(); i ++)
    {
        const RamTable* r = tables_[0][i];
        if (ucmp->Compare(user_key, r->GetSmallestInternalKey().user_key()) >= 0 &&
            ucmp->Compare(user_key, r->GetLargestInternalKey().user_key()) <= 0)
        {
            tmp.push_back(r);
        }
    }
    if(!tmp.empty())
    {
        std::sort(tmp.begin(), tmp.end(), NewestFirst);
        for(int i=0; i <tmp.size(); i++)
        {
            if(!(*func)(arg, 0, tmp[i]))
            {
                return;
            }
        }
    }

    // search other level
    for(int level = 1; level < config::kNumLevels; level++){
        int num_tables = tables_[level].size();
        if(num_tables == 0) continue;

        // binary seach to find earliest index whose largest key >= internal_key
        int index = FindTable(icmp_, tables_[level], internal_key);
        if(index < num_tables){
            const RamTable* r = tables_[level][index];
            if(ucmp->Compare(user_key, r->GetSmallestInternalKey().user_key()) < 0){

            } else {
                if(!(*func)(arg, level, r)){
                    return ;
                }
            }

        }
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




CompactionNVM* VersionNVM::PickCompaction()
{
    CompactionNVM* c;
    int level;

    bool size_compaction = (compaction_score_ >= 1);
    //bool seek_compaction = ()
    if(size_compaction)
    {
        level = compaction_level_;
        assert(level >= 0);
        assert(level + 1 <config::kNumLevels);

        c = new CompactionNVM(options_,level);
        for(int i =0; i <tables_[level].size(); i++ )
        {
            const RamTable* ram_table = tables_[level][i];
            if(compact_pointer_[level].empty() ||
                icmp_.Compare(ram_table->GetLargestInternalKey().Encode(), compact_pointer_[level] ) > 0)
            {
                c->inputs_[0].push_back(ram_table);
                break;
            }

        }

        if(c->inputs_[0].empty())
        {
            c->inputs_[0].push_back(tables_[level][0]);
        }
    }
    else
    {
        return nullptr;
    }


    c->input_version_ = this;
    if (level == 0)
    {
        InternalKey smallest, largest;
        GetRange(c->inputs_[0], &smallest, &largest);
        GetOverlappingInputs(0, &smallest, &largest,&c->inputs_[0]);
        int overlapping_table_num = c->inputs_[0].size();
        assert(!c->inputs_[0].empty());

    }


    SetupOtherInputs(c);
    return c;
    
}

void VersionNVM::GetRange(const std::vector<const RamTable*>& inputs, InternalKey* smallest,
                InternalKey* largest)
{
    assert(!inputs.empty());
    smallest->Clear();
    largest->Clear();
    for(int i=0; i < inputs.size();i ++)
    {
        const RamTable* ramtable = inputs[i];
        if(i == 0)
        {
            *smallest = ramtable->GetSmallestInternalKey();
            *largest = ramtable->GetLargestInternalKey();
        }
        else
        {
            if(icmp_.Compare(ramtable->GetSmallestInternalKey(),*smallest) < 0)
            {
                *smallest = ramtable->GetSmallestInternalKey();
            }
            if(icmp_.Compare(ramtable->GetLargestInternalKey(), *largest) > 0)
            {
                *largest = ramtable->GetLargestInternalKey();
            }
        }
        
    }
}


void VersionNVM::GetOverlappingInputs(int level,const InternalKey* begin,  
                                    const InternalKey* end,    
                                    std::vector<const RamTable*>* inputs)
{
    assert(level >= 0);
    assert(level < config::kNumLevels);
    inputs->clear();
    Slice user_begin, user_end;
    if (begin != nullptr) {
        user_begin = begin->user_key();
    }
    if (end != nullptr) {
        user_end = end->user_key();
    }
    const Comparator* user_comp = icmp_.user_comparator();
    for(int i=0; i < tables_[level].size();)
    {
        const RamTable* ramtable = tables_[level][i++];
        InternalKey smallest_key = ramtable->GetSmallestInternalKey();
        InternalKey largest_key = ramtable->GetLargestInternalKey();
        const Slice table_start = smallest_key.user_key();
        const Slice table_limit = largest_key.user_key();

        const std::string t_start = ramtable->GetSmallestInternalKey().DebugString();
        const std::string t_limit = ramtable->GetLargestInternalKey().DebugString();

        if(begin != nullptr && user_comp->Compare(table_limit, user_begin) < 0)
        {

        }
        else if(end !=nullptr && user_comp->Compare(table_start, user_end) > 0)
        {
            
        }
        else
        {
            inputs->push_back(ramtable);
            if(level == 0)
            {
                //level-0 tables may overlap each other. so
                // check if the newly added table has expanded
                // the range. If so, restart searach
                if(begin != nullptr && user_comp->Compare(table_start, user_begin) < 0)
                {
                    user_begin = table_start;
                    inputs->clear();
                    i = 0;
                }
                else if (end != nullptr && user_comp->Compare(table_limit, user_end) > 0)
                {
                    user_end = table_limit;
                    inputs->clear();
                    i = 0;
                }
            }
        }
        
    }
}


// Finds the largest key in a vector of files. Returns true if files it not
// empty.
bool FindLargestKey(const InternalKeyComparator& icmp,
                    const std::vector<const RamTable*>& files,
                    InternalKey* largest_key) {
  if (files.empty()) {
    return false;
  }
  *largest_key = files[0]->GetLargestInternalKey();
  for (size_t i = 1; i < files.size(); ++i) {
    const RamTable* f = files[i];
    if (icmp.Compare(f->GetLargestInternalKey(), *largest_key) > 0) {
      *largest_key = f->GetLargestInternalKey();
    }
  }
  return true;
}

// Finds minimum file b2=(l2, u2) in level file for which l2 > u1 and
// user_key(l2) = user_key(u1)
const RamTable* FindSmallestBoundaryTable(
    const InternalKeyComparator& icmp,
    const std::vector<const RamTable*>& level_tables,
    const InternalKey& largest_key) {
  const Comparator* user_cmp = icmp.user_comparator();
  const RamTable* smallest_boundary_table = nullptr;
  for (size_t i = 0; i < level_tables.size(); ++i) {
    const RamTable* f = level_tables[i];
    if (icmp.Compare(f->GetSmallestInternalKey(), largest_key) > 0 &&
        user_cmp->Compare(f->GetSmallestInternalKey().user_key(), largest_key.user_key()) ==
            0) {
      if (smallest_boundary_table == nullptr ||
          icmp.Compare(f->GetSmallestInternalKey(), smallest_boundary_table->GetSmallestInternalKey()) < 0) {
        smallest_boundary_table = f;
      }
    }
  }
  return smallest_boundary_table;
}

void AddBoundaryInputs(const InternalKeyComparator& icmp,
                       const std::vector<const RamTable*>& level_tables,
                       std::vector<const RamTable*>* compaction_tables) 
{
    InternalKey largest_key;

    if(!FindLargestKey(icmp, *compaction_tables, &largest_key)){
        return;
    }

    bool continue_searching = true;
    while(continue_searching)
    {
        const RamTable * smallest_boundary_table = 
            FindSmallestBoundaryTable(icmp, level_tables, largest_key);
        if(smallest_boundary_table != nullptr)
        {
            compaction_tables->push_back(smallest_boundary_table);
            largest_key = smallest_boundary_table->GetLargestInternalKey();
        }
        else
        {
            
            continue_searching = false;
        }
        
    }
}

void VersionNVM::SetupOtherInputs(CompactionNVM* c)
{
    const int level = c->level();
    InternalKey smallest, largest;

    AddBoundaryInputs(icmp_, tables_[level], &c->inputs_[0]);
    GetRange(c->inputs_[0], &smallest, &largest);

    GetOverlappingInputs(level + 1, &smallest, &largest, &c->inputs_[1]);

    // Get entire range coverd by compaction
    InternalKey all_start, all_limit;
    GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);


    compact_pointer_[level] = largest.Encode().ToString();
    c->edit_.SetCompactPointer(level, largest);
}

std::string CompactionNVM::CompactionInfo() {
    std::string result;
    result.append("level:" + std::to_string(level_) + "\n");
    result.append("c0 size:" + std::to_string(inputs_[0].size()) + "\n");
    result.append("c1_size:" + std::to_string(inputs_[1].size()) + "\n");
    result.append("c0 key range" + inputs_[0][0]->GetSmallestInternalKey().DebugString() + "to" + 
                    inputs_[0][inputs_[0].size() - 1]->GetLargestInternalKey().DebugString());
    if(!inputs_[1].empty()) {
        result.append("c1 key range" + inputs_[1][0]->GetSmallestInternalKey().DebugString() + "to" + 
                    inputs_[1][inputs_[1].size() - 1]->GetLargestInternalKey().DebugString());
    }

    return result;
    
}


class VersionNVM::BuilderNVM
{
private:
    // Helper to sort by v->files_[file_number].smallest
    struct BySmallestKey {
        const InternalKeyComparator* internal_comparator;

        bool operator()(const RamTable* f1, const RamTable* f2) const {
        int r = internal_comparator->Compare(f1->GetSmallestInternalKey(), f2->GetSmallestInternalKey());
        if (r != 0) {
            return (r < 0);
        } else {
            // Break ties by file number
            return (f1->GetTableNumber() < f2->GetTableNumber());
        
            }
        }
    };

    typedef std::set<const RamTable*, BySmallestKey> TableSet;
    struct LevelState {
        //std::set<RamTa> deleted_files;
        std::vector<const  RamTable*> deleted_tables;
        TableSet* added_tables;
    };


    VersionNVM* base_;
    LevelState levels_[config::kNumLevels];

public:
    BuilderNVM(VersionNVM* v): base_(v)
    {
        BySmallestKey cmp;
        cmp.internal_comparator = &base_->icmp_;
        for(int level = 0; level < config::kNumLevels; level++)
        {
            levels_[level].added_tables = new TableSet(cmp);
        }
    }

    void Apply(VersionEditNVM* edit)
    {
        //update compaction pointer
        for(int i=0; i < edit->compact_pointers_.size(); i++)
        {
            const int level = edit->compact_pointers_[i].first;
            base_->compact_pointer_[level] = edit->compact_pointers_[i].second.Encode().ToString();
        }

        //delete table
        for(const auto& deleted_table_set_kvp: edit->deleted_tables_)
        {
            const int level = deleted_table_set_kvp.first;
            const RamTable* ramtable = deleted_table_set_kvp.second;
            levels_[level].deleted_tables.push_back(ramtable);

        }
        // add new tables;
        for(int i=0; i < edit->new_tables_.size(); i++)
        {
            const int level = edit->new_tables_[i].first;
            const RamTable* r = edit->new_tables_[i].second;
            levels_[level].added_tables->insert(r);

        }
    }

    void SaveTo(VersionNVM* v)
    {
        BySmallestKey cmp;
        cmp.internal_comparator = &base_->icmp_;
        for(int level = 0; level < config::kNumLevels; level++)
        {
            const std::vector<const RamTable*>& base_tables = base_->tables_[level];
            std::vector<const RamTable*>::const_iterator base_iter = base_tables.begin();
            std::vector<const RamTable*>::const_iterator base_end = base_tables.end();

            int base_tables_size = base_tables.size();

            const TableSet* added_tables = levels_[level].added_tables;
            v->tables_[level].reserve(base_tables.size() + added_tables->size());  
            for(const auto& added_table: *added_tables)
            {
                int i =0;


                InternalKey k1;
                InternalKey current_largest;
                InternalKey add_table_smallest_key;
                int compare_number ;
                std::string former_table_info;
                std::string add_table_info;

                for(std::vector<const RamTable*>::const_iterator bpos = 
                        std::upper_bound(base_iter, base_end, added_table, cmp);
                        base_iter != bpos; ++base_iter)
                {
                    //k1 =  (*base_iter)->GetSmallestInternalKey();
                    //current_largest = (*base_iter)->GetLargestInternalKey();
                    //add_table_smallest_key = added_table->GetSmallestInternalKey();
                    //compare_number = cmp.internal_comparator->Compare(k1, add_table_smallest_key);
                    /*
                    former_table_info.append("former table info: ");
                    former_table_info.append("smallest key:" + (*base_iter)->GetSmallestInternalKey().DebugString());
                    former_table_info.append("largest key:" + (*base_iter)->GetLargestInternalKey().DebugString() + "\n");
                    */
                    MaybeAddTable(v, level, *base_iter);
                    ++i;
                }
                /*
                add_table_info.append(" added_table_info: ");
                add_table_info.append("smallest key:" + added_table->GetSmallestInternalKey().DebugString()  );
                add_table_info.append("largest key:" + added_table->GetLargestInternalKey().DebugString() + "\n");
                std::cout<<former_table_info << std::endl;
                std::cout<<add_table_info << std::endl;
                */
                MaybeAddTable(v, level, added_table);
            }

            for(; base_iter != base_end; ++base_iter)
            {
                MaybeAddTable(v, level, *base_iter);
            }
#ifndef NDEBUG
            //make sure there is no overlap in levels > 0
            if(level > 0)
            {
                for(int i = 1; i < v->tables_[level].size(); i++)
                {
                    const InternalKey& prev_end = v->tables_[level][i - 1]->GetLargestInternalKey();
                    const InternalKey& this_begin = v->tables_[level][i]->GetSmallestInternalKey();
                    if(v->icmp_.Compare(prev_end, this_begin) >= 0)
                    {
                        fprintf(stderr, "overlapping ranges in same level %s. \n",
                                prev_end.DebugString().c_str(),
                                this_begin.DebugString().c_str());
                        abort();
                    }
                }
            }
#endif

        }
    }

    void MaybeAddTable(VersionNVM* v, int level, const RamTable* r)
    {
        std::vector<const RamTable*>::iterator it = 
                                        std::find(levels_[level].deleted_tables.begin(), levels_[level].deleted_tables.end(), r);
        
        if(it != levels_[level].deleted_tables.end())
        {
            // table is deleted: do nothing
        }
        else
        {
            std::vector<const RamTable*>* tables = &v->tables_[level];
            if(level > 0 && !tables->empty())
            {
                // must not overlap
                InternalKey current_largest = (*tables)[tables->size() - 1]->GetLargestInternalKey();
                InternalKey r_smallest = r->GetSmallestInternalKey();
                int compare_number = v->icmp_.Compare(current_largest, r_smallest);
                int base_compare_number = base_->icmp_.Compare(current_largest, r_smallest);
                std::string current_user_key = current_largest.user_key().ToString();
                std::string r_user_key = r_smallest.user_key().ToString();
                assert(v->icmp_.Compare((*tables)[tables->size() - 1]->GetLargestInternalKey(), 
                        r->GetSmallestInternalKey() ) < 0);
            }
            tables->push_back(r);
        }
        
        //if(levels_[level].deleted_tables)

    }
};

Status VersionNVM::Apply(VersionEditNVM* edit, VersionNVM** v,port::Mutex* mu)
{
    (*v) = new VersionNVM((*this));
    {
        BuilderNVM builder(this);
        builder.Apply(edit);
        builder.SaveTo(*v);
    }
    (*v)->Finalize();

    return Status::OK();
}

void VersionNVM::Finalize()
{
    int best_level = -1;
    double best_score=  - 1;
    for(int level =0; level < config::kNumLevels; level++)
    {
        double score;

        score = tables_[level].size() / static_cast<double>(MaxNumTablesForLevel(level));

        if(score > best_score)
        {
            best_score = score;
            best_level = level;
        }   
    }

    compaction_level_ = best_level;
    compaction_score_ = best_score;
}


void VersionNVM::Clear() {
    for(int i=0; i < config::kNumLevels; i++)
    {
        std::vector<const RamTable*>::iterator it;
        for( it = tables_[i].begin(); it != tables_[i].end(); it++) {
            delete *it;
        }

    }
}

Iterator* VersionNVM::GetIteratorByTableNumber(int tablenumber){
    for(int level=0; level < config::kNumLevels; level++) {
        for(int i=0; i < tables_[level].size(); i++) {
            if(tables_[level][i]->GetTableNumber() == tablenumber) {
                return tables_[level][i]->NewIterator();
            }
        }
    }

    return nullptr;
}





void VersionNVM::GetRange2(const std::vector<const RamTable*>& inputs1,
                const std::vector<const RamTable*>& inputs2,
                InternalKey* smallest, InternalKey* largest)

{
    std::vector<const RamTable*> all = inputs1;
    all.insert(all.end(), inputs2.begin(), inputs2.end());
    GetRange(all, smallest, largest);

}



Iterator*  VersionNVM::MakeInputIterator(CompactionNVM * c)
{
    //Level0 sstables have to be merged together
    const int space = (c->level() == 0 ? c->inputs_[0].size() + 1 : 2);
    Iterator** list = new Iterator*[space];
    int num = 0;
    for(int which=0; which < 2; which ++)
    {
        if(!c->inputs_[which].empty())
        {
            if(which + c->level() == 0)
            {
                const std::vector<const  RamTable*> ram_tables = c->inputs_[which];
            }
            else
            {
                list[num++] = new LevelConcatIterator(icmp_, &c->inputs_[which]);
            }
        }
    }

    assert(num <= space);
    Iterator *result = new MergingIterator(&icmp_, list, num);
    return result;
}




static bool AfterTable(const Comparator* ucmp, const Slice* user_key,
                      const RamTable* f) {
  // null user_key occurs before all keys and is therefore never after *f
  return (user_key != nullptr &&
          ucmp->Compare(*user_key, f->GetLargestInternalKey().user_key()) > 0);
}

static bool BeforeTable(const Comparator* ucmp, const Slice* user_key,
                       const RamTable* f) {
  // null user_key occurs after all keys and is therefore never before *f
  return (user_key != nullptr &&
          ucmp->Compare(*user_key, f->GetSmallestInternalKey().user_key()) < 0);
}


bool SomeFileOverlapsRange(const InternalKeyComparator& icmp,
                           bool disjoint_sorted_files,
                           const std::vector<const RamTable*>& tables,
                           const Slice* smallest_user_key,
                           const Slice* largest_user_key) {
  const Comparator* ucmp = icmp.user_comparator();
  if (!disjoint_sorted_files) {
    // Need to check against all files
    for (size_t i = 0; i < tables.size(); i++) {
      const RamTable* f = tables[i];
      if (AfterTable(ucmp, smallest_user_key, f) ||
          BeforeTable(ucmp, largest_user_key, f)) {
        // No overlap
      } else {
        return true;  // Overlap
      }
    }
    return false;
  }

  // Binary search over file list
  uint32_t index = 0;
  if (smallest_user_key != nullptr) {
    // Find the earliest possible internal key for smallest_user_key
    InternalKey small_key(*smallest_user_key, kMaxSequenceNumber,
                          kValueTypeForSeek);
    index = FindTable(icmp, tables, small_key.Encode());
  }

  if (index >= tables.size()) {
    // beginning of range is after all files, so no overlap.
    return false;
  }

  return !BeforeTable(ucmp, largest_user_key, tables[index]);
}


bool VersionNVM::OverlapInLevel(int level, const Slice* smallest_user_key, const Slice* largest_user_key)
{
  return SomeFileOverlapsRange(icmp_, (level > 0), tables_[level],
                               smallest_user_key, largest_user_key);
}


CompactionNVM* VersionNVM::CompactRange(int level, const InternalKey* begin,
                                     const InternalKey* end)
{
    std::vector<const RamTable*> inputs;
    GetOverlappingInputs(level, begin, end, &inputs);
    if (inputs.empty()) {
        return nullptr;
    }



    CompactionNVM* c = new CompactionNVM(options_, level);
    c->input_version_ = this;
    //c->input_version_->Ref();
    c->inputs_[0] = inputs;
    SetupOtherInputs(c);
    return c;
}                                     

VersionNVM::VersionNVM(const Options *options, const InternalKeyComparator* icmp)
    :last_sequence_(0),
    options_(options),
    icmp_(*icmp),
    env_(options_->env),
    next_table_number_(0)
{

}


VersionNVM::VersionNVM(const VersionNVM& v)
: last_sequence_(v.last_sequence_),
    options_(v.options_),
    icmp_(v.icmp_),
    env_(v.env_),
    next_table_number_(v.next_table_number_) {
    for(int i=0; i< config::kNumLevels; i++) {
        compact_pointer_[i] = v.compact_pointer_[i];
    }
    /*
    for(int level=0; level < config::kNumLevels; level++) {
        tables_[level] = v.tables_[level];
    }
    */
}

VersionNVM::~VersionNVM()
{
    
}

void VersionNVM::AddIterators(std::vector<Iterator*> * list) {
    for(int i = 0; i < tables_[0].size(); i++) {
        list->push_back(tables_[0][i]->NewIterator());
    }

    for(int level = 1; level < config::kNumLevels; level++) {
        if(!tables_[level].empty()) {
            list->push_back(new LevelConcatIterator(icmp_, &tables_[level]));
        }
    }
}

namespace {
enum SaverState {
  kNotFound,
  kFound,
  kDeleted,
  kCorrupt,
};
struct Saver {
  SaverState state;
  const Comparator* ucmp;
  Slice user_key;
  std::string* value;
};
}  // namespace
static void SaveValue(void* arg, const Slice& ikey, const Slice& v) {
  Saver* s = reinterpret_cast<Saver*>(arg);
  ParsedInternalKey parsed_key;
  if (!ParseInternalKey(ikey, &parsed_key)) {
    s->state = kCorrupt;
  } else {
    if (s->ucmp->Compare(parsed_key.user_key, s->user_key) == 0) {
      s->state = (parsed_key.type == kTypeValue) ? kFound : kDeleted;
      if (s->state == kFound) {
        s->value->assign(v.data(), v.size());
      }
    }
  }
}

Status VersionNVM::Get(const ReadOptions& options, const LookupKey& k,
                std::string* value)
{
    struct State
    {
        Saver saver;
        const ReadOptions* options;
        Slice ikey;
        const RamTable* last_table_read;
        int last_table_read_level;

        VersionNVM* v;
        Status s;
        bool found;

        static bool Match(void* arg, int level, const RamTable* r)
        {
            State* state = reinterpret_cast<State*>(arg);

            state->last_table_read = r;
            state->last_table_read_level = level;

            //state->s = state->v->Get(*state->options, )
            state->s = r->Get(state->ikey, &state->saver, SaveValue);

            switch (state->saver.state)
            {
            case kNotFound:
                return true; // keep searching
            case kFound:
                state->found = true;
                return false;
            case kDeleted:
                return false;
            default:
                return false;
            }

            return false;
        }
    };

    State state;
    state.found = false;
    state.last_table_read = nullptr;
    state.last_table_read_level = -1 ;

    state.options = &options;
    state.ikey = k.internal_key();
    state.v = this;

    state.saver.state =kNotFound;
    state.saver.ucmp = icmp_.user_comparator();
    state.saver.user_key = k.user_key();
    state.saver.value = value;

    ForEachOverlapping(state.saver.user_key, state.ikey, &state, &State::Match);

    return state.found ? state.s : Status::NotFound((Slice()));

    
}               

bool CompactionNVM::IsTrivialMove()
{
    return num_input_tables(0) == 1 && num_input_tables(1) == 0;
}

bool CompactionNVM::IsBaseLevelForKey(const Slice& user_key)
{
    return true;
}

CompactionNVM::~CompactionNVM()
{

}



MergingIterator::MergingIterator(const InternalKeyComparator* icomp, Iterator** list, int n)
: icmp_(icomp), n_(n), current_(nullptr)
{
    child_ = new Iterator*[n_];
    for(int i=0; i< n_; i++) {
        child_[i] = list[i];
    }

}

bool MergingIterator::Valid() const 
{
    return current_ != nullptr;
}

void MergingIterator::Seek(const Slice& target) 
{

}

void MergingIterator::SeekToFirst() {
    for(int i = 0; i< n_; i++){
        child_[i]->SeekToFirst();
    }
    FindSmallest();
}

void MergingIterator::SeekToLast() 
{

}

Slice MergingIterator::key() const 
{
    assert(Valid());
    return current_->key();
}

Slice MergingIterator::value() const 
{
    assert(Valid());
    return current_->value();

}

void MergingIterator::Next() 
{
    current_->Next();
    FindSmallest();
}

void MergingIterator::Prev() 
{

}

Status MergingIterator::status() const 
{
    return Status::OK();
}

void MergingIterator::FindSmallest()
{
    Iterator* smallest = nullptr;
    for(int i=0; i < n_; i++)
    {
        Iterator* child = child_[i];
        if(child->Valid()) {
            if(smallest == nullptr) {
                smallest = child;
            } else if(icmp_->Compare(child->key(), smallest->key()) < 0){
                smallest = child;
            }
        }

    }

    current_ = smallest;
}



LevelConcatIterator::LevelConcatIterator(const InternalKeyComparator& icmp, 
const std::vector<const RamTable*> *table_list) : icmp_(icmp), table_list_(table_list), 
index_(table_list_->size()) // mark as invalid 
{
    CreateTableIterators();
}

bool LevelConcatIterator::Valid() const 
{
    return index_ >= 0 && index_ < table_list_->size() && ram_table_iterators[index_]->Valid();
}

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

void LevelConcatIterator::Seek(const Slice& target) 
{
    //binary seach to find the index of sstable

    index_ = FindTable(icmp_, (*table_list_), target);
    /*
    int left = 0;
    int right = table_list_->size() - 1;


    while(left < right)
    {
        int mid = (left + right) / 2;
        ParsedInternalKey pkey = ParseInternalKey()
        
    }
    */

    //using iterator to find target
    ram_table_iterators[index_]->Seek(target);

}

void LevelConcatIterator::SeekToFirst() 
{
    index_ = 0;
    ram_table_iterators[index_]->SeekToFirst();

}

void LevelConcatIterator::SeekToLast() 
{
    if(table_list_->empty()) 
    {
        return;
    }
    else
    {
        index_ = table_list_->size() - 1;
        ram_table_iterators[index_]->SeekToLast();
    }

}

void LevelConcatIterator::Next() 
{
    //assert(Valid());
    //index_++;
    /*
    ram_table_iterators[index_]->Next();
    if(!Valid())
    {
        index_++;
        if(index_ == table_list_->size())
        {
            return;
        }
        else
        {
            ram_table_iterators[index_]->SeekToFirst();
        }
        

    }
    
    if(index_ >= table_list_->size() || index_ <0) return;
    if(!ram_table_iterators[index_]->Valid())
    {
        if(index_ < table_list_->size() - 1)
        {
            index_++;
            ram_table_iterators[index_]->SeekToFirst();
        }
        else 
        {
            // index has come to the end
            // mark as invalid
            index_ = table_list_->size(); 
        }
    }
    else {
        */
    ram_table_iterators[index_]->Next();
    if(!ram_table_iterators[index_]->Valid()) {
        index_++;
        if(index_ >= ram_table_iterators.size()) return;
        ram_table_iterators[index_]->SeekToFirst();
    }
    
    
    
}

void LevelConcatIterator::Prev() 
{
    assert(Valid());
    /*
    if(index_ == 0)
    {
        index_ = table_list_->size(); //mark as invalid
    }
    else
    {
        index_--;
    }
    */
}

Slice LevelConcatIterator::key() const 
{
    assert(Valid());
    return ram_table_iterators[index_]->key();
    //return (*table_list_)[index_]->GetLargestKey();
}

Slice LevelConcatIterator::value() const 
{
    assert(Valid());
    return ram_table_iterators[index_]->value();

}

Status LevelConcatIterator::status() const
{
    return Status::OK();
}





}