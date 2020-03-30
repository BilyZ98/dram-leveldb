#include <iostream>

#include "db/ram_table.h"


#include "leveldb/export.h"
#include "leveldb/iterator.h"

#include "db/dbformat.h"

namespace leveldb{

struct RamTable::Node{
    // maybe InternalKey
    InternalKey key;
    Slice value;
    //ValueType type;
    //SequenceNumber seq;
};

RamTable::~RamTable()
{
    delete[] node_arr_;
}



RamTable::RamTable(const  InternalKeyComparator* comparator)
{
    comparator_ = comparator;
    table_size_ = config::TableSize;

    node_arr_ = new Node[table_size_];

    pos_ = -1;
}

bool RamTable::Full()
{
    return pos_ == table_size_ - 1;
}

bool RamTable::Append(const InternalKey& ikey, const Slice& value)
{
    if(Full())
    {
        return false;
    }
    Node node;
    node.key = ikey;
    node.value = value;


    pos_ ++;
    if(pos_ == 0)
    {
        smallest_key_ = ikey;
        //smallest_key_.DecodeFrom(ikey.Encode());
    }

    node_arr_[pos_] = node;
    //largest_key_.DecodeFrom(ikey.Encode());
    largest_key_ = ikey;
    
    if(pos_ > 0) {
        InternalKey former_key = node_arr_[pos_ - 1].key;
        InternalKey cur_key = node_arr_[pos_].key;
        assert(comparator_->Compare(ikey, node_arr_[pos_ - 1].key) > 0);
    }
    return true;
}

RamTable::RamTable(Iterator* iter, const InternalKeyComparator* comparator) 
{
    comparator_ = comparator;
    int mem_entry_size= NumMemTableEntry(iter);
    table_size_ =  mem_entry_size;

    node_arr_ = new Node[table_size_];

    iter->SeekToFirst();
    assert(iter->Valid());

    pos_ =-1;
    smallest_key_.DecodeFrom(iter->key());
    for(; iter->Valid(); iter->Next())
    {
        pos_++;
        Slice key = iter->key();
        largest_key_.DecodeFrom(key);
        Node node;
        node.key.DecodeFrom(key);
        node.value = iter->value();

        node_arr_[pos_] = node;
        //InsertNodeAtPos(pos_, node);
        //pos_++ ;
    }

    //std::cout << "table_size:" <<  table_size_ << std::endl;

}

void RamTable::InsertNodeAtPos(int pos, const Node& node){
    node_arr_[pos] = node;

}

int RamTable::NumMemTableEntry(Iterator* iter)
{
    iter->SeekToFirst();
    assert(iter->Valid());
    int num=0;
    for(;iter->Valid(); iter->Next()){
        ++num;
    }
    
    return num;
}

/*
    key internal_key
    void* arg  state.saver
    *handle_result Match function
*/
Status RamTable::Get(const Slice& key, void* arg, 
        void(*handle_result)(void*, const Slice&, const Slice&)) const  {

    
    Iterator* riter = NewIterator();
    riter->Seek(key);
    if(riter->Valid()) {
        const Slice key = riter->key();
        const Slice value = riter->value();
        (&*handle_result)(arg, riter->key(), riter->value());
    }

    delete riter;
    return Status::OK();
    /*
    //InternalKeyComparator comparator;
    Slice internal_key = key.internal_key();
    ParsedInternalKey * parsed_key = new ParsedInternalKey();
    for()
    for(int i=0; i < table_size_; i++){
        if(ParseInternalKey(node_arr_[i].key.Encode(), parsed_key))
        {
            if(comparator_->user_comparator()->Compare(parsed_key->user_key, key.user_key()) == 0)
            {
                const ValueType tag = parsed_key->type;
                switch (tag)
                {
                case kTypeValue:
                {
                    value->assign(node_arr_[i].value.data(), node_arr_[i].value.size());
                    return true;

                }

                case kTypeDeletion:
                    *s = Status::NotFound(Slice());
                    return true;

                }
            }
        }
        
    }
    return false;
    */
}


InternalKey RamTable::GetSmallestInternalKey() const
{
    return smallest_key_;
}

InternalKey RamTable::GetLargestInternalKey() const
{
    return largest_key_;
}

Iterator* RamTable::NewIterator() const
{

    return new RamTableIterator(this);
        
}




RamTableIterator::RamTableIterator(const RamTable* ram_table)
    :ram_table_(ram_table),
    pos_(-1)
    {

    }


//if not found search key, might set pos = -1
bool RamTableIterator::Valid() const 
{ 
    return pos_ >= 0 && pos_ <= ram_table_->pos_;
    
}

// binary search the key in the node_arr_ array 
// of RamTable that is the first key that is 
// larger than the search key 
void RamTableIterator::Seek(const Slice& k) {
    int left = 0;
    int right = ram_table_->pos_ ;
    const Comparator* comp = ram_table_->comparator_;
    while(left < right)
    {
        int mid = (left + right) / 2;
        const Slice ikey = ram_table_->node_arr_[mid].key.Encode();

        if(comp->Compare(ikey, k) <0) {
            left = mid + 1;
        } else {
            right = mid ;
        }

    }
    //pos_ = -1; //if not found the key, mark as invalid
    const Slice ikey = ram_table_->node_arr_[right].key.Encode();
    if(comp->Compare(ikey, k) >= 0) {
        pos_ = right;
    } else {
        //not found markas invalid
        pos_ = -1 ;
    }


}

void RamTableIterator::SeekToFirst() 
{
    //valid_ = true;
    pos_ =0;
}

void RamTableIterator::SeekToLast()
{
    //valid_ = true;
    pos_ = ram_table_->pos_;
}

void RamTableIterator::Next()
{
    //if pos_ == ram_table_->table_size_ -1
    // then pos_ ++ will mark the iter as invalid
    // so we can see this iterator has come to the end

    
    /*
    if(pos_ == ram_table_->table_size_ ) 
    {
        return;
    }
    else
    {
        pos_++;
    }*/

    //assert(Valid());
    if(!Valid()) return;
    pos_++;
    
}

void RamTableIterator::Prev() 
{
    assert(Valid());
    //if(!valid_) return;
    pos_--;
    if(pos_ < 0 )
    {
        pos_ = ram_table_->pos_;
    }
}

Slice RamTableIterator::key() const 
{
    return ram_table_->node_arr_[pos_].key.Encode();
}

Slice RamTableIterator::value() const
{
    return ram_table_->node_arr_[pos_].value;
}

Status RamTableIterator::status() const
{
    return Status::OK();
}




}
