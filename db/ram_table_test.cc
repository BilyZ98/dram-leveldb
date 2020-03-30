#include "db/ram_table.h"

#include <map>
#include <string>

#include "gtest/gtest.h"
#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/version_NVM.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/iterator.h"
#include "util/testutil.h"

namespace leveldb {


// An STL comparator that uses a Comparator
namespace {
struct STLLessThan {
  const Comparator* cmp;

  STLLessThan() : cmp(BytewiseComparator()) {}
  STLLessThan(const Comparator* c) : cmp(c) {}
  bool operator()(const std::string& a, const std::string& b) const {
    return cmp->Compare(Slice(a), Slice(b)) < 0;
  }
};
}  // namespace


typedef std::map<std::string, std::string, STLLessThan> KVMap;

// Helper class for tests to unify the interface between
// BlockBuilder/TableBuilder and Block/Table.
class Constructor {
 public:
  explicit Constructor(const Comparator* cmp) : data_(STLLessThan(cmp)) {}
  virtual ~Constructor() = default;

  void Add(const std::string& key, const Slice& value) {
    data_[key] = value.ToString();
  }

  // Finish constructing the data structure with all the keys that have
  // been added so far.  Returns the keys in sorted order in "*keys"
  // and stores the key/value pairs in "*kvmap"
  void Finish(const Options& options, std::vector<std::string>* keys,
              KVMap* kvmap) {
    *kvmap = data_;
    keys->clear();
    for (const auto& kvp : data_) {
      keys->push_back(kvp.first);
    }
    data_.clear();
    Status s = FinishImpl(options, *kvmap);
    ASSERT_TRUE(s.ok()) << s.ToString();
  }

  // Construct the data structure from the data in "data"
  virtual Status FinishImpl(const Options& options, const KVMap& data) = 0;

  virtual Iterator* NewIterator() const = 0;

  const KVMap& data() const { return data_; }

  virtual DB* db() const { return nullptr; }  // Overridden in DBConstructor

 private:
  KVMap data_;
};

// A helper class that converts internal format keys into user keys
class KeyConvertingIterator : public Iterator {
 public:
  explicit KeyConvertingIterator(Iterator* iter) : iter_(iter) {}

  KeyConvertingIterator(const KeyConvertingIterator&) = delete;
  KeyConvertingIterator& operator=(const KeyConvertingIterator&) = delete;

  ~KeyConvertingIterator() override { delete iter_; }

  bool Valid() const override { return iter_->Valid(); }
  void Seek(const Slice& target) override {
    ParsedInternalKey ikey(target, kMaxSequenceNumber, kTypeValue);
    std::string encoded;
    AppendInternalKey(&encoded, ikey);
    iter_->Seek(encoded);
  }
  void SeekToFirst() override { iter_->SeekToFirst(); }
  void SeekToLast() override { iter_->SeekToLast(); }
  void Next() override { iter_->Next(); }
  void Prev() override { iter_->Prev(); }

  Slice key() const override {
    assert(Valid());
    ParsedInternalKey key;
    if (!ParseInternalKey(iter_->key(), &key)) {
      status_ = Status::Corruption("malformed internal key");
      return Slice("corrupted key");
    }
    return key.user_key;
  }

  Slice value() const override { return iter_->value(); }
  Status status() const override {
    return status_.ok() ? iter_->status() : status_;
  }

 private:
  mutable Status status_;
  Iterator* iter_;
};


class MemTableConstructor : public Constructor {
 public:
  explicit MemTableConstructor(const Comparator* cmp)
      : Constructor(cmp), internal_comparator_(cmp) {
    memtable_ = new MemTable(internal_comparator_);
    memtable_->Ref();
  }
  ~MemTableConstructor() override { memtable_->Unref(); }
  Status FinishImpl(const Options& options, const KVMap& data) override {
    memtable_->Unref();
    memtable_ = new MemTable(internal_comparator_);
    memtable_->Ref();
    int seq = 1;
    for (const auto& kvp : data) {
      memtable_->Add(seq, kTypeValue, kvp.first, kvp.second);
      seq++;
    }
    return Status::OK();
  }
  Iterator* NewIterator() const override {
    return new KeyConvertingIterator(memtable_->NewIterator());
  }

 private:
  const InternalKeyComparator internal_comparator_;
  MemTable* memtable_;
};


class RamTableConstructor: public Constructor {
public:

    RamTableConstructor(const Comparator* cmp): Constructor(cmp),internal_comparator(cmp) {
        
        //ram_table_ = new RamTable(mem_iter, &internal_comparator);
    }

    Iterator* NewIterator() const override {
        return new KeyConvertingIterator(ram_table_->NewIterator());
    }

    Status FinishImpl(const Options& options, const KVMap& data) override{
        
        ram_table_ = new RamTable(&internal_comparator);

        int seq = 1;
        for(const auto& kvp : data) {
            InternalKey ikey(kvp.first, seq, kTypeValue);
            ram_table_->Append(ikey, kvp.second);
            /*
            if(ikey.DecodeFrom(kvp.first)){
                ram_table_->Append(ikey, kvp.second);
            }
            */
            seq++;
            
        }

        return Status::OK();
    }

    ~RamTableConstructor() {
        delete ram_table_;
    }

private:

const InternalKeyComparator internal_comparator;
RamTable* ram_table_;
};


class Harness: public testing::Test {
public:
    Harness() {
        options_ = Options();
        constructor_ = new RamTableConstructor( options_.comparator);

    }

    ~Harness() {
        delete constructor_;
    }

    void Add(const std::string& key, const std::string& value) {
        constructor_->Add(key, value);
    }

    void Test() {
        std::vector<std::string> keys;
        KVMap data;
        constructor_->Finish(options_, &keys, &data);

        TestForwardScan(keys, data);        
    }

    void TestForwardScan(const std::vector<std::string>& keys,
                       const KVMap& data) {

        Iterator* iter = constructor_->NewIterator();
        ASSERT_TRUE(!iter->Valid());
        iter->SeekToFirst();
        for(KVMap::const_iterator model_iter = data.begin(); 
            model_iter != data.end(); ++model_iter) {
            ASSERT_EQ(ToString(data, model_iter), ToString(iter));
            iter->Next();
        }
        ASSERT_TRUE(!iter->Valid());
        delete iter;                   
    }


    std::string ToString(const KVMap& data, const KVMap::const_iterator& it) {
        if (it == data.end()) {
        return "END";
        } else {
        return "'" + it->first + "->" + it->second + "'";
        }
    }

    std::string ToString(const Iterator* it) {
        if (!it->Valid()) {
        return "END";
        } else {
        return "'" + it->key().ToString() + "->" + it->value().ToString() + "'";
        }
    }

    Iterator* GetIterator() {
      return constructor_->NewIterator();
    }

    Constructor* GetConstructor() {
      return constructor_;
    }

        
private:
    Constructor *constructor_;
    Options options_;
};

TEST_F(Harness, Empty) {
    Test();
}

TEST_F(Harness, SimpleEmptyKey) {
    Add("", "v");
    Test();

}

TEST_F(Harness, SimpleSingle) {
    Add("abc", "v");
    Test();
}

TEST_F(Harness, SimpleMulti) {

    Random rnd(test::RandomSeed() + 3);
    Add("abc", "v");
    Add("abcd", "v");
    Add("ac", "v2");
    Test();
  
}

TEST_F(Harness, Randomized) {


    Random rnd(test::RandomSeed() + 5);
    for (int num_entries = 0; num_entries < 200;
         num_entries += (num_entries < 5 ? 1 : 20)) {

      for (int e = 0; e < num_entries; e++) {
        std::string v;
        Add(test::RandomKey(&rnd, rnd.Skewed(4)),
            test::RandomString(&rnd, rnd.Skewed(5), &v).ToString());
      }
      Test();
    }
  
}


TEST(RamTable, Seek) {
  Options options;
  InternalKeyComparator icmp(options.comparator);
  RamTable * r = new RamTable(&icmp);
  Slice target = "346";
  Slice target_value = "hi";
  Slice noExist = "wode";
  InternalKey ikey1("345", 1, kTypeValue);
  InternalKey ikey2("346", 2, kTypeValue);
  InternalKey ikey3("789", 3, kTypeValue);
  r->Append(ikey1, "hello");
  r->Append(ikey2, "hi");
  r->Append(ikey3, "hola");

  Iterator* iter = r->NewIterator();
  iter->SeekToFirst();
  ParsedInternalKey ikey(target, kMaxSequenceNumber, kTypeValue);
  std::string encoded;
  AppendInternalKey(&encoded, ikey);
  iter->Seek(encoded);
  EXPECT_EQ(target_value, iter->value());


  ParsedInternalKey pikey2(noExist, kMaxSequenceNumber, kTypeValue);
  encoded.clear();
  AppendInternalKey(&encoded, pikey2);
  iter->Seek(encoded);
  EXPECT_FALSE(iter->Valid());

  delete iter;
}


TEST(LevelConcatIterator, TestSequential) {
  Options options;
  InternalKeyComparator icmp(options.comparator);
  RamTable * r1 = new RamTable(&icmp);
  RamTable * r2 = new RamTable(&icmp);

  KVMap sorted_data;

  Slice key1 = "345";
  Slice key2 = "346";
  Slice key3 = "789";

  InternalKey ikey1(key1, 1, kTypeValue);
  InternalKey ikey2(key2, 2, kTypeValue);
  InternalKey ikey3(key3, 3, kTypeValue);

  Slice val1 = "345";
  Slice val2 = "346";
  Slice val3 = "789";

  sorted_data[key1.ToString()] = val1.ToString();
  sorted_data[key2.ToString()] = val2.ToString();
  sorted_data[key3.ToString()] = val3.ToString();


  r1->Append(ikey1, val1);
  r1->Append(ikey2, val2);

  r2->Append(ikey3, val3);

  std::vector<const RamTable*> ramtable_vec;
  ramtable_vec.push_back(r1);
  ramtable_vec.push_back(r2);

  Iterator* iter = new LevelConcatIterator(icmp, &ramtable_vec);

  iter->SeekToFirst();
  for(const auto&kvp: sorted_data) {
    EXPECT_EQ(kvp.second, iter->value());
    iter->Next();
  }

  // test seek


  for(const auto&kvp: sorted_data) {
    Slice target = kvp.first;
    ParsedInternalKey ikey(target, kMaxSequenceNumber, kTypeValue);
    std::string encoded;
    AppendInternalKey(&encoded, ikey);

    iter->Seek(encoded);
    Slice value = iter->value();
    EXPECT_EQ(kvp.second, iter->value());
    iter->Next();
  }

}

TEST(MergingIterator, SequentialAndSeek) {
  Options options;
  InternalKeyComparator icmp(options.comparator);
  RamTable * r1 = new RamTable(&icmp);
  RamTable * r2 = new RamTable(&icmp);
  RamTable* r3 = new RamTable(&icmp);

  KVMap sorted_data;

  Slice key1 = "345";
  Slice key2 = "346";
  Slice key3 = "789";
  Slice key4 = "890";

  InternalKey ikey1(key1, 1, kTypeValue);
  InternalKey ikey2(key2, 2, kTypeValue);
  InternalKey ikey3(key3, 3, kTypeValue);
  InternalKey ikey4(key4, 4, kTypeValue);

  Slice val1 = "345";
  Slice val2 = "346";
  Slice val3 = "789";
  Slice val4 = "890";

  sorted_data[key1.ToString()] = val1.ToString();
  sorted_data[key2.ToString()] = val2.ToString();
  sorted_data[key3.ToString()] = val3.ToString();
  sorted_data[key4.ToString()] = val4.ToString();

  r1->Append(ikey1, val1);
  r1->Append(ikey2, val2);

  r2->Append(ikey3, val3);

  r3->Append(ikey4, val4);

  std::vector<const RamTable*> ramtable_vec;
  ramtable_vec.push_back(r1);
  ramtable_vec.push_back(r2);

  Iterator** list = new Iterator*[2];
  list[1] = new LevelConcatIterator(icmp, &ramtable_vec);
  list[0] = r3->NewIterator();

  Iterator* iter = new MergingIterator(&icmp, list, 2);

  iter->SeekToFirst();
  for(const auto&kvp: sorted_data) {
    EXPECT_EQ(kvp.second, iter->value());
    iter->Next();
  }



  delete r1;
  delete r2;
  delete r3;

  for(int i=0; i< 2; i++) {
    delete list[i];
  }

  delete iter;

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


TEST(RamTable, CopyMemTable) {
  Options options;
  InternalKeyComparator icmp(options.comparator);
  MemTable* mem = new MemTable(icmp);
  mem->Ref();

  Slice keys[3] = {"1234", "234", "145"};
  Slice vals[3] = {"1234", "234", "145"};

  int seq = 1;
  int entry_num = 3;
  for(int i=0; i<entry_num; i++) {
    mem->Add(seq++, kTypeValue, keys[i], vals[i]);
  }

  Iterator* mem_iter = mem->NewIterator();
  RamTable* ramtable = new RamTable(mem_iter, &icmp);

  mem->Unref();

  Iterator* ram_iter = ramtable->NewIterator();

  CheckIterator(ram_iter);

  delete ram_iter;
  

}


} //namespace leveldb

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}