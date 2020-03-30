#include "leveldb/db.h"

#include <atomic>
#include <string>

#include "gtest/gtest.h"
#include "db/db_impl_nvm.h"
#include "db/ram_table.h"
#include "db/write_batch_internal.h"
#include "db/version_NVM.h"

#include "port/port.h"
#include "port/thread_annotations.h"
#include "util/mutexlock.h"
#include "util/testutil.h"



namespace leveldb {
static std::string RandomString(Random* rnd, int len) 
{
    std::string r;
    test::RandomString(rnd, len, &r);
    return r;
}

static std::string RandomKey(Random* rnd) {
// Short sometimes to encourage collisions
int len = (rnd->OneIn(3) ? 1 : (rnd->OneIn(100) ? rnd->Skewed(10) : rnd->Uniform(10)));
return test::RandomKey(rnd, len);
}

class DBTest: public testing::Test
{
public:

    std::string dbname_;
    DB* db_;

    DBTest()
    {
        dbname_ = "db_test";
        db_ = nullptr;
        Options opts;
        //DB::Open(opts, dbname_, &db_);
        //ASSERT_LEVELDB_OK(TryReopen());
        TryReopen();
    }

    void TestBody() override {

    }

    DBImplNVM* dbfull() { return reinterpret_cast<DBImplNVM*>(db_); }

    void TryReopen() {
        delete db_;
        db_ = nullptr;
        Options opts;
        DB::Open(opts, dbname_, &db_);
    }

    Status Put(const std::string& k, const std::string& v) {
    return db_->Put(WriteOptions(), k, v);
    }

    Status Delete(const std::string& k) { return db_->Delete(WriteOptions(), k); }    

    std::string Get(const std::string& k, const Snapshot* snapshot = nullptr) {
        ReadOptions options;
        options.snapshot = snapshot;
        std::string result;
        Status s = db_->Get(options, k, &result);
        if (s.IsNotFound()) {
        result = "NOT_FOUND";
        } else if (!s.ok()) {
        result = s.ToString();
        }
        return result;
    }


    void Compact(const Slice& start, const Slice& limit) {
        db_->CompactRange(&start, &limit);
    }

    void MakeTables(int n, const std::string& small_key,
                  const std::string& large_key) {
        for (int i = 0; i < n; i++) {
        Put(small_key, "begin");
        Put(large_key, "end");
        dbfull()->TEST_CompactMemTable();
        }
    }

    // Prevent pushing of new sstables into deeper levels by adding
    // tables that cover a specified range to all levels.
    void FillLevels(const std::string& smallest, const std::string& largest) {
        MakeTables(config::kNumLevels, smallest, largest);
    }

    int NumTablesAtLevel(int level) {
        std::string property;
        EXPECT_TRUE(db_->GetProperty(
            "leveldb.num-tables-at-level"+ std::to_string(level), &property));
        return std::stoi(property);
    }


    std::string TablesPerLevel() {
        std::string result;
        int last_non_zero_offset = 0;
        for(int level = 0; level <config::kNumLevels; level++) {
            int f = NumTablesAtLevel(level);
            char buf[100];
            snprintf(buf, sizeof(buf), "%s%d", (level ? "," : ""), f);
            result += buf;
            if(f > 0) {
                last_non_zero_offset = result.size();
            }
        }
        result.resize(last_non_zero_offset);
        return result;
    }



};

/*


TEST_F(DBTest, ReadWrite) {

    ASSERT_LEVELDB_OK(Put("foo", "v1"));
    ASSERT_EQ("v1", Get("foo"));
    ASSERT_LEVELDB_OK(Put("bar", "v2"));
    ASSERT_LEVELDB_OK(Put("foo", "v3"));
    ASSERT_EQ("v3", Get("foo"));
    ASSERT_EQ("v2", Get("bar"));

}

TEST_F(DBTest, PutDeleteGet) {

    ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "foo", "v1"));
    ASSERT_EQ("v1", Get("foo"));
    ASSERT_LEVELDB_OK(db_->Put(WriteOptions(), "foo", "v2"));
    ASSERT_EQ("v2", Get("foo"));
    ASSERT_LEVELDB_OK(db_->Delete(WriteOptions(), "foo"));
    ASSERT_EQ("NOT_FOUND", Get("foo"));

}

*/


/*
TEST_F(DBTest, GetFromVersions) {
    ASSERT_LEVELDB_OK(Put("foo", "v1"));
    dbfull()->TEST_CompactMemTable();
    ASSERT_EQ("v1", Get("foo"));

}



*/

TEST_F(DBTest, GetPicksCorrectFile) {

    // Arrange to have multiple files in a non-level-0 level.
    ASSERT_LEVELDB_OK(Put("a", "va"));
    ASSERT_EQ("va", Get("a"));    
    Compact("a", "b");
    ASSERT_EQ("va", Get("a"));
    ASSERT_LEVELDB_OK(Put("x", "vx"));
    ASSERT_EQ("va", Get("a"));    
    Compact("x", "y");
    ASSERT_EQ("va", Get("a"));
    ASSERT_LEVELDB_OK(Put("f", "vf"));
    ASSERT_EQ("va", Get("a"));
    Compact("f", "g");
    ASSERT_EQ("va", Get("a"));
    ASSERT_EQ("vf", Get("f"));
    ASSERT_EQ("vx", Get("x"));

}


TEST_F(DBTest, GetFromVersions) {
    ASSERT_LEVELDB_OK(Put("foo", "v1"));
    dbfull()->TEST_CompactMemTable();
    ASSERT_EQ("v1", Get("foo"));

}

TEST_F(DBTest, GetLevel0Ordering) {

    // Check that we process level-0 files in correct order.  The code
    // below generates two level-0 files where the earlier one comes
    // before the later one in the level-0 file list since the earlier
    // one has a smaller "smallest" key.
    ASSERT_LEVELDB_OK(Put("bar", "b"));
    ASSERT_LEVELDB_OK(Put("foo", "v1"));
    dbfull()->TEST_CompactMemTable();
    ASSERT_LEVELDB_OK(Put("foo", "v2"));
    dbfull()->TEST_CompactMemTable();
    ASSERT_EQ("v2", Get("foo"));

}


TEST_F(DBTest, GetOrderedByLevels) {

    ASSERT_LEVELDB_OK(Put("foo", "v1"));
    Compact("a", "z");
    ASSERT_EQ("v1", Get("foo"));
    ASSERT_LEVELDB_OK(Put("foo", "v2"));
    ASSERT_EQ("v2", Get("foo"));
    dbfull()->TEST_CompactMemTable();
    ASSERT_EQ("v2", Get("foo"));

}


TEST_F(DBTest, ManualCompaction) {
  ASSERT_EQ(config::kMaxMemCompactLevel, 2)
      << "Need to update this test to match kMaxMemCompactLevel";

  MakeTables(3, "p", "q");
  ASSERT_EQ("1,1,1", TablesPerLevel());

  // Compaction range falls before files
  Compact("", "c");
  ASSERT_EQ("1,1,1", TablesPerLevel());

  // Compaction range falls after files
  Compact("r", "z");
  ASSERT_EQ("1,1,1", TablesPerLevel());

  // Compaction range overlaps files
  Compact("p1", "p9");
  ASSERT_EQ("0,0,1", TablesPerLevel());

  // Populate a different range
  MakeTables(3, "c", "e");
  ASSERT_EQ("1,1,2", TablesPerLevel());

  // Compact just the new range
  Compact("b", "f");
  ASSERT_EQ("0,0,2", TablesPerLevel());

  // Compact all
  MakeTables(1, "a", "z");
  ASSERT_EQ("0,1,2", TablesPerLevel());
  db_->CompactRange(nullptr, nullptr);
  ASSERT_EQ("0,0,1", TablesPerLevel());
}



TEST_F(DBTest, CompactAtLevel0){
    ASSERT_LEVELDB_OK(Put("a", "va"));
    dbfull()->TEST_CompactMemTable();
    ASSERT_LEVELDB_OK(Put("b", "vb"));  
    //dbfull()->TEST_CompactMemTable();
    Compact("a","c");

}




/*

TEST_F(DBTest, CompactionsGenerateMultipleFiles) {
  Options options 
  options.write_buffer_size = 100000000;  // Large write buffer
  Reopen(&options);

  Random rnd(301);

  // Write 8MB (80 values, each 100K)
  ASSERT_EQ(NumTableFilesAtLevel(0), 0);
  std::vector<std::string> values;
  for (int i = 0; i < 80; i++) {
    values.push_back(RandomString(&rnd, 100000));
    ASSERT_LEVELDB_OK(Put(Key(i), values[i]));
  }

  // Reopening moves updates to level-0
  Reopen(&options);
  dbfull()->TEST_CompactRange(0, nullptr, nullptr);

  ASSERT_EQ(NumTableFilesAtLevel(0), 0);
  ASSERT_GT(NumTableFilesAtLevel(1), 1);
  for (int i = 0; i < 80; i++) {
    ASSERT_EQ(Get(Key(i)), values[i]);
  }
}
*/
    
} // namespace leveldb

int main(int argc, char** argv)
{
    testing::InitGoogleTest(&argc, argv);

    return RUN_ALL_TESTS();
}