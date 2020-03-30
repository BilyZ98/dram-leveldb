#include "leveldb/db.h"
#include <string>
#include <iostream>

using namespace leveldb;

int main()
{
    
    leveldb::DB* db;
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open(options, "/tmp/testdb", &db);
    assert(status.ok());


    if(status.ok())
    {
        int entry_num = 10000;
        
        for(int i =0; i < entry_num; i++) {
            std::string val = std::to_string(i);
            std::string key = std::to_string(i);
            db->Put(WriteOptions(),key, val);
        }
        /*
        std::string k1 = "kkk";
        std::string val1 = "vvv";
        Status s = db->Put(WriteOptions(), k1, val1);

        if(s.ok()) 
        {
            std::cout<<"hello"<<std::endl;
        }
        */
       std::string value;
       std::string key = std::to_string(0);
       db->Get(ReadOptions(), key, &value);
       std::cout<< "hello:" << value <<std::endl;

    }
    
    return 0;
}