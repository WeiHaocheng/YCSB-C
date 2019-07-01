#ifndef LEVELDB_DB_H
#define LEVELDB_DB_H

#include "core/db.h"

#include <iostream>
#include <string>

#include "leveldb/cache.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/write_batch.h"



namespace ycsbc {

class Level_DB : public DB {
public:
    Level_DB();

    ~Level_DB(){
        delete db_;
    }

    int Read(const std::string &table, const std::string &key,
             const std::vector<std::string> *fields,
             std::vector<KVPair> &result);

    int Scan(const std::string &table, const std::string &key,
             int len, const std::vector<std::string> *fields,
             std::vector<std::vector<KVPair>> &result);

    int Update(const std::string &table, const std::string &key,
               std::vector<KVPair> &values);

    int Insert(const std::string &table, const std::string &key,
               std::vector<KVPair> &values);

    int Delete(const std::string &table, const std::string &key);
      //return ycsbc::DB::kOK;

    void Init(){std::cout<<"initial leveldb......"<<std::endl;}
    void Close(){std::cout<<"finished leveldb......"<<std::endl;}




private:
    leveldb::DB* db_;
};

}//namespace ycsbc

#endif // LEVELDB_DB_H
