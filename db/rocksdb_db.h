#ifndef YCSB_C_ROCKS_DB_H_
#define YCSB_C_ROCKS_DB_H_

#include "core/db.h"

#include <iostream>
#include <string>
#include "rocksdb/db.h"
#include "rocksdb/slice.h"  
#include "rocksdb/options.h" 

using std::cout;
using std::endl;

namespace ycsbc {

class RocksDB : public DB {
 public:
  RocksDB();

  ~RocksDB();

  int Read(const std::string &table, const std::string &key,
           const std::vector<std::string> *fields,
           std::vector<KVPair> &result);

  int Scan(const std::string &table, const std::string &key,
           int len, const std::vector<std::string> *fields,
           std::vector<std::vector<KVPair>> &result);

  int Update(const std::string &table, const std::string &key,
             std::vector<KVPair> &values);

  int Insert(const std::string &table, const std::string &key,
             std::vector<KVPair> &values) {
    return Update(table, key, values);
  }

  int Delete(const std::string &table, const std::string &key);

 private:
  rocksdb::DB * db_;
};

} // ycsbc

#endif // YCSB_C_ROCKS_DB_H_
