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
  RocksDB() {
    std::string kDBPath = "/tmp/rocksdb_YCSB";
    rocksdb::Options options; 
    options.create_if_missing = true;
    rocksdb::Status s = rocksdb::DB::Open(options ,kDBPath ,&db_);
    assert(s.ok());
  }

  ~RocksDB() {
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
             std::vector<KVPair> &values) {
    return Update(table, key, values);
  }

  int Delete(const std::string &table, const std::string &key) {
    return DB::kOK;
  }

 private:
  rocksdb::DB * db_;
};

} // ycsbc

#endif // YCSB_C_ROCKS_DB_H_