#ifndef YCSB_C_ROCKS_DB_H_
#define YCSB_C_ROCKS_DB_H_

#include "core/db.h"

#include <iostream>
#include <string>
#include "rocksdb/db.h"
#include "rocksdb/slice.h"  
#include "rocksdb/options.h" 


//#include "util/cachestat_ebpf.h"

#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <stdio.h>
#include <signal.h>

#include <thread>//cyf add for test probe linux kernel function

using std::cout;
using std::endl;

namespace ycsbc {

template <typename T>
class Probe_Timer {
 public:
  void Start() {
    time_ = Clock::now();
  }

  T End() {
    Duration span;
    Clock::time_point t = Clock::now();
    span = std::chrono::duration_cast<Duration>(t - time_);
    return span.count();
  }

 private:
  //typedef std::chrono::high_resolution_clock Clock;
  typedef std::chrono::steady_clock Clock;
  typedef std::chrono::duration<T> Duration;

  Clock::time_point time_;
};

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

  void Init(){std::cout<<"initial rocksdb......"<<std::endl;}
  void Close(){std::cout<<"finished rocksdb......"<<std::endl;}

 private:
  rocksdb::DB * db_;
  pthread_t pth;

  static uint64_t put_num;
  static uint64_t get_num;
  static void* BCC_BGWork(void* db);
};

} // ycsbc

#endif // YCSB_C_ROCKS_DB_H_
