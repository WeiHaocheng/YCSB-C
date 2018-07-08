#ifndef PMEMKV_DB_H
#define PMEMKV_DB_H


#include "../core/db.h"
#include "libpmemkv.h"
#include <iostream>

#define LOG(msg) std::cout << msg << "\n"

using std::cout;
using std::endl;
using namespace pmemkv;
//class KVEngine;

namespace ycsbc {


class PmemKV : public DB
{

public:
    PmemKV();

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


    ~PmemKV();


private:
    KVEngine* kv;

};


}//namespace ycsbc
#endif // PMEMKV_DB_H
