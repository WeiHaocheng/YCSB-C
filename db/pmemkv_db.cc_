#include "pmemkv_db.h"

using namespace pmemkv;

namespace ycsbc{
PmemKV::~PmemKV()
{
    delete kv;
}

PmemKV::PmemKV()
{
    std::string kvPath = "/mnt/pmem/pmemkv";
    std::string kvEngineType = "kvtree2";
    LOG("opening pmemkv database instance");
    kv = KVEngine::Open(kvEngineType,kvPath,PMEMOBJ_MIN_POOL);
}

int PmemKV::Read(const std::string &table, const std::string &key,
                 const std::vector<std::string> *fields,
                 std::vector<KVPair> &result)
{
    std::string value;
    KVStatus s = kv->Get(key,&value);
    assert(s==OK);
    result.push_back(std::make_pair("",value));
    LOG("Read operation!");
    return DB::kOK;

}

int PmemKV::Scan(const std::string &table, const std::string &key,
                 int len, const std::vector<std::string> *fields,
                 std::vector<std::vector<KVPair> > &result)
{
    LOG("No time to code....");

}

int PmemKV::Update(const std::string &table, const std::string &key,
                   std::vector<KVPair> &values)
{

    KVStatus s = kv->Put(key,values[0].second);
    assert(s==OK);
    LOG("Update operation!");
    return DB::kOK;


}

int PmemKV::Insert(const std::string &table, const std::string &key,
                   std::vector<KVPair> &values)
{
    KVStatus s = kv->Put(key,values[0].second);
    assert(s==OK);
    LOG("Insert operation!");
    return DB::kOK;

}

int PmemKV::Delete(const std::string &table, const std::string &key)
{
    KVStatus s = kv->Remove(key);
    assert(s==OK);
    LOG("Remove operation!");
    return DB::kOK;

}


}//namespace_ycsbc





