#include "db/rocksdb_db.h"
#include "rocksdb/iterator.h"

namespace ycsbc{
	int RocksDB::Read(const std::string &table, const std::string &key,
           const std::vector<std::string> *fields,
           std::vector<KVPair> &result) {
			std::string value;
			rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), key, &value);
			result.push_back(std::make_pair("", value));
		return DB::kOK;
	}

	int RocksDB::Scan(const std::string &table, const std::string &key,
           int len, const std::vector<std::string> *fields,
           std::vector<std::vector<KVPair>> &result) {
			std::vector<KVPair> values;
			rocksdb::ReadOptions options;
			options.tailing =false;
			rocksdb::Iterator* iter = db_->NewIterator(options);
			iter->Seek(key);
			for(size_t i = 0; i < len && iter->Valid(); i++) {
				//values.push_back(iter->value().Encode());
				values.push_back(std::make_pair("", iter->value().ToString()));
                iter->Next();
			}
			result.push_back(values);
		return DB::kOK;
	}

	int RocksDB::Update(const std::string &table, const std::string &key,
             std::vector<KVPair> &values) {

        rocksdb::Status s = db_->Put(rocksdb::WriteOptions(),
                                     key, values[0].second);
        return DB::kOK;
    }

    int RocksDB::Delete(const std::string &table, const std::string &key)
    {
        rocksdb::Status s = db_->Delete(rocksdb::WriteOptions(),key);
        return DB::kOK;
    }
}
