#include "db/rocksdb_db.h"
#include "rocksdb/iterator.h"
#include "rocksdb/table.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/rate_limiter.h"

namespace ycsbc{

uint64_t RocksDB::put_num = 0;
uint64_t RocksDB::get_num = 0;

RocksDB::RocksDB()
{
        pth = NULL;
        std::string kDBPath = "/mnt/ssd/ldb_2pc";
        rocksdb::Options options;
        options.target_file_size_base = 4 << 20;
        options.target_file_size_multiplier = 1;
        options.write_buffer_size  = 2* options.target_file_size_base;//cyf : Memtable is 2 times larger than SST
        options.max_bytes_for_level_base = 4 * options.target_file_size_base;//default LDB's SSTable is 4MB * 4
        options.max_open_files  = 1000;
        options.max_file_opening_threads = 1;
        options.max_background_jobs = 1;
        options.level0_slowdown_writes_trigger = 8;
        options.level0_stop_writes_trigger = 12;
        options.level0_file_num_compaction_trigger = 4;
        options.max_write_buffer_number = 2;
        options.min_write_buffer_number_to_merge = 1;
        options.compression = rocksdb::kSnappyCompression;
        //cyf add for change bloomfilter
        rocksdb::BlockBasedTableOptions table_options;
        table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10,true));//set true for the same as LevelDB
        options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
        //cyf add for select comapction mechanism
        options.target_file_size_base = options.max_bytes_for_level_base/4;

        for(auto iter = options.compression_per_level.begin();iter != options.compression_per_level.end();iter++)
            *iter = rocksdb::kSnappyCompression;
        options.compaction_style = rocksdb::kCompactionStyleLevel;
        //options.compaction_style = rocksdb::kCompactionStyleUniversal;

        bool auto_tune = false;
        if(auto_tune)
        {
           options.rate_limiter.reset(rocksdb::NewGenericRateLimiter(
                                          1000*(1<<20),100*1000,10,
                                          rocksdb::RateLimiter::Mode::kWritesOnly,true));



        }







        options.create_if_missing = true;
        rocksdb::Status s = rocksdb::DB::Open(options ,kDBPath ,&db_);
        assert(s.ok());
        if(pth == NULL)
            pthread_create(&pth,NULL,BCC_BGWork,(void*)this);

}

RocksDB::~RocksDB()
{
    delete db_;

}

int RocksDB::Read(const std::string &table, const std::string &key,
           const std::vector<std::string> *fields,
           std::vector<KVPair> &result) {
			std::string value;
			rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), key, &value);
			result.push_back(std::make_pair("", value));
            RocksDB::get_num++;
		return DB::kOK;
	}

	int RocksDB::Scan(const std::string &table, const std::string &key,
           int len, const std::vector<std::string> *fields,
           std::vector<std::vector<KVPair>> &result) {
			std::vector<KVPair> values;
			rocksdb::ReadOptions options;
			options.tailing =false;
            rocksdb::Iterator* iter = db_->NewIterator(rocksdb::ReadOptions());
			iter->Seek(key);
			for(size_t i = 0; i < len && iter->Valid(); i++) {
                //values.push_back(iter->value().Encode());
				values.push_back(std::make_pair("", iter->value().ToString()));
                iter->Next();
			}
			result.push_back(values);
            RocksDB::get_num++;
            delete iter;
		return DB::kOK;
	}

	int RocksDB::Update(const std::string &table, const std::string &key,
             std::vector<KVPair> &values) {

        rocksdb::Status s = db_->Put(rocksdb::WriteOptions(),
                                     key, values[0].second);
        RocksDB::put_num++;
        return DB::kOK;
    }

    int RocksDB::Delete(const std::string &table, const std::string &key)
    {
        rocksdb::Status s = db_->Delete(rocksdb::WriteOptions(),key);
        return DB::kOK;
    }

    void *RocksDB::BCC_BGWork(void *db)
    {
        pthread_setcancelstate(PTHREAD_CANCEL_ENABLE,NULL);
        std::cout <<"BCC_BGWork is running~" <<std::endl;
        uint64_t tmp_put_num = 0;
        uint64_t tmp_get_num = 0;
        uint64_t dlta_put_num = 0;
        uint64_t dlta_get_num = 0;

        double probe_time = 0.0;
        Probe_Timer<double> probe_timer;
        while(true)
        {
            probe_timer.Start();
            tmp_put_num = RocksDB::put_num;
            tmp_get_num = RocksDB::get_num;
            sleep(5);//cyf add for sleep 5s
            dlta_put_num = RocksDB::put_num - tmp_put_num;
            dlta_get_num = RocksDB::get_num - tmp_get_num;
            probe_time = probe_timer.End();

            std::cout << "#_Transaction_throughput_(KTPS): \t"
                <<(dlta_get_num + dlta_put_num) / probe_time / 1000
               <<std::endl;
        }
    }
}
