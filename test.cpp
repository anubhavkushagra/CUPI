#include <rocksdb/db.h>
#include <iostream>

int main() {
    rocksdb::DB* db;
    rocksdb::Options options;
    options.create_if_missing = true;

    rocksdb::Status status =
        rocksdb::DB::Open(options, "./data", &db);

    if (!status.ok()) {
        std::cerr << status.ToString() << std::endl;
        return 1;
    }

    db->Put(rocksdb::WriteOptions(), "hello", "world");

    std::string value;
    db->Get(rocksdb::ReadOptions(), "hello", &value);

    std::cout << value << std::endl;

    delete db;
    return 0;
}
