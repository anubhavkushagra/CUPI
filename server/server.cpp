// #include <iostream>
// #include <vector>
// #include <thread>
// #include <fstream>
// #include <sstream>
// #include <rocksdb/db.h>
// #include <rocksdb/options.h>
// #include <rocksdb/utilities/optimistic_transaction_db.h>
// #include <rocksdb/utilities/transaction.h>

// #include <grpcpp/grpcpp.h>
// #include "kv.grpc.pb.h"

// using namespace rocksdb;

// // Pre-defined for faster metadata lookup
// const std::string AUTH_HEADER = "authorization";
// const std::string AUTH_TOKEN = "rocksdb-super-secret-key-2026";

// // --- SECURITY HELPER: Read Certificate Files ---
// std::string ReadFile(const std::string& filename) {
//     std::ifstream ifs(filename);
//     if (!ifs.is_open()) {
//         std::cerr << "CRITICAL ERROR: Failed to open " << filename << std::endl;
//         exit(1);
//     }
//     return std::string((std::istreambuf_iterator<char>(ifs)),
//                        std::istreambuf_iterator<char>());
// }

// class ServerImpl final {
// public:
//     void Run() {
//         Options options;
//         options.create_if_missing = true;

//         // --- ROCKSDB HIGH-THROUGHPUT TUNING ---
//         options.IncreaseParallelism(); 
//         options.OptimizeLevelStyleCompaction();
        
//         // Memory Buffers: Increase size to handle massive ingestion
//         options.write_buffer_size = 256 * 1024 * 1024; // 256MB MemTable
//         options.max_write_buffer_number = 4;
        
//         // Pipelining: Allows multiple threads to prep writes in parallel
//         options.enable_pipelined_write = true;
//       //  options.unordered_write = true; // Fastest possible write mode for concurrent operations
//         options.allow_concurrent_memtable_write = true;

//         // Open as OptimisticTransactionDB
//         Status s = OptimisticTransactionDB::Open(options, "./data", &txn_db_);
//         if (!s.ok()) {
//             std::cerr << "Failed to open TransactionDB: " << s.ToString() << std::endl;
//             exit(1);
//         }

//         // --- SECURE CHANNEL CONFIG ---
//         grpc::SslServerCredentialsOptions ssl_opts;
//         ssl_opts.pem_key_cert_pairs.push_back({ReadFile("server.key"), ReadFile("server.crt")});

//         grpc::ServerBuilder builder;
//         builder.AddListeningPort("0.0.0.0:50051", grpc::SslServerCredentials(ssl_opts));
//         builder.RegisterService(&service_);

//         // Align Completion Queues with Hardware Threads
//         int threads = std::thread::hardware_concurrency();
//         for (int i = 0; i < threads; i++) cqs_.emplace_back(builder.AddCompletionQueue());

//         server_ = builder.BuildAndStart();
//         std::cout << "1M+ TPS Optimized Transactional Server Live." << std::endl;

//         std::vector<std::thread> workers;
//         for (auto& cq : cqs_) workers.emplace_back(&ServerImpl::HandleRpcs, this, cq.get());
//         for (auto& t : workers) t.join();
//     }

// private:
//     struct CallData {
//         kv::KVService::AsyncService* service;
//         grpc::ServerCompletionQueue* cq;
//         OptimisticTransactionDB* txn_db;
//         grpc::ServerContext ctx;
//         kv::BatchRequest req;
//         kv::BatchResponse resp;
//         grpc::ServerAsyncResponseWriter<kv::BatchResponse> responder;
//         enum { CREATE, PROCESS, FINISH } status;

//         CallData(kv::KVService::AsyncService* s, grpc::ServerCompletionQueue* c, OptimisticTransactionDB* d)
//             : service(s), cq(c), txn_db(d), responder(&ctx), status(CREATE) { Proceed(); }

//         void Proceed() {
//             if (status == CREATE) {
//                 status = PROCESS;
//                 service->RequestExecuteBatch(&ctx, &req, &responder, cq, cq, this);
//             } else if (status == PROCESS) {
//                 new CallData(service, cq, txn_db);

//                 // --- OPTIMIZED AUTH CHECK ---
//                 auto& client_metadata = ctx.client_metadata();
//                 auto it = client_metadata.find(AUTH_HEADER);
//                 if (it == client_metadata.end() || it->second != AUTH_TOKEN) {
//                     status = FINISH;
//                     responder.Finish(resp, grpc::Status(grpc::StatusCode::UNAUTHENTICATED, "Denied"), this);
//                     return;
//                 }

//                 // --- TRANSACTIONAL BATCH PROCESSING ---
//                 WriteOptions write_options;
//                 // Set to false for benchmark speed (risks data loss on crash, but hits 1M+ TPS)
//                 write_options.disableWAL = true; 
                
//                 OptimisticTransactionOptions txn_options;
//                 txn_options.set_snapshot = true; // Provides consistent point-in-time view

//                 Transaction* txn = txn_db->BeginTransaction(write_options, txn_options);

//                 ReadOptions read_opts;
//                 for (const auto& entry : req.entries()) {
//                     if (entry.type() == kv::PUT) {
//                         txn->Put(entry.key(), entry.value());
//                     } else if (entry.type() == kv::DELETE) {
//                         txn->Delete(entry.key());
//                     } else if (entry.type() == kv::GET) {
//                         std::string val;
//                         // GetForUpdate prevents race conditions by tracking this key's version
//                         txn->GetForUpdate(read_opts, entry.key(), &val);
//                         resp.add_values(val);
//                     }
//                 }

//                 Status s = txn->Commit();
//                 resp.set_success(s.ok());

//                 if (s.ok()) {
//                     responder.Finish(resp, grpc::Status::OK, this);
//                 } else {
//                     // Send ABORTED so the client knows to trigger its Retry/Backoff logic
//                     responder.Finish(resp, grpc::Status(grpc::StatusCode::ABORTED, "Conflict Detected"), this);
//                 }
                
//                 delete txn;
//                 status = FINISH;
//             } else { 
//                 delete this; 
//             }
//         }
//     };

//     void HandleRpcs(grpc::ServerCompletionQueue* cq) {
//         new CallData(&service_, cq, txn_db_);
//         void* tag; bool ok;
//         while (cq->Next(&tag, &ok)) {
//             if (ok) static_cast<CallData*>(tag)->Proceed();
//         }
//     }

//     std::vector<std::unique_ptr<grpc::ServerCompletionQueue>> cqs_;
//     kv::KVService::AsyncService service_;
//     std::unique_ptr<grpc::Server> server_;
//     OptimisticTransactionDB* txn_db_;
// };

// int main() { ServerImpl s; s.Run(); return 0; }



// update 2 - Added SSL/TLS support to the server for secure communication with clients.




// #include <iostream>
// #include <vector>
// #include <thread>
// #include <fstream>
// #include <rocksdb/db.h>
// #include <rocksdb/options.h>
// #include <rocksdb/utilities/optimistic_transaction_db.h>
// #include <rocksdb/utilities/transaction.h>
// #include <grpcpp/grpcpp.h>
// #include <grpcpp/resource_quota.h>
// #include "kv.grpc.pb.h"

// using namespace rocksdb;

// const std::string AUTH_TOKEN = "rocksdb-super-secret-key-2026";

// std::string ReadFile(const std::string& filename) {
//     std::ifstream ifs(filename);
//     return std::string((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());
// }

// class ServerImpl final {
// public:
//     void Run() {
//         Options options;
//         options.create_if_missing = true;
        
//         // --- ROCKSDB AGGRESSIVE TUNING ---
//         int num_cores = std::thread::hardware_concurrency();
//         options.IncreaseParallelism(num_cores);
//         options.OptimizeLevelStyleCompaction();
        
//         // 512MB Memtable for fewer flushes
//         options.write_buffer_size = 512 * 1024 * 1024; 
//         options.max_write_buffer_number = 8;
//         options.min_write_buffer_number_to_merge = 2;
        
//         // Disable WAL and use Pipelined Writes for 1M+ TPS
//         options.enable_pipelined_write = true;
//         options.allow_concurrent_memtable_write = true;
//         options.max_background_compactions = 4;

//         Status s = OptimisticTransactionDB::Open(options, "./data", &txn_db_);
//         if (!s.ok()) {
//             std::cerr << "DB Error: " << s.ToString() << std::endl;
//             exit(1);
//         }

//         w_opts.disableWAL = true; 
//         w_opts.sync = false;

//         // --- gRPC RESOURCE OPTIMIZATION ---
//         grpc::ResourceQuota quota;
//         quota.SetMaxThreads(num_cores * 2);

//         grpc::SslServerCredentialsOptions ssl_opts;
//         ssl_opts.pem_key_cert_pairs.push_back({ReadFile("server.key"), ReadFile("server.crt")});

//         grpc::ServerBuilder builder;
//         builder.SetResourceQuota(quota);
//         // Increase message limit for huge batches
//         builder.SetMaxReceiveMessageSize(128 * 1024 * 1024); 
//         builder.AddListeningPort("0.0.0.0:50051", grpc::SslServerCredentials(ssl_opts));
//         builder.RegisterService(&service_);

//         for (int i = 0; i < num_cores; i++) {
//             cqs_.emplace_back(builder.AddCompletionQueue());
//         }

//         server_ = builder.BuildAndStart();
//         std::cout << ">>> TURBO MODE ACTIVE: Targeting 1.2M+ TPS <<<" << std::endl;

//         std::vector<std::thread> workers;
//         for (auto& cq : cqs_) {
//             workers.emplace_back([this, cq_ptr = cq.get()]() {
//                 // Initialize the pipeline
//                 for(int i=0; i<100; ++i) { 
//                     new CallData(&service_, cq_ptr, txn_db_, &w_opts);
//                 }
                
//                 void* tag;
//                 bool ok;
//                 while (cq_ptr->Next(&tag, &ok)) {
//                     if (ok) static_cast<CallData*>(tag)->Proceed();
//                 }
//             });
//         }
//         for (auto& t : workers) t.join();
//     }

// private:
//     struct CallData {
//         kv::KVService::AsyncService* service;
//         grpc::ServerCompletionQueue* cq;
//         OptimisticTransactionDB* txn_db;
//         WriteOptions* w_opts;
//         grpc::ServerContext ctx;
//         kv::BatchRequest req;
//         kv::BatchResponse resp;
//         grpc::ServerAsyncResponseWriter<kv::BatchResponse> responder;
//         enum { CREATE, PROCESS, FINISH } status;

//         CallData(kv::KVService::AsyncService* s, grpc::ServerCompletionQueue* c, 
//                  OptimisticTransactionDB* d, WriteOptions* wo)
//             : service(s), cq(c), txn_db(d), w_opts(wo), responder(&ctx), status(CREATE) {
//             Proceed();
//         }

//         void Proceed() {
//             if (status == CREATE) {
//                 status = PROCESS;
//                 service->RequestExecuteBatch(&ctx, &req, &responder, cq, cq, this);
//             } else if (status == PROCESS) {
//                 // SPAWN IMMEDIATELY: Don't wait for DB write to accept next RPC
//                 new CallData(service, cq, txn_db, w_opts);

//                 auto const& md = ctx.client_metadata();
//                 auto it = md.find("authorization");
//                 if (it == md.end() || std::string(it->second.data(), it->second.size()) != AUTH_TOKEN) {
//                     responder.Finish(resp, grpc::Status(grpc::StatusCode::UNAUTHENTICATED, "Deny"), this);
//                 } else {
//                     // --- OPTIMIZED BATCH WRITE ---
//                     Transaction* txn = txn_db->BeginTransaction(*w_opts);
//                     for (const auto& entry : req.entries()) {
//                         if (entry.type() == kv::PUT) txn->Put(entry.key(), entry.value());
//                     }
//                     Status s = txn->Commit();
//                     resp.set_success(s.ok());
//                     delete txn;

//                     responder.Finish(resp, grpc::Status::OK, this);
//                 }
//                 status = FINISH;
//             } else {
//                 delete this;
//             }
//         }
//     };

//     std::vector<std::unique_ptr<grpc::ServerCompletionQueue>> cqs_;
//     kv::KVService::AsyncService service_;
//     std::unique_ptr<grpc::Server> server_;
//     OptimisticTransactionDB* txn_db_;
//     WriteOptions w_opts;
// };

// int main() {
//     ServerImpl s;
//     s.Run();
//     return 0;
// }




#include <iostream>
#include <vector>
#include <thread>
#include <fstream>
#include <unordered_map>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/utilities/optimistic_transaction_db.h>
#include <rocksdb/utilities/transaction.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/resource_quota.h>
#include <jwt-cpp/jwt.h>
#include "kv.grpc.pb.h"

using namespace rocksdb;

const std::string MASTER_SECRET = "super-secret-server-key-256";

bool VerifyJWTCached(const std::string& token_str) {
    // THE FIX: 'thread_local' means every worker thread gets its own private cache!
    // NO MUTEX NEEDED. NO WAITING IN LINE.
    thread_local std::unordered_map<std::string, std::chrono::time_point<std::chrono::steady_clock>> local_cache;
    
    auto now = std::chrono::steady_clock::now();
    
    // 1. Check the thread's private cache
    auto it = local_cache.find(token_str);
    if (it != local_cache.end()) {
        if (now < it->second) return true; // Still valid!
        local_cache.erase(it); // Expired
    }

    // 2. Heavy math only if it's not in this specific thread's cache
    try {
        auto decoded = jwt::decode(token_str);
        auto verifier = jwt::verify()
            .allow_algorithm(jwt::algorithm::hs256{MASTER_SECRET})
            .with_issuer("kv_server");
        verifier.verify(decoded);
        
        // 3. Save to the private cache
        local_cache[token_str] = now + std::chrono::seconds(60);
        return true;
    } catch (...) {
        return false; 
    }
}

std::string ReadFile(const std::string& filename) {
    std::ifstream ifs(filename);
    return std::string((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());
}

// Base class so our worker loop can process both Logins and Batches
class CallDataBase {
public:
    virtual void Proceed() = 0;
    virtual ~CallDataBase() = default;
};

class ServerImpl final {
public:
    void Run() {
        Options options;
        options.create_if_missing = true;
        
        // --- ROCKSDB AGGRESSIVE TUNING ---
        int num_cores = std::thread::hardware_concurrency();
        options.IncreaseParallelism(num_cores);
        options.OptimizeLevelStyleCompaction();
        // ADD THIS LINE: Maximize background threads to prevent latency stalls
        options.max_background_jobs = num_cores; 
        
        // 512MB Memtable for fewer flushes
        options.write_buffer_size = 512 * 1024 * 1024; 
        options.max_write_buffer_number = 8;
        options.min_write_buffer_number_to_merge = 2;
        
        // Disable WAL and use Pipelined Writes for 1M+ TPS
        options.enable_pipelined_write = true;
        options.allow_concurrent_memtable_write = true;
        options.max_background_compactions = 4;

        Status s = OptimisticTransactionDB::Open(options, "./data", &txn_db_);
        if (!s.ok()) {
            std::cerr << "DB Error: " << s.ToString() << std::endl;
            exit(1);
        }

        w_opts.disableWAL = true; 
        w_opts.sync = false;

        // --- gRPC RESOURCE OPTIMIZATION ---
        grpc::ResourceQuota quota;
        quota.SetMaxThreads(num_cores * 2);

        grpc::SslServerCredentialsOptions ssl_opts;
        ssl_opts.pem_key_cert_pairs.push_back({ReadFile("server.key"), ReadFile("server.crt")});

        grpc::ServerBuilder builder;
        builder.SetResourceQuota(quota);
        builder.SetMaxReceiveMessageSize(128 * 1024 * 1024); 
        builder.AddListeningPort("0.0.0.0:50051", grpc::SslServerCredentials(ssl_opts));
        builder.RegisterService(&service_);

        for (int i = 0; i < num_cores; i++) {
            cqs_.emplace_back(builder.AddCompletionQueue());
        }

        server_ = builder.BuildAndStart();
        std::cout << ">>> TURBO MODE + JWT CACHE ACTIVE: Targeting 1.2M+ TPS <<<" << std::endl;

        std::cout << ">>> TURBO MODE + MULTI-THREADED CQs ACTIVE <<<" << std::endl;

        std::vector<std::thread> workers;
        
        // --- THE FIX: Multi-Threaded Completion Queues ---
        // Spawn 4 threads per core. If you have 8 cores, this creates 32 threads.
        // If RocksDB pauses one thread, the others keep draining the network!
        int threads_per_core = 4; 
        
        for (auto& cq : cqs_) {
            for (int i = 0; i < threads_per_core; i++) {
                workers.emplace_back([this, cq_ptr = cq.get()]() {
                    
                    // Pre-allocate a safe amount of CallData per thread
                    for(int j=0; j<250; ++j) { 
                        new LoginCallData(&service_, cq_ptr);
                        new BatchCallData(&service_, cq_ptr, txn_db_, &w_opts);
                    }
                    
                    void* tag;
                    bool ok;
                    // gRPC safely handles multiple threads pulling from the exact same queue!
                    while (cq_ptr->Next(&tag, &ok)) {
                        if (ok) static_cast<CallDataBase*>(tag)->Proceed();
                    }
                });
            }
        }
        for (auto& t : workers) t.join();
    }

private:
    // --- STATE MACHINE 1: LOGIN ---
    struct LoginCallData : public CallDataBase {
        kv::KVService::AsyncService* service;
        grpc::ServerCompletionQueue* cq;
        grpc::ServerContext ctx;
        kv::LoginRequest req;
        kv::LoginResponse resp;
        grpc::ServerAsyncResponseWriter<kv::LoginResponse> responder;
        enum { CREATE, PROCESS, FINISH } status;

        LoginCallData(kv::KVService::AsyncService* s, grpc::ServerCompletionQueue* c)
            : service(s), cq(c), responder(&ctx), status(CREATE) { Proceed(); }

        void Proceed() override {
            if (status == CREATE) {
                status = PROCESS;
                service->RequestLogin(&ctx, &req, &responder, cq, cq, this);
            } else if (status == PROCESS) {
                new LoginCallData(service, cq); // Spawn next

                // Generate token if API key matches
                if (req.api_key() == "initial-pass") {
                    auto token = jwt::create()
                        .set_issuer("kv_server")
                        .set_subject(req.client_id())
                        .set_issued_at(std::chrono::system_clock::now())
                        .set_expires_at(std::chrono::system_clock::now() + std::chrono::hours(1))
                        .sign(jwt::algorithm::hs256{MASTER_SECRET});
                    resp.set_success(true);
                    resp.set_jwt_token(token);
                } else {
                    resp.set_success(false);
                    resp.set_error_message("Invalid API Key");
                }
                status = FINISH;
                responder.Finish(resp, grpc::Status::OK, this);
            } else { delete this; }
        }
    };

    // --- STATE MACHINE 2: HIGH SPEED BATCH ---
    struct BatchCallData : public CallDataBase {
        kv::KVService::AsyncService* service;
        grpc::ServerCompletionQueue* cq;
        OptimisticTransactionDB* txn_db;
        WriteOptions* w_opts;
        grpc::ServerContext ctx;
        kv::BatchRequest req;
        kv::BatchResponse resp;
        grpc::ServerAsyncResponseWriter<kv::BatchResponse> responder;
        enum { CREATE, PROCESS, FINISH } status;

        BatchCallData(kv::KVService::AsyncService* s, grpc::ServerCompletionQueue* c, 
                      OptimisticTransactionDB* d, WriteOptions* wo)
            : service(s), cq(c), txn_db(d), w_opts(wo), responder(&ctx), status(CREATE) { Proceed(); }

        void Proceed() override {
            if (status == CREATE) {
                status = PROCESS;
                service->RequestExecuteBatch(&ctx, &req, &responder, cq, cq, this);
            } else if (status == PROCESS) {
                new BatchCallData(service, cq, txn_db, w_opts); // Spawn next

                auto const& md = ctx.client_metadata();
                auto it = md.find("authorization");
                
                // Fast JWT Verification using Cache
                if (it == md.end() || !VerifyJWTCached(std::string(it->second.data(), it->second.size()))) {
                    responder.Finish(resp, grpc::Status(grpc::StatusCode::UNAUTHENTICATED, "Deny"), this);
                } else {
                    // --- OPTIMIZED BATCH WRITE ---
                    Transaction* txn = txn_db->BeginTransaction(*w_opts);
                    for (const auto& entry : req.entries()) {
                        if (entry.type() == kv::PUT) txn->Put(entry.key(), entry.value());
                    }
                    Status s = txn->Commit();
                    delete txn;

                    if (!s.ok()) {
                        // THE FIX: If the database rejects the write due to a conflict, 
                        // instantly abort the gRPC request so the client knows it failed!
                        responder.Finish(resp, grpc::Status(grpc::StatusCode::ABORTED, "Optimistic Lock Conflict"), this);
                    } else {
                        resp.set_success(true);
                        responder.Finish(resp, grpc::Status::OK, this);
                    }
                }
                status = FINISH;
            } else { delete this; }
        }
    };

    std::vector<std::unique_ptr<grpc::ServerCompletionQueue>> cqs_;
    kv::KVService::AsyncService service_;
    std::unique_ptr<grpc::Server> server_;
    OptimisticTransactionDB* txn_db_;
    WriteOptions w_opts;
};

int main() {
    ServerImpl s;
    s.Run();
    return 0;
}