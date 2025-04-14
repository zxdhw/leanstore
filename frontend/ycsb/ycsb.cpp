#include "Time.hpp"
#include "Units.hpp"
#include "leanstore/BTreeAdapter.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/LeanStore.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/profiling/counters/ThreadCounters.hpp"
#include "leanstore/utils/FVector.hpp"
#include "leanstore/utils/Files.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "leanstore/utils/ScrambledZipfGenerator.hpp"
// -------------------------------------------------------------------------------------
#include "leanstore/concurrency/Mean.hpp"
#include "leanstore/io/IoInterface.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
// -------------------------------------------------------------------------------------
#include <cstdint>
#include <iostream>
#include <set>
// -------------------------------------------------------------------------------------
DEFINE_uint32(ycsb_read_ratio, 100, "");
DEFINE_uint32(ycsb_scan_ratio, 0, "");
DEFINE_uint32(ycsb_type, 1, ""); // 1: A/B/C 2:D; 3:E; 4:F;
DEFINE_uint64(ycsb_tuple_count, 0, "");
DEFINE_uint32(ycsb_payload_size, 100, "tuple size in bytes");
DEFINE_uint32(ycsb_warmup_rounds, 0, "");
DEFINE_uint32(ycsb_tx_rounds, 1, "");
DEFINE_uint32(ycsb_tx_count, 0, "default = tuples");
DEFINE_bool(verify, false, "");
DEFINE_bool(ycsb_scan, false, "");
DEFINE_bool(ycsb_tx, true, "");
DEFINE_bool(ycsb_count_unique_lookup_keys, true, "");
// -------------------------------------------------------------------------------------
using namespace leanstore;
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
using YCSBKey = u64;
using YCSBPayload = BytesPayload<120>;
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
double calculateMTPS(chrono::high_resolution_clock::time_point begin, chrono::high_resolution_clock::time_point end, u64 factor)
{
   double tps = ((factor * 1.0 / (chrono::duration_cast<chrono::microseconds>(end - begin).count() / 1000000.0)));
   return (tps / 1000000.0);
}
// -------------------------------------------------------------------------------------
void run_ycsb() {
   // -------------------------------------------------------------------------------------
   chrono::high_resolution_clock::time_point begin, end;
   // -------------------------------------------------------------------------------------
   // LeanStore DB
   LeanStore db;
   unique_ptr<BTreeInterface<YCSBKey, YCSBPayload>> adapter;
   mean::task::scheduleTaskSync([&]() {
         auto& vs_btree = db.registerBTreeLL("ycsb", "y");
         adapter.reset(new BTreeVSAdapter<YCSBKey, YCSBPayload>(vs_btree));
         db.registerConfigEntry("ycsb_read_ratio", FLAGS_ycsb_read_ratio);
         db.registerConfigEntry("ycsb_target_gib", FLAGS_target_gib);
   });
   // -------------------------------------------------------------------------------------
   auto& table = *adapter;
   const u64 ycsb_tuple_count = (FLAGS_ycsb_tuple_count)
                                    ? FLAGS_ycsb_tuple_count
                                    : FLAGS_target_gib * 1024 * 1024 * 1024 * 1.0  / (sizeof(YCSBKey) + sizeof(YCSBPayload));
   

   // Insert values
   {
      const u64 n = ycsb_tuple_count;
      cout << "-------------------------------------------------------------------------------------" << endl;
      cout << "Inserting values" << endl;
      begin = chrono::high_resolution_clock::now();

      mean::BlockedRange bb(0, (u64)n);
      ensure((bool)((bb.end - bb.begin) > 1));
      auto ycsb_insert_fun = [&](mean::BlockedRange bb, std::atomic<bool>&) {
         // vector<u64> keys(range.size());
         // std::iota(keys.begin(), keys.end(), range.begin());
         // std::random_shuffle(keys.begin(), keys.end());
         for (u64 t_i = bb.begin; t_i < bb.end; t_i++) {
            YCSBPayload payload;
            utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&payload), sizeof(YCSBPayload));
            auto& key = t_i;
            table.insert(key, payload);
            // YCSBPayload result; /// FIXME remove this check
            // table.lookup(t_i, result);
            // ensure(result == payload);

            mean::task::yield();
         }
      };
      mean::task::parallelFor(bb, ycsb_insert_fun, FLAGS_worker_tasks, 100000);
      end = chrono::high_resolution_clock::now();
      cout << "time elapsed = " << (chrono::duration_cast<chrono::microseconds>(end - begin).count() / 1000000.0) << endl;
      cout << calculateMTPS(begin, end, n) << " M tps" << endl;
      // -------------------------------------------------------------------------------------
      const u64 written_pages = db.getBufferManager().consumedPages();
      const u64 mib = written_pages * PAGE_SIZE / 1024 / 1024;
      cout << "Inserted volume: (pages, MiB) = (" << written_pages << ", " << mib << ")" << endl;
      cout << "-------------------------------------------------------------------------------------" << endl;
   }
   db.startProfilingThread();
   // -------------------------------------------------------------------------------------
   auto zipf_random = std::make_unique<utils::ScrambledZipfGenerator>(0, ycsb_tuple_count, FLAGS_zipf_factor);
   cout << setprecision(4);
   // -------------------------------------------------------------------------------------
   // Scan
   if (FLAGS_ycsb_scan) {
      const u64 n = ycsb_tuple_count;
      cout << "-------------------------------------------------------------------------------------" << endl;
      cout << "Scan" << endl;
      {
         begin = chrono::high_resolution_clock::now();
         mean::BlockedRange bb(0, (u64)n);
         ensure((bool)((bb.end - bb.begin) > 1));
         auto ycsb_fun = [&](mean::BlockedRange bb, std::atomic<bool>&) {
            for (u64 i = bb.begin; i < bb.end; i++) {
               YCSBPayload result;
               table.lookup(i, result);
               mean::task::yield();
            }
         };
         mean::task::parallelFor(bb, ycsb_fun, FLAGS_worker_tasks, 100000);
         // mean::task::parallelFor(bb, ycsb_fun,1, 1000000000);
         end = chrono::high_resolution_clock::now();
      }
      // -------------------------------------------------------------------------------------
      cout << "time elapsed = " << (chrono::duration_cast<chrono::microseconds>(end - begin).count() / 1000000.0) << endl;
      // -------------------------------------------------------------------------------------
      cout << calculateMTPS(begin, end, n) << " M tps" << endl;
      cout << "-------------------------------------------------------------------------------------" << endl;
   }
   // -------------------------------------------------------------------------------------
   cout << "-------------------------------------------------------------------------------------" << endl;
   cout << "~Transactions" << endl;
   atomic<bool> keep_running = true;
   atomic<u64> running_threads_counter = 0;
   {
      auto start = mean::getSeconds();
      auto ycsb_tx = [&](mean::BlockedRange bb, std::atomic<bool>& cancelled){

       thread_local auto nextStartTime = mean::readTSC();
       thread_local u64 longLat = 0;
       auto tx_start_time = nextStartTime;
       const float rate = FLAGS_tx_rate / mean::env::workerCount();
       std::random_device rd;
       std::mt19937 gen(rd());
       std::exponential_distribution<> expDist(rate);
       volatile u64 i = bb.begin;
       //yscb D used
       uint64_t ycsb_last = 0;
       uint64_t ycsb_scan_length = 0;
       YCSBKey scan_key = 0;

         running_threads_counter++;
         int timeCheck = 0;
         while (i < bb.end && keep_running) {
            auto before = mean::readTSC();
            timeCheck++;
            if (timeCheck % 32 == 0 && mean::getSeconds() - start > FLAGS_run_for_seconds) {
               cancelled = true;
               break;
            }
            //zhengxd: YCSB A B C
            if(FLAGS_ycsb_type == 1){
               YCSBKey key = zipf_random->rand();
               assert(key < ycsb_tuple_count);
               YCSBPayload result;
               if (FLAGS_ycsb_read_ratio == 100 || utils::RandomGenerator::getRandU64(0, 100) < FLAGS_ycsb_read_ratio) {
                  table.lookup(key, result);
               } else {
                  YCSBPayload payload;
                  utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&payload), sizeof(YCSBPayload));
                  table.update(key, payload);
               }
            //zhengxd: YCSB D
            } else if (FLAGS_ycsb_type == 2) {
               YCSBKey key;
               YCSBPayload result;
               if (ycsb_last!= 0 && utils::RandomGenerator::getRandU64(0, 100) < FLAGS_ycsb_read_ratio) {
                  key = zipf_random->rand() % ycsb_last;
                  key = ycsb_last - key;
                  key =  utils::FNV::hash(key) % ycsb_tuple_count;
                  table.lookup(key, result);
               } else {
                  ycsb_last++;
                  key =  utils::FNV::hash(ycsb_last) % ycsb_tuple_count;
                  YCSBPayload payload;
                  utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&payload), sizeof(YCSBPayload));
                  table.update(key, payload);
               }
            //zhengxd: YCSB E
            } else if (FLAGS_ycsb_type == 3) {
               YCSBPayload result;
               YCSBKey key;
               if ( ycsb_scan_length > 0 || utils::RandomGenerator::getRandU64(0, 100) < FLAGS_ycsb_scan_ratio) {
                  if ( ycsb_scan_length == 0) {
                     ycsb_scan_length = utils::RandomGenerator::getRandU64(1, 100);
                     // ycsb_scan_length = 100;
                     scan_key = zipf_random->rand();
                     assert(scan_key < ycsb_tuple_count);
                  } 
                  table.lookup(scan_key, result);
                  mean::task::yield();
                  scan_key++;
                  ycsb_scan_length--;
                  if(scan_key >= ycsb_tuple_count) {
                     ycsb_scan_length = 0;
                  }
               } else {
                  key = zipf_random->rand();
                  assert(key < ycsb_tuple_count);
                  YCSBPayload payload;
                  utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&payload), sizeof(YCSBPayload));
                  table.update(key, payload);
                  mean::task::yield();
               }
            //zhengxd: YCSB F
            } else if (FLAGS_ycsb_type == 4) {
               YCSBKey key = zipf_random->rand();
               assert(key < ycsb_tuple_count);
               YCSBPayload result;
               if (utils::RandomGenerator::getRandU64(0, 100) < FLAGS_ycsb_read_ratio) {
                  table.lookup(key, result);
               } else {
                  table.lookup(key, result);
                  YCSBPayload payload;
                  utils::RandomGenerator::getRandString(reinterpret_cast<u8*>(&payload), sizeof(YCSBPayload));
                  table.update(key, payload);
                  mean::task::yield();
               }
            } else {
               std::cout << "unknown ycsb type" << std::endl;
               exit(1);
            }
           i++;
           auto now = mean::readTSC();
           auto timeDiff = mean::tscDifferenceUs(now, before);
           auto timeDiffIncWait = mean::tscDifferenceUs(now, tx_start_time);
           WorkerCounters::myCounters().total_tx_time += timeDiff;
           WorkerCounters::myCounters().tx_latency_hist.increaseSlot(timeDiff);
           if (timeDiffIncWait < 10000000) {
              WorkerCounters::myCounters().total_tx_time_inc_wait += timeDiffIncWait;
           }
           WorkerCounters::myCounters().tx_latency_hist_incwait.increaseSlot(timeDiffIncWait);
           WorkerCounters::myCounters().tx++;
           ThreadCounters::myCounters().tx++;
           while (i < bb.end && keep_running) {
              mean::task::yield();
              now = mean::readTSC();
              if (rate == 0) break;
              if (now >= nextStartTime) {
                 if (mean::tscDifferenceS(now, nextStartTime) > 5) {
                     longLat++;
                     nextStartTime = now;
                     std::cout << "reset start time" << std::endl;
                     if (longLat % 100000 == 0) {
                        //std::cout << "thr: " << mean::exec::getId() << " long latency: " << longLat << std::endl;
                    }
                 }
                 auto d = expDist(gen);
                 tx_start_time = nextStartTime;
                  nextStartTime += mean::nsToTSC(d*1e9);
                  //std::cout << "next: " << nextStartTime << std::flush << std::endl;
                  break;
              }
           }
         }
         running_threads_counter--;
      };
      mean::BlockedRange bb(0, (u64)1000000000000ul);
      auto startTsc = mean::readTSC();
      auto startTP = mean::getTimePoint();
      mean::task::parallelFor(bb, ycsb_tx, FLAGS_worker_tasks, 100000);
      auto diffTSC = mean::tscDifferenceNs(mean::readTSC(), startTsc) / 1e9;
      auto diffTP = mean::timePointDifference(mean::getTimePoint(), startTP) / 1e9;
      std::cout << "done: time: " << diffTP << " tsc: " << diffTSC << std::endl;
   }
   mean::env::shutdown();
   cout << "-------------------------------------------------------------------------------------" << endl;
   // -------------------------------------------------------------------------------------
}
// -------------------------------------------------------------------------------------
int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Leanstore Frontend");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   // -------------------------------------------------------------------------------------
   using namespace mean;
   IoOptions ioOptions("auto", FLAGS_ssd_path);
   ioOptions.write_back_buffer_size = PAGE_SIZE;
   ioOptions.engine = FLAGS_ioengine;
   ioOptions.ioUringPollMode = FLAGS_io_uring_poll_mode;
   ioOptions.ioUringShareWq = FLAGS_io_uring_share_wq;
   ioOptions.raid5 = FLAGS_raid5;
   ioOptions.iodepth = (FLAGS_async_batch_size + FLAGS_worker_tasks)*2; // hacky, how to take into account for remotes 
   ioOptions.hitchhike = FLAGS_hitchhike;
   // -------------------------------------------------------------------------------------
   if (FLAGS_nopp) {
      std::cout << "---------- not use pp thread-------" << std::endl; 
      ioOptions.channelCount = FLAGS_worker_threads;
      mean::env::init(
         FLAGS_worker_threads, //std::min(std::thread::hardware_concurrency(), FLAGS_tpcc_warehouse_count),
         0/*FLAGS_pp_threads*/, ioOptions);
   } else {
      std::cout << "----------use pp thread-------" << std::endl; 
      ioOptions.channelCount = FLAGS_worker_threads + FLAGS_pp_threads;
      mean::env::init(FLAGS_worker_threads, FLAGS_pp_threads, ioOptions);
   }
   if(FLAGS_hitchhike) {
      std::cout << "----------use hitchhike-------" << std::endl; 
   }
   mean::env::start(run_ycsb);
   // -------------------------------------------------------------------------------------
   mean::env::join();
   // -------------------------------------------------------------------------------------
   return 0;
}
