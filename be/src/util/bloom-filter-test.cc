// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "util/bloom-filter.h"

#include <algorithm>
#include <unordered_set>
#include <vector>

#include "kudu/rpc/rpc_controller.h"
#include "kudu/util/random.h"
#include "runtime/bufferpool/buffer-pool.h"
#include "runtime/bufferpool/reservation-tracker.h"
#include "runtime/mem-tracker.h"
#include "runtime/test-env.h"
#include "service/fe-support.h"
#include "testutil/gtest-util.h"
#include "util/bit-util.h"

#include "gen-cpp/data_stream_service.pb.h"

// This flag is used in Kudu to temporarily disable AVX2 support for testing purpose.
DECLARE_bool(disable_blockbloomfilter_avx2);
DECLARE_int64(bloom_filter_prefetch_min_bytes);

using namespace std;

using namespace impala;
using kudu::rpc::RpcController;

namespace bloom_filter_test_util {

// Make a random uint32_t, avoiding the absent high bit and the low-entropy low bits
// produced by rand().
uint32_t MakeRand() {
  uint32_t result = (rand() >> 8) & 0xffff;
  result <<= 16;
  result |= (rand() >> 8) & 0xffff;
  return result;
}

// BfInsert() and BfFind() call BloomFilter::{Insert,Find} respectively.

void BfInsert(BloomFilter& bf, uint32_t h) {
  bf.Insert(h);
}

bool BfFind(BloomFilter& bf, uint32_t h) {
  return bf.Find(h);
}

// Computes union of 'x' and 'y'. Computes twice with AVX enabled and disabled and
// verifies both produce the same result. 'success' is set to true if both union
// computations returned the same result and set to false otherwise.
void BfUnion(BloomFilter& x, BloomFilter& y, int64_t directory_size, bool* success,
    BloomFilterPB* protobuf, std::string* directory) {
  BloomFilterPB protobuf_x, protobuf_y;
  RpcController controller_x;
  RpcController controller_y;
  BloomFilter::ToProtobuf(&x, &controller_x, &protobuf_x);
  BloomFilter::ToProtobuf(&y, &controller_y, &protobuf_y);

  FLAGS_disable_blockbloomfilter_avx2 = false;
  string directory_x(
      reinterpret_cast<const char*>(x.GetBlockBloomFilter()->directory().data()),
      directory_size);
  string directory_y(
      reinterpret_cast<const char*>(y.GetBlockBloomFilter()->directory().data()),
      directory_size);

  BloomFilter::Or(protobuf_x, reinterpret_cast<const uint8_t*>(directory_x.data()),
      &protobuf_y, reinterpret_cast<uint8_t*>(const_cast<char*>(directory_y.data())),
      directory_size);

  {
    FLAGS_disable_blockbloomfilter_avx2 = true;
    BloomFilterPB protobuf_x2, protobuf_y2;
    RpcController controller_x2;
    RpcController controller_y2;
    BloomFilter::ToProtobuf(&x, &controller_x2, &protobuf_x2);
    BloomFilter::ToProtobuf(&y, &controller_y2, &protobuf_y2);

    string directory_x2(
        reinterpret_cast<const char*>(x.GetBlockBloomFilter()->directory().data()),
        directory_size);
    string directory_y2(
        reinterpret_cast<const char*>(y.GetBlockBloomFilter()->directory().data()),
        directory_size);

    BloomFilter::Or(protobuf_x2, reinterpret_cast<const uint8_t*>(directory_x2.data()),
        &protobuf_y2, reinterpret_cast<uint8_t*>(const_cast<char*>(directory_y2.data())),
        directory_size);

    *success = directory_y.compare(directory_y2) == 0;
  }

  *protobuf = protobuf_y;
  *directory = directory_y;
}

} // namespace bloom_filter_test_util

using namespace bloom_filter_test_util;

namespace impala {

// Test that MaxNdv() and MinLogSpace() are dual
TEST(BloomFilter, MinSpaceMaxNdv) {
  for (int log2fpp = -2; log2fpp >= -30; --log2fpp) {
    const double fpp = pow(2, log2fpp);
    for (int given_log_space = 8; given_log_space < 30; ++given_log_space) {
      const size_t derived_ndv = BloomFilter::MaxNdv(given_log_space, fpp);
      // If NO values can be added without exceeding fpp, then the space needed is
      // trivially zero. This becomes a useless test; skip to the next iteration.
      if (0 == derived_ndv) continue;
      int derived_log_space = BloomFilter::MinLogSpace(derived_ndv, fpp);

      EXPECT_EQ(derived_log_space, given_log_space) << "fpp: " << fpp
                                                    << " derived_ndv: " << derived_ndv;

      // If we lower the fpp, we need more space; if we raise it we need less.
      derived_log_space = BloomFilter::MinLogSpace(derived_ndv, fpp / 2);
      EXPECT_GE(derived_log_space, given_log_space) << "fpp: " << fpp
                                                    << " derived_ndv: " << derived_ndv;
      derived_log_space = BloomFilter::MinLogSpace(derived_ndv, fpp * 2);
      EXPECT_LE(derived_log_space, given_log_space) << "fpp: " << fpp
                                                    << " derived_ndv: " << derived_ndv;
    }
    for (size_t given_ndv = 1000; given_ndv < 1000 * 1000; given_ndv *= 3) {
      const int derived_log_space = BloomFilter::MinLogSpace(given_ndv, fpp);
      const size_t derived_ndv = BloomFilter::MaxNdv(derived_log_space, fpp);

      // The max ndv is close to, but larger than, then ndv we asked for
      EXPECT_LE(given_ndv, derived_ndv) << "fpp: " << fpp
                                        << " derived_log_space: " << derived_log_space;
      EXPECT_GE(given_ndv * 2, derived_ndv)
          << "fpp: " << fpp << " derived_log_space: " << derived_log_space;

      // Changing the fpp changes the ndv capacity in the expected direction.
      size_t new_derived_ndv = BloomFilter::MaxNdv(derived_log_space, fpp / 2);
      EXPECT_GE(derived_ndv, new_derived_ndv)
          << "fpp: " << fpp << " derived_log_space: " << derived_log_space;
      new_derived_ndv = BloomFilter::MaxNdv(derived_log_space, fpp * 2);
      EXPECT_LE(derived_ndv, new_derived_ndv)
          << "fpp: " << fpp << " derived_log_space: " << derived_log_space;
    }
  }
}

TEST(BloomFilter, MinSpaceEdgeCase) {
  int min_space = BloomFilter::MinLogSpace(1, 0.75);
  EXPECT_GE(min_space, 0) << "LogSpace should always be >= 0";
}

// Check that MinLogSpace() and FalsePositiveProb() are dual
TEST(BloomFilter, MinSpaceForFpp) {
  for (size_t ndv = 10000; ndv < 100 * 1000 * 1000; ndv *= 1.1) {
    for (double fpp = 0.1; fpp > pow(2, -20); fpp *= 0.9) { // NOLINT: loop on double
      // When contructing a Bloom filter, we can request a particular fpp by calling
      // MinLogSpace().
      const int min_log_space = BloomFilter::MinLogSpace(ndv, fpp);
      // However, at the resulting ndv and space, the expected fpp might be lower than
      // the one that was requested.
      double expected_fpp = BloomFilter::FalsePositiveProb(ndv, min_log_space);
      EXPECT_LE(expected_fpp, fpp);
      // The fpp we get might be much lower than the one we asked for. However, if the
      // space were just one size smaller, the fpp we get would be larger than the one we
      // asked for.
      expected_fpp = BloomFilter::FalsePositiveProb(ndv, min_log_space - 1);
      EXPECT_GE(expected_fpp, fpp);
      // Therefore, the return value of MinLogSpace() is actually the minimum
      // log space at which we can guarantee the requested fpp.
    }
  }
}

class BloomFilterTest : public testing::Test {
 protected:
  /// Temporary runtime environment for the BloomFilters.
  unique_ptr<TestEnv> test_env_;
  RuntimeState* runtime_state_;

  ObjectPool pool_;
  unique_ptr<MemTracker> tracker_;
  unique_ptr<BufferPool::ClientHandle> buffer_pool_client_;
  vector<BloomFilter*> bloom_filters_;

  virtual void SetUp() {
    int64_t min_page_size = 64; // Min filter size that we allocate in our tests.
    int64_t buffer_bytes_limit = 4L * 1024 * 1024 * 1024;
    test_env_.reset(new TestEnv());
    test_env_->SetBufferPoolArgs(min_page_size, buffer_bytes_limit);
    ASSERT_OK(test_env_->Init());
    ASSERT_OK(test_env_->CreateQueryState(0, nullptr, &runtime_state_));
    buffer_pool_client_.reset(new BufferPool::ClientHandle);
    tracker_.reset(new MemTracker(-1, "client", runtime_state_->instance_mem_tracker()));
    BufferPool* buffer_pool = test_env_->exec_env()->buffer_pool();
    ASSERT_OK(buffer_pool->RegisterClient("", nullptr,
        runtime_state_->instance_buffer_reservation(), tracker_.get(),
        std::numeric_limits<int64>::max(), runtime_state_->runtime_profile(),
        buffer_pool_client_.get()));
  }

  virtual void TearDown() {
    for (BloomFilter* filter : bloom_filters_) filter->Close();
    bloom_filters_.clear();
    runtime_state_ = nullptr;
    pool_.Clear();
    test_env_->exec_env()->buffer_pool()->DeregisterClient(buffer_pool_client_.get());
    buffer_pool_client_.reset();
    tracker_.reset();
    test_env_.reset();
  }

  BloomFilter* CreateBloomFilter(int log_bufferpool_space) {
    int64_t filter_size = BloomFilter::GetExpectedMemoryUsed(log_bufferpool_space);
    EXPECT_TRUE(buffer_pool_client_->IncreaseReservation(filter_size));
    // Randomly disable AVX2 instructions half of the time. These are used for testing
    // that AVX2 machines and non-AVX2 machines produce compatible BloomFilters.
    FLAGS_disable_blockbloomfilter_avx2 = (MakeRand() & 0x1) == 0;
    BloomFilter* bloom_filter = pool_.Add(new BloomFilter(buffer_pool_client_.get()));
    EXPECT_OK(bloom_filter->Init(log_bufferpool_space, 0));
    bloom_filters_.push_back(bloom_filter);
    EXPECT_NE(bloom_filter->GetBufferPoolSpaceUsed(), -1);
    return bloom_filter;
  }

  BloomFilter* CreateBloomFilter(
      const BloomFilterPB& filter_pb, const std::string& directory) {
    int64_t filter_size =
        BloomFilter::GetExpectedMemoryUsed(filter_pb.log_bufferpool_space());
    EXPECT_TRUE(buffer_pool_client_->IncreaseReservation(filter_size));
    // Randomly disable AVX2 instructions half of the time. These are used for testing
    // that AVX2 machines and non-AVX2 machines produce compatible BloomFilters.
    FLAGS_disable_blockbloomfilter_avx2 = (MakeRand() & 0x1) == 0;
    BloomFilter* bloom_filter = pool_.Add(new BloomFilter(buffer_pool_client_.get()));

    EXPECT_OK(bloom_filter->Init(filter_pb,
        reinterpret_cast<const uint8_t*>(directory.data()), directory.size(), 0));

    bloom_filters_.push_back(bloom_filter);
    EXPECT_NE(bloom_filter->GetBufferPoolSpaceUsed(), -1);
    return bloom_filter;
  }
};

// We can construct (and destruct) Bloom filters with different spaces.
TEST_F(BloomFilterTest, Constructor) {
  // The minimum log_bufferpool_space size is 5 for bloom filter, which is defined
  // as BlockBloomFilter.kLogBucketWordBits in kudu/util/block_bloom_filter.h,
  // and is checked in function BlockBloomFilter.GetExpectedMemoryUsed().
  for (int i = 5; i < 30; ++i) {
    CreateBloomFilter(i);
  }
}

// We can Insert() hashes into a Bloom filter with different spaces.
TEST_F(BloomFilterTest, Insert) {
  srand(0);
  for (int i = 13; i < 17; ++i) {
    BloomFilter* bf = CreateBloomFilter(i);
    for (int k = 0; k < (1 << 15); ++k) {
      BfInsert(*bf, MakeRand());
    }
  }
}

// After Insert()ing something into a Bloom filter, it can be found again immediately.
TEST_F(BloomFilterTest, Find) {
  srand(0);
  for (int i = 13; i < 17; ++i) {
    BloomFilter* bf = CreateBloomFilter(i);
    for (int k = 0; k < (1 << 15); ++k) {
      const uint32_t to_insert = MakeRand();
      BfInsert(*bf, to_insert);
      EXPECT_TRUE(BfFind(*bf, to_insert));
    }
  }
}

// After Insert()ing something into a Bloom filter, it can be found again much later.
TEST_F(BloomFilterTest, CumulativeFind) {
  srand(0);
  for (int i = 5; i < 11; ++i) {
    std::vector<uint32_t> inserted;
    BloomFilter* bf = CreateBloomFilter(i);
    for (int k = 0; k < (1 << 10); ++k) {
      const uint32_t to_insert = MakeRand();
      inserted.push_back(to_insert);
      BfInsert(*bf, to_insert);
      for (int n = 0; n < inserted.size(); ++n) {
        EXPECT_TRUE(BfFind(*bf, inserted[n]));
      }
    }
  }
}

// The empirical false positives we find when looking for random items is with a constant
// factor of the false positive probability the Bloom filter was constructed for.
TEST_F(BloomFilterTest, FindInvalid) {
  // We use a deterministic pseudorandom number generator with a set seed. The reason is
  // that with a run-dependent seed, there will always be inputs that can fail. That's a
  // potential argument for this to be a benchmark rather than a test, although the
  // measured quantity would be not time but deviation from predicted fpp.
  ::kudu::Random rgen(867 + 5309);
  static const int find_limit = 1 << 22;
  unordered_set<uint32_t> to_find;
  while (to_find.size() < find_limit) {
    to_find.insert(rgen.Next64());
  }
  static const int max_log_ndv = 19;
  unordered_set<uint32_t> to_insert;
  while (to_insert.size() < (1ull << max_log_ndv)) {
    const auto candidate = rgen.Next64();
    if (to_find.find(candidate) == to_find.end()) {
      to_insert.insert(candidate);
    }
  }
  vector<uint32_t> shuffled_insert(to_insert.begin(), to_insert.end());
  for (int log_ndv = 12; log_ndv < max_log_ndv; ++log_ndv) {
    for (int log_fpp = 4; log_fpp < 12; ++log_fpp) {
      double fpp = 1.0 / (1 << log_fpp);
      const size_t ndv = 1 << log_ndv;
      const int log_heap_space = BloomFilter::MinLogSpace(ndv, fpp);
      BloomFilter* bf = CreateBloomFilter(log_heap_space);
      // Fill up a BF with exactly as much ndv as we planned for it:
      for (size_t i = 0; i < ndv; ++i) {
        bf->Insert( shuffled_insert[i]);
      }
      int found = 0;
      // Now we sample from the set of possible hashes, looking for hits.
      for (const auto& i : to_find) {
        found += bf->Find(i);
      }
      EXPECT_LE(found, find_limit * fpp * 2)
          << "Too many false positives with -log2(fpp) = " << log_fpp
          << " and log_ndv = " << log_ndv << " and log_heap_space = " << log_heap_space;
      // Because the space is rounded up to a power of 2, we might actually get a lower
      // fpp than the one passed to MinLogSpace().
      const double expected_fpp = BloomFilter::FalsePositiveProb(ndv, log_heap_space);
      // Fudge factors are present because filter characteristics are true in the limit,
      // and will deviate for small samples.
      EXPECT_GE(found, find_limit * expected_fpp * 0.75)
          << "Too few false positives with -log2(fpp) = " << log_fpp
          << " expected_fpp = " << expected_fpp;
      EXPECT_LE(found, find_limit * expected_fpp * 1.25)
          << "Too many false positives with -log2(fpp) = " << log_fpp
          << " expected_fpp = " << expected_fpp;
    }
  }
}

// BucketAddress() repeats Kudu's private bucket computation. Check that it is the
// bucket that Kudu's Insert() writes. CreateBloomFilter() randomly disables Kudu's AVX2
// path, so both Kudu paths are covered.
TEST_F(BloomFilterTest, BucketAddressMatchesKudu) {
  constexpr int BUCKET_BYTES = 32;
  // 5 is the smallest filter (2 buckets), 20 = 1 MB is the default
  // RUNTIME_FILTER_MIN_SIZE, 23 needs more than 16 bits of bucket index.
  for (int log_space : {5, 6, 10, 16, 20, 23}) {
    BloomFilter* bf = CreateBloomFilter(log_space);
    kudu::Slice directory = bf->GetBlockBloomFilter()->directory();
    uint8_t* dir_begin = const_cast<uint8_t*>(directory.data());
    const uint8_t* dir_end = dir_begin + directory.size();
    for (int i = 0; i < 1000; ++i) {
      uint32_t h = MakeRand();
      uint8_t* bucket = const_cast<uint8_t*>(bf->BucketAddress(h));
      ASSERT_GE(bucket, dir_begin);
      ASSERT_LE(bucket + BUCKET_BYTES, dir_end);
      ASSERT_EQ(0, (bucket - dir_begin) % BUCKET_BYTES);
      // The directory is all zeros here, so Insert() sets bits only in the bucket of
      // 'h'. Clear them again to keep the directory zero.
      bf->Insert(h);
      ASSERT_TRUE(bf->Find(h));
      bool bucket_has_bits = false;
      for (int j = 0; j < BUCKET_BYTES; ++j) bucket_has_bits |= bucket[j] != 0;
      ASSERT_TRUE(bucket_has_bits) << "log_space=" << log_space << " h=" << h;
      memset(bucket, 0, BUCKET_BYTES);
    }
    // If any Insert() had set bits in a different bucket, they would still be set.
    EXPECT_EQ(dir_end, std::find_if(dir_begin, const_cast<uint8_t*>(dir_end),
        [](uint8_t b) { return b != 0; })) << "log_space=" << log_space;
    // Prefetch() only computes the same address, it must not change the filter.
    for (int i = 0; i < 100; ++i) {
      bf->Prefetch(MakeRand());
      bf->Prefetch(MakeRand(), /*for_write=*/true);
    }
  }
}

// InsertDeferred() and IrInsert(), followed by FlushDeferredInserts(), must leave the
// filter in the same state as Insert().
TEST_F(BloomFilterTest, DeferredInsertsMatchInsert) {
  gflags::FlagSaver saver;
  // Make every filter defer its inserts.
  FLAGS_bloom_filter_prefetch_min_bytes = 0;
  for (int log_space : {5, 10, 16, 20}) {
    // Less than one batch, exactly one batch and several batches with a remainder.
    for (int num_inserts : {1, 127, 128, 1000}) {
      BloomFilter* direct_bf = CreateBloomFilter(log_space);
      BloomFilter* deferred_bf = CreateBloomFilter(log_space);
      BloomFilter* ir_bf = CreateBloomFilter(log_space);
      vector<uint32_t> hashes;
      for (int i = 0; i < num_inserts; ++i) {
        uint32_t h = MakeRand();
        hashes.push_back(h);
        direct_bf->Insert(h);
        deferred_bf->InsertDeferred(h);
        ir_bf->IrInsert(h);
      }
      // The batch of 128 is inserted when it is full, the rest is buffered.
      EXPECT_EQ(num_inserts % 128 != 0, deferred_bf->HasDeferredInserts());
      EXPECT_EQ(num_inserts % 128 != 0, ir_bf->HasDeferredInserts());
      deferred_bf->FlushDeferredInserts();
      ir_bf->FlushDeferredInserts();
      EXPECT_FALSE(deferred_bf->HasDeferredInserts());
      EXPECT_FALSE(ir_bf->AlwaysFalse());
      // Flushing again does nothing.
      ir_bf->FlushDeferredInserts();
      for (uint32_t h : hashes) {
        ASSERT_TRUE(deferred_bf->Find(h));
        ASSERT_TRUE(ir_bf->Find(h));
      }
      kudu::Slice direct_dir = direct_bf->GetBlockBloomFilter()->directory();
      for (BloomFilter* bf : {deferred_bf, ir_bf}) {
        kudu::Slice dir = bf->GetBlockBloomFilter()->directory();
        ASSERT_EQ(direct_dir.size(), dir.size());
        EXPECT_EQ(0, memcmp(direct_dir.data(), dir.data(), dir.size()))
            << "log_space=" << log_space << " num_inserts=" << num_inserts;
      }
    }
  }
}

// Only filters of at least --bloom_filter_prefetch_min_bytes defer their inserts.
TEST_F(BloomFilterTest, DeferredInsertsThreshold) {
  gflags::FlagSaver saver;
  FLAGS_bloom_filter_prefetch_min_bytes = 1 << 16;
  EXPECT_FALSE(BloomFilter::ShouldPrefetch((1 << 16) - 1));
  EXPECT_TRUE(BloomFilter::ShouldPrefetch(1 << 16));
  BloomFilter* small_bf = CreateBloomFilter(15);
  BloomFilter* large_bf = CreateBloomFilter(16);
  small_bf->InsertDeferred(42);
  large_bf->InsertDeferred(42);
  EXPECT_FALSE(small_bf->HasDeferredInserts());
  EXPECT_TRUE(small_bf->Find(42));
  EXPECT_TRUE(large_bf->HasDeferredInserts());
  large_bf->FlushDeferredInserts();
  EXPECT_TRUE(large_bf->Find(42));

  // A negative value disables prefetching.
  FLAGS_bloom_filter_prefetch_min_bytes = -1;
  EXPECT_FALSE(BloomFilter::ShouldPrefetch(int64_t{1} << 40));
  BloomFilter* bf = CreateBloomFilter(20);
  bf->InsertDeferred(42);
  EXPECT_FALSE(bf->HasDeferredInserts());
  EXPECT_TRUE(bf->Find(42));
}

// Close() and Init() drop deferred inserts, they would not fit into a new directory.
TEST_F(BloomFilterTest, DeferredInsertsReinit) {
  gflags::FlagSaver saver;
  FLAGS_bloom_filter_prefetch_min_bytes = 0;
  BloomFilter* bf = CreateBloomFilter(20);
  for (int i = 0; i < 10; ++i) bf->InsertDeferred(MakeRand());
  EXPECT_TRUE(bf->HasDeferredInserts());
  bf->Close();
  EXPECT_FALSE(bf->HasDeferredInserts());
  EXPECT_OK(bf->Init(10, 0));
  EXPECT_FALSE(bf->HasDeferredInserts());
  EXPECT_TRUE(bf->AlwaysFalse());
  bf->InsertDeferred(42);
  EXPECT_TRUE(bf->HasDeferredInserts());
  EXPECT_OK(bf->Init(10, 0));
  EXPECT_FALSE(bf->HasDeferredInserts());
  EXPECT_TRUE(bf->AlwaysFalse());
}

// Folding a directory with ShrinkDirectory() must give exactly the directory of a
// smaller filter with the same inserts. This relies on how Kudu computes bucket indexes,
// a mismatch would make shrunk filters drop rows.
TEST_F(BloomFilterTest, ShrinkDirectoryMatchesSmallerFilter) {
  for (int log_space : {13, 16, 20}) {
    for (int folds : {1, 3, 7}) {
      BloomFilter* large = CreateBloomFilter(log_space);
      BloomFilter* small = CreateBloomFilter(log_space - folds);
      for (int i = 0; i < 300; ++i) {
        uint32_t h = MakeRand();
        large->Insert(h);
        small->Insert(h);
      }
      kudu::Slice large_dir = large->GetBlockBloomFilter()->directory();
      string folded(reinterpret_cast<const char*>(large_dir.data()), large_dir.size());
      // An fpp limit of 1 folds down to 'min_log_space'.
      ASSERT_EQ(log_space - folds,
          BloomFilter::ShrinkDirectory(reinterpret_cast<uint8_t*>(&folded[0]), log_space,
              1.0, log_space - folds));
      kudu::Slice small_dir = small->GetBlockBloomFilter()->directory();
      ASSERT_EQ(small_dir.size(), size_t{1} << (log_space - folds));
      EXPECT_EQ(0, memcmp(small_dir.data(), folded.data(), small_dir.size()))
          << "log_space=" << log_space << " folds=" << folds;
    }
  }
}

// EstimateFpp() agrees with Kudu's FalsePositiveProb() and with measured lookups.
TEST_F(BloomFilterTest, EstimateFpp) {
  // About 10 bits per value, ~1% fpp.
  for (const auto& [log_space, ndv] : {std::pair{16, 50000}, std::pair{20, 800000}}) {
    BloomFilter* bf = CreateBloomFilter(log_space);
    for (int i = 0; i < ndv; ++i) bf->Insert(MakeRand());
    kudu::Slice dir = bf->GetBlockBloomFilter()->directory();
    const double estimate = BloomFilter::EstimateFpp(dir.data(), dir.size());
    const double expected = BloomFilter::FalsePositiveProb(ndv, log_space);
    EXPECT_GT(estimate, expected * 0.8) << "log_space=" << log_space;
    EXPECT_LT(estimate, expected * 1.25) << "log_space=" << log_space;
    constexpr int NUM_PROBES = 200000;
    int found = 0;
    for (int i = 0; i < NUM_PROBES; ++i) found += bf->Find(MakeRand());
    EXPECT_GT(found, NUM_PROBES * estimate * 0.8) << "log_space=" << log_space;
    EXPECT_LT(found, NUM_PROBES * estimate * 1.25) << "log_space=" << log_space;
  }
  BloomFilter* empty = CreateBloomFilter(16);
  kudu::Slice dir = empty->GetBlockBloomFilter()->directory();
  EXPECT_EQ(0, BloomFilter::EstimateFpp(dir.data(), dir.size()));
}

// ShrinkDirectory() folds as long as the estimated fpp stays at most the limit.
TEST_F(BloomFilterTest, ShrinkDirectoryFppLimit) {
  constexpr double MAX_FPP = 0.001;
  constexpr int MIN_LOG_SPACE = 13;
  for (int ndv : {0, 1, 1000, 30000}) {
    BloomFilter* bf = CreateBloomFilter(20);
    for (int i = 0; i < ndv; ++i) bf->Insert(MakeRand());
    kudu::Slice dir = bf->GetBlockBloomFilter()->directory();
    string shrunk(reinterpret_cast<const char*>(dir.data()), dir.size());
    uint8_t* shrunk_data = reinterpret_cast<uint8_t*>(&shrunk[0]);
    const int log_space = BloomFilter::ShrinkDirectory(shrunk_data, 20, MAX_FPP,
        MIN_LOG_SPACE);
    EXPECT_GE(log_space, MIN_LOG_SPACE);
    EXPECT_LE(BloomFilter::EstimateFpp(shrunk_data, int64_t{1} << log_space), MAX_FPP);
    if (log_space > MIN_LOG_SPACE) {
      // One more fold would have exceeded the limit.
      ASSERT_EQ(log_space - 1,
          BloomFilter::ShrinkDirectory(shrunk_data, log_space, 1.0, log_space - 1));
      EXPECT_GT(BloomFilter::EstimateFpp(shrunk_data, int64_t{1} << (log_space - 1)),
          MAX_FPP) << "ndv=" << ndv;
    }
    // The planner's formula picks about the same size.
    if (ndv >= 1000) {
      EXPECT_NEAR(BloomFilter::MinLogSpace(ndv, MAX_FPP), log_space, 1) << "ndv=" << ndv;
    }
  }
  // A filter that is already above the limit (~10 bits per value, ~1% fpp) is left
  // unchanged.
  BloomFilter* full = CreateBloomFilter(16);
  for (int i = 0; i < 50000; ++i) full->Insert(MakeRand());
  kudu::Slice dir = full->GetBlockBloomFilter()->directory();
  string copy(reinterpret_cast<const char*>(dir.data()), dir.size());
  EXPECT_EQ(16, BloomFilter::ShrinkDirectory(
      reinterpret_cast<uint8_t*>(&copy[0]), 16, MAX_FPP, MIN_LOG_SPACE));
  EXPECT_EQ(0, memcmp(dir.data(), copy.data(), dir.size()));
}

// BloomFilter::Shrink() reallocates a smaller directory that finds the same values.
TEST_F(BloomFilterTest, Shrink) {
  MemTracker tracker;
  BloomFilter* bf = CreateBloomFilter(20);
  vector<uint32_t> hashes;
  for (int i = 0; i < 1000; ++i) {
    hashes.push_back(MakeRand());
    bf->Insert(hashes.back());
  }
  bool shrunk = false;
  ASSERT_OK(bf->Shrink(0.001, 13, &tracker, &shrunk));
  EXPECT_TRUE(shrunk);
  EXPECT_LT(bf->directory_size(), 1 << 20);
  EXPECT_GE(bf->directory_size(), 1 << 13);
  EXPECT_EQ(bf->directory_size(), bf->GetBlockBloomFilter()->directory().size());
  EXPECT_EQ(0, tracker.consumption());
  EXPECT_FALSE(bf->AlwaysFalse());
  for (uint32_t h : hashes) ASSERT_TRUE(bf->Find(h));
  // The shrunk filter can still be serialized and inserted into.
  BloomFilterPB pb;
  RpcController controller;
  BloomFilter::ToProtobuf(bf, &controller, &pb);
  EXPECT_EQ(BitUtil::Log2Ceiling64(bf->directory_size()), pb.log_bufferpool_space());
  bf->Insert(42);
  EXPECT_TRUE(bf->Find(42));

  // Empty filters and filters at the minimum size are not shrunk.
  BloomFilter* empty = CreateBloomFilter(20);
  ASSERT_OK(empty->Shrink(0.001, 13, &tracker, &shrunk));
  EXPECT_FALSE(shrunk);
  EXPECT_EQ(1 << 20, empty->directory_size());
  BloomFilter* tiny = CreateBloomFilter(13);
  tiny->Insert(42);
  ASSERT_OK(tiny->Shrink(0.001, 13, &tracker, &shrunk));
  EXPECT_FALSE(shrunk);

  // If the temporary copy does not fit into the memory limit, nothing changes.
  MemTracker limited(1000);
  BloomFilter* unchanged = CreateBloomFilter(20);
  unchanged->Insert(42);
  string before(reinterpret_cast<const char*>(
      unchanged->GetBlockBloomFilter()->directory().data()), 1 << 20);
  ASSERT_OK(unchanged->Shrink(0.001, 13, &limited, &shrunk));
  EXPECT_FALSE(shrunk);
  EXPECT_EQ(0, memcmp(before.data(),
      unchanged->GetBlockBloomFilter()->directory().data(), 1 << 20));
  EXPECT_EQ(0, limited.consumption());
  tracker.Close();
  limited.Close();
}

TEST_F(BloomFilterTest, Protobuf) {
  BloomFilter* bf = CreateBloomFilter(BloomFilter::MinLogSpace(100, 0.01));
  for (int i = 0; i < 10; ++i) BfInsert(*bf, i);
  // Check no unexpected new false positives.
  unordered_set<int> missing_ints;
  for (int i = 11; i < 100; ++i) {
    if (!BfFind(*bf, i)) missing_ints.insert(i);
  }

  BloomFilterPB to_protobuf;

  RpcController controller;
  BloomFilter::ToProtobuf(bf, &controller, &to_protobuf);

  EXPECT_EQ(to_protobuf.always_true(), false);

  std::string directory(
      reinterpret_cast<const char*>(bf->GetBlockBloomFilter()->directory().data()),
      BloomFilter::GetExpectedMemoryUsed(BloomFilter::MinLogSpace(100, 0.01)));

  BloomFilter* from_protobuf = CreateBloomFilter(to_protobuf, directory);

  for (int i = 0; i < 10; ++i) ASSERT_TRUE(BfFind(*from_protobuf, i));
  for (int missing : missing_ints) ASSERT_FALSE(BfFind(*from_protobuf, missing));

  RpcController controller_2;
  BloomFilter::ToProtobuf(nullptr, &controller_2, &to_protobuf);
  EXPECT_EQ(to_protobuf.always_true(), true);
}

// Basic test for the Or() operation on BloomFilter objects.
// ThriftOr() tests the low-level implementation more exhaustively.
TEST_F(BloomFilterTest, Or) {
  BloomFilter* bf1 = CreateBloomFilter(BloomFilter::MinLogSpace(100, 0.01));
  BloomFilter* bf2 = CreateBloomFilter(BloomFilter::MinLogSpace(100, 0.01));

  for (int i = 60; i < 80; ++i) BfInsert(*bf2, i);
  for (int i = 0; i < 10; ++i) BfInsert(*bf1, i);

  bf1->Or(*bf2);
  for (int i = 0; i < 10; ++i) ASSERT_TRUE(BfFind(*bf1, i)) << i;
  for (int i = 60; i < 80; ++i) ASSERT_TRUE(BfFind(*bf1, i)) << i;

  // Insert another value to aggregated BloomFilter.
  for (int i = 11; i < 50; ++i) BfInsert(*bf1, i);

  for (int i = 11; i < 50; ++i) ASSERT_TRUE(BfFind(*bf1, i));
  ASSERT_FALSE(BfFind(*bf1, 81));

  // Check that AlwaysFalse() is updated correctly.
  BloomFilter* bf3 = CreateBloomFilter(BloomFilter::MinLogSpace(100, 0.01));
  BloomFilter* always_false = CreateBloomFilter(BloomFilter::MinLogSpace(100, 0.01));
  bf3->Or(*always_false);
  EXPECT_TRUE(bf3->AlwaysFalse());
  bf3->Or(*bf2);
  EXPECT_FALSE(bf3->AlwaysFalse());
  for (int i = 60; i < 80; ++i) ASSERT_TRUE(BfFind(*bf1, i)) << i;
  ASSERT_FALSE(BfFind(*bf1, 81));
}

TEST_F(BloomFilterTest, ProtobufOr) {
  BloomFilter* bf1 = CreateBloomFilter(BloomFilter::MinLogSpace(100, 0.01));
  BloomFilter* bf2 = CreateBloomFilter(BloomFilter::MinLogSpace(100, 0.01));

  for (int i = 60; i < 80; ++i) BfInsert(*bf2, i);
  for (int i = 0; i < 10; ++i) BfInsert(*bf1, i);

  bool success;
  BloomFilterPB protobuf;
  std::string directory;
  int64_t directory_size =
      BloomFilter::GetExpectedMemoryUsed(BloomFilter::MinLogSpace(100, 0.01));

  BfUnion(*bf1, *bf2, directory_size, &success, &protobuf, &directory);

  BloomFilter* bf3 = CreateBloomFilter(protobuf, directory);

  ASSERT_TRUE(success) << "SIMD BloomFilter::Union error";
  for (int i = 0; i < 10; ++i) ASSERT_TRUE(BfFind(*bf3, i)) << i;
  for (int i = 60; i < 80; ++i) ASSERT_TRUE(BfFind(*bf3, i)) << i;

  // Insert another value to aggregated BloomFilter.
  for (int i = 11; i < 50; ++i) BfInsert(*bf3, i);

  // Apply BloomFilterPB back to BloomFilter and verify if aggregation was correct.
  BfUnion(*bf1, *bf3, directory_size, &success, &protobuf, &directory);
  BloomFilter* bf4 = CreateBloomFilter(protobuf, directory);

  ASSERT_TRUE(success) << "SIMD BloomFilter::Union error";
  for (int i = 11; i < 50; ++i) ASSERT_TRUE(BfFind(*bf4, i)) << i;
  for (int i = 60; i < 80; ++i) ASSERT_TRUE(BfFind(*bf4, i)) << i;
  ASSERT_FALSE(BfFind(*bf4, 81));
}

}  // namespace impala

