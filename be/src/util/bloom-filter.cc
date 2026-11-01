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
#include <math.h>
#include <string.h>

#include <cmath>
#include <cstdint>
#include <memory>
#include <ostream>

#include <gflags/gflags.h>

#include "gen-cpp/data_stream_service.pb.h"
#include "kudu/rpc/rpc_controller.h"
#include "kudu/rpc/rpc_sidecar.h"
#include "kudu/util/block_bloom_filter.h"
#include "kudu/util/slice.h"
#include "kudu/util/status.h"
#include "runtime/exec-env.h"
#include "runtime/mem-tracker.h"
#include "util/kudu-status-util.h"

using namespace std;

// Filters larger than the L2 cache of a core (1-2 MB on current server CPUs) are mostly
// read from L3 or memory. In a microbenchmark with a 30 MB L3 cache, prefetching broke
// even at 8 MB single-threaded, but at 1-4 MB with three filters per row on two
// hyperthreads of a core.
DEFINE_int64(bloom_filter_prefetch_min_bytes, 2L * 1024 * 1024, "(Advanced) Runtime "
    "Bloom filters of at least this many bytes are built and probed in batches whose "
    "buckets are prefetched first, so that the cache misses of a batch overlap. Applies "
    "to filters built by hash joins and evaluated by Parquet and ORC scans. Smaller "
    "filters mostly stay in the CPU cache, so prefetching would only slow them down. "
    "A negative value disables prefetching.");

namespace impala {

constexpr BloomFilter* const BloomFilter::ALWAYS_TRUE_FILTER;

BloomFilter::BloomFilter(BufferPool::ClientHandle* client)
  : buffer_allocator_(client), block_bloom_filter_(&buffer_allocator_) {}

BloomFilter::~BloomFilter() {}

Status BloomFilter::Init(const int log_bufferpool_space, uint32_t hash_seed) {
  hash_seed_ = hash_seed;
  KUDU_RETURN_IF_ERROR(
      block_bloom_filter_.Init(log_bufferpool_space, kudu::FAST_HASH, hash_seed),
      "Failed to init Block Bloom Filter");
  InitDeferredInserts();
  return Status::OK();
}

Status BloomFilter::Init(const BloomFilterPB& protobuf, const uint8_t* directory_in,
    size_t directory_in_size, uint32_t hash_seed) {
  hash_seed_ = hash_seed;
  if (protobuf.always_false() || directory_in_size == 0) {
    // Directory size equal 0 only when it's always false.
    KUDU_RETURN_IF_ERROR(block_bloom_filter_.Init(
                             protobuf.log_bufferpool_space(), kudu::FAST_HASH, hash_seed),
        "Failed to init Block Bloom Filter");
  } else {
    kudu::Slice slice(directory_in, directory_in_size);
    KUDU_RETURN_IF_ERROR(
        block_bloom_filter_.InitFromDirectory(
            protobuf.log_bufferpool_space(), slice, false, kudu::FAST_HASH, hash_seed),
        "Failed to init Block Bloom Filter");
  }
  InitDeferredInserts();
  return Status::OK();
}

void BloomFilter::InitDeferredInserts() {
  num_deferred_inserts_ = 0;
  defer_inserts_ = ShouldPrefetch(block_bloom_filter_.GetSpaceUsed());
}

void BloomFilter::Close() {
  // Hashes buffered by InsertDeferred() would not fit into a re-initialized directory.
  num_deferred_inserts_ = 0;
  block_bloom_filter_.Close();
}

bool BloomFilter::ShouldPrefetch(int64_t directory_bytes) {
  return FLAGS_bloom_filter_prefetch_min_bytes >= 0
      && directory_bytes >= FLAGS_bloom_filter_prefetch_min_bytes;
}

void BloomFilter::FlushDeferredInserts() noexcept {
  const int n = num_deferred_inserts_;
  if (n == 0) return;
  // Issue all prefetches first, then insert: the inserts find their buckets in the
  // cache instead of waiting for each cache miss in turn.
  for (int i = 0; i < n; ++i) Prefetch(deferred_inserts_[i], /*for_write=*/true);
  for (int i = 0; i < n; ++i) block_bloom_filter_.Insert(deferred_inserts_[i]);
  num_deferred_inserts_ = 0;
}

namespace {

/// Number of 32-bit words in a bucket of kudu::BlockBloomFilter.
constexpr int BUCKET_WORDS = 8;

/// Sum over the buckets of the folded directory 'lo[i] | hi[i]' (or just 'lo[i]' if 'hi'
/// is null) of the product of the number of set bits in each word of the bucket. The
/// fpp is this sum divided by 32^8 and by the number of buckets. Stops early and returns
/// a value above 'limit' as soon as the sum exceeds 'limit'.
double SumOfBucketProducts(const uint32_t* lo, const uint32_t* hi, int64_t num_buckets,
    double limit) {
  double sum = 0;
  // Check the limit every 1024 buckets.
  constexpr int64_t CHECK_INTERVAL = 1024;
  for (int64_t start = 0; start < num_buckets; start += CHECK_INTERVAL) {
    const int64_t end = std::min(num_buckets, start + CHECK_INTERVAL);
    for (int64_t b = start; b < end; ++b) {
      // The product of 8 counts of at most 32 fits into 40 bits.
      uint64_t product = 1;
      for (int w = 0; w < BUCKET_WORDS; ++w) {
        const int64_t i = b * BUCKET_WORDS + w;
        const uint32_t word = hi == nullptr ? lo[i] : (lo[i] | hi[i]);
        product *= __builtin_popcount(word);
      }
      sum += static_cast<double>(product);
    }
    if (sum > limit) return sum;
  }
  return sum;
}

/// 32^8: the product of the bit counts of a bucket with all bits set.
constexpr double FULL_BUCKET_PRODUCT = 1099511627776.0;

} // namespace

double BloomFilter::EstimateFpp(const uint8_t* directory, int64_t directory_size) {
  const int64_t num_buckets = directory_size / (BUCKET_WORDS * sizeof(uint32_t));
  DCHECK_GT(num_buckets, 0);
  const double sum = SumOfBucketProducts(reinterpret_cast<const uint32_t*>(directory),
      nullptr, num_buckets, std::numeric_limits<double>::infinity());
  return sum / FULL_BUCKET_PRODUCT / num_buckets;
}

int BloomFilter::ShrinkDirectory(
    uint8_t* directory, int log_space, double max_fpp, int min_log_space) {
  // Kudu's smallest directory has 2 buckets.
  min_log_space = std::max(min_log_space, LOG_BUCKET_BYTE_SIZE + 1);
  while (log_space > min_log_space) {
    const int64_t half_size = int64_t{1} << (log_space - 1);
    const int64_t num_buckets = half_size >> LOG_BUCKET_BYTE_SIZE;
    // Check the fpp of the folded directory before folding, so that the directory stays
    // unchanged if the fpp would be too high.
    const double limit = max_fpp * FULL_BUCKET_PRODUCT * num_buckets;
    const uint32_t* lo = reinterpret_cast<const uint32_t*>(directory);
    const uint32_t* hi = reinterpret_cast<const uint32_t*>(directory + half_size);
    if (SumOfBucketProducts(lo, hi, num_buckets, limit) > limit) break;
    kudu::BlockBloomFilter::OrEqualArray(half_size, directory + half_size, directory);
    --log_space;
  }
  return log_space;
}

Status BloomFilter::Shrink(
    double max_fpp, int min_log_space, MemTracker* tracker, bool* shrunk) {
  DCHECK(!HasDeferredInserts());
  *shrunk = false;
  const int log_space = block_bloom_filter_.log_space_bytes();
  if (AlwaysFalse() || log_space <= min_log_space) return Status::OK();
  // The copy of the shrunk directory is at most half of the current size. Reserve it
  // before changing the directory, so that the filter is either shrunk or unchanged.
  const int64_t max_copy_size = directory_size() / 2;
  if (!tracker->TryConsume(max_copy_size)) return Status::OK();
  uint8_t* directory = const_cast<uint8_t*>(block_bloom_filter_.directory().data());
  const int new_log_space = ShrinkDirectory(directory, log_space, max_fpp, min_log_space);
  if (new_log_space == log_space) {
    tracker->Release(max_copy_size);
    return Status::OK();
  }
  // InitFromDirectory() frees the current directory before allocating the new one, so
  // copy the shrunk directory first.
  const int64_t new_size = int64_t{1} << new_log_space;
  unique_ptr<uint8_t[]> copy(new uint8_t[new_size]);
  memcpy(copy.get(), directory, new_size);
  kudu::Status status = block_bloom_filter_.InitFromDirectory(new_log_space,
      kudu::Slice(copy.get(), new_size), /*always_false=*/false, kudu::FAST_HASH,
      hash_seed_);
  copy.reset();
  tracker->Release(max_copy_size);
  KUDU_RETURN_IF_ERROR(status, "Failed to shrink Block Bloom Filter");
  InitDeferredInserts();
  *shrunk = true;
  return Status::OK();
}

void BloomFilter::AddDirectorySidecar(BloomFilterPB* rpc_params,
    kudu::rpc::RpcController* controller, const char* directory,
    unsigned long directory_size) {
  DCHECK(rpc_params != nullptr);
  DCHECK(!rpc_params->always_false());
  DCHECK(!rpc_params->always_true());
  kudu::Slice dir_slice(directory, directory_size);
  unique_ptr<kudu::rpc::RpcSidecar> rpc_sidecar =
      kudu::rpc::RpcSidecar::FromSlice(dir_slice);

  int sidecar_idx = -1;
  kudu::Status sidecar_status =
      controller->AddOutboundSidecar(std::move(rpc_sidecar), &sidecar_idx);
  if (!sidecar_status.ok()) {
    LOG(ERROR) << "Cannot add outbound sidecar: " << sidecar_status.message().ToString();
    // If AddOutboundSidecar() fails, we 'disable' the BloomFilterPB by setting it to
    // an always true filter.
    rpc_params->set_always_false(false);
    rpc_params->set_always_true(true);
    return;
  }
  rpc_params->set_directory_sidecar_idx(sidecar_idx);
  rpc_params->set_always_false(false);
  rpc_params->set_always_true(false);
}

void BloomFilter::AddDirectorySidecar(BloomFilterPB* rpc_params,
    kudu::rpc::RpcController* controller, const string& directory) {
  AddDirectorySidecar(rpc_params, controller,
      reinterpret_cast<const char*>(&(directory[0])),
      static_cast<unsigned long>(directory.size()));
}

void BloomFilter::ToProtobuf(
    BloomFilterPB* protobuf, kudu::rpc::RpcController* controller) const {
  DCHECK(!HasDeferredInserts());
  protobuf->set_log_bufferpool_space(block_bloom_filter_.log_space_bytes());
  if (AlwaysFalse()) {
    protobuf->set_always_false(true);
    protobuf->set_always_true(false);
    return;
  }
  kudu::Slice directory = block_bloom_filter_.directory();
  BloomFilter::AddDirectorySidecar(protobuf, controller,
      reinterpret_cast<const char*>(directory.data()),
      static_cast<unsigned long>(directory.size()));
}

void BloomFilter::ToProtobuf(const BloomFilter* filter,
    kudu::rpc::RpcController* controller, BloomFilterPB* protobuf) {
  DCHECK(protobuf != nullptr);
  // If filter == nullptr, then this BloomFilter is an always true filter.
  if (filter == nullptr) {
    protobuf->set_always_true(true);
    DCHECK(!protobuf->always_false());
    return;
  }
  filter->ToProtobuf(protobuf, controller);
}

int64_t BloomFilter::GetBufferPoolSpaceUsed() {
  return buffer_allocator_.IsAllocated() ? block_bloom_filter_.GetSpaceUsed() : -1;
}

void BloomFilter::Or(const BloomFilter& other) {
  DCHECK_NE(this, &other);
  DCHECK_NE(&other, ALWAYS_TRUE_FILTER);
  DCHECK(!HasDeferredInserts());
  if (other.AlwaysFalse()) return;
  DCHECK_EQ(
      block_bloom_filter_.log_space_bytes(), other.block_bloom_filter_.log_space_bytes());
  block_bloom_filter_.Or(other.block_bloom_filter_);
}

void BloomFilter::RawOr(const BloomFilter& other) {
  DCHECK_NE(this, &other);
  DCHECK_NE(&other, ALWAYS_TRUE_FILTER);
  DCHECK(!HasDeferredInserts());
  DCHECK(!other.HasDeferredInserts());
  DCHECK_EQ(
      block_bloom_filter_.log_space_bytes(), other.block_bloom_filter_.log_space_bytes());
  kudu::Slice target_slice = block_bloom_filter_.directory();
  kudu::Slice input_slice = other.block_bloom_filter_.directory();
  kudu::BlockBloomFilter::OrEqualArray(
      target_slice.size(), input_slice.data(), const_cast<uint8_t*>(target_slice.data()));
}

void BloomFilter::Or(const BloomFilterPB& in, const kudu::Slice& input_slice) {
  DCHECK_NE(this, BloomFilter::ALWAYS_TRUE_FILTER);
  DCHECK(!HasDeferredInserts());
  DCHECK(!in.always_true());
  if (in.always_false()) return;
  DCHECK_EQ(in.log_bufferpool_space(), block_bloom_filter_.log_space_bytes());
  kudu::Slice target_slice = block_bloom_filter_.directory();
  DCHECK_EQ(input_slice.size(), target_slice.size());
  kudu::BlockBloomFilter::OrEqualArray(
      target_slice.size(), input_slice.data(), const_cast<uint8_t*>(target_slice.data()));
}

void BloomFilter::Or(const BloomFilterPB& in, const uint8_t* directory_in,
    BloomFilterPB* out, uint8_t* directory_out, size_t directory_size) {
  DCHECK(out != nullptr);
  DCHECK_NE(&in, out);
  // These cases are impossible in current code. If they become possible in the future,
  // memory usage should be tracked accordingly.
  DCHECK(!out->always_false());
  DCHECK(!out->always_true());
  DCHECK(!in.always_true());
  if (in.always_false()) return;
  DCHECK_EQ(in.log_bufferpool_space(), out->log_bufferpool_space());
  kudu::BlockBloomFilter::OrEqualArray(directory_size, directory_in, directory_out);
}

} // namespace impala
