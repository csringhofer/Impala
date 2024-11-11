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

#include "runtime/outbound-row-batch.h"
#include "runtime/outbound-row-batch.inline.h"
#include "util/compress.h"
#include "util/fixed-size-hash-table.h"
#include "util/scope-exit-trigger.h"

namespace impala {

Status OutboundRowBatch::PrepareForSend(int num_tuples_per_row,
    TrackedString* compression_scratch, bool used_append_row) {
  if (used_append_row) {
    DCHECK_GE(tuple_data_.size(), tuple_data_offset_);
    tuple_data_.resize(tuple_data_offset_);
  } else {
    DCHECK_EQ(tuple_data_offset_, 0);
  }
  bool is_compressed = false;
  int64_t uncompressed_size = tuple_data_.size();
  if (uncompressed_size > 0 && compression_scratch != nullptr) {
    RETURN_IF_ERROR(TryCompress(compression_scratch, &is_compressed));
  }
  int num_tuples = tuple_offsets_.size();
  DCHECK_EQ(num_tuples % num_tuples_per_row, 0);
  int num_rows = num_tuples / num_tuples_per_row;
  SetHeader(num_rows, num_tuples_per_row, uncompressed_size, is_compressed);
  return Status::OK();
}

Status OutboundRowBatch::TryCompress(TrackedString* compression_scratch,
    bool* is_compressed) {
  DCHECK(compression_scratch != nullptr);
  Lz4Compressor compressor(nullptr, false);
  RETURN_IF_ERROR(compressor.Init());
  auto compressor_cleanup =
      MakeScopeExitTrigger([&compressor]() { compressor.Close(); });

  *is_compressed = false;
  int64_t uncompressed_size = tuple_data_.size();
  // If the input size is too large for LZ4 to compress, MaxOutputLen() will return 0.
  int64_t compressed_size = compressor.MaxOutputLen(uncompressed_size);
  if (compressed_size == 0) {
      return Status(TErrorCode::LZ4_COMPRESSION_INPUT_TOO_LARGE, uncompressed_size);
  }
  DCHECK_GT(compressed_size, 0);
  if (compression_scratch->size() < compressed_size) {
      compression_scratch->resize(compressed_size);
  }

  uint8_t* input = reinterpret_cast<uint8_t*>(tuple_data_.data());
  uint8_t* compressed_output = const_cast<uint8_t*>(
      reinterpret_cast<const uint8_t*>(compression_scratch->data()));
  RETURN_IF_ERROR(compressor.ProcessBlock(
      true, uncompressed_size, input, &compressed_size, &compressed_output));
  if (LIKELY(compressed_size < uncompressed_size)) {
      compression_scratch->resize(compressed_size);
      tuple_data_.swap(*compression_scratch);
      *is_compressed = true;
      // TODO: could copy to a smaller buffer if compressed data is much smaller to
      //       save memory
  }
  VLOG_ROW << "uncompressed size: " << uncompressed_size << ", compressed size: "
      << compressed_size;
  return Status::OK();
}

void OutboundRowBatch::SetHeader(int num_rows, int num_tuples_per_row,
    int64_t uncompressed_size, bool is_compressed) {
  bool need_tuple_offsets = num_rows == 1; // TODO 1 row with 0 size tuple could pass the next loop, maybe find another way?
  int last_offset = -1;
  for (int offset: tuple_offsets_) {
    //LOG(INFO) << "offset " << offset;
    // Only keep need_tuple_offsets false if offsets are monotonically increasing.
    if (offset <= last_offset) {
      //LOG(INFO) << "last offset " << last_offset;
      need_tuple_offsets = true;
      break;
    }
    last_offset = offset;
  }
  header_.Clear();
  header_.set_num_rows(num_rows);
  header_.set_num_tuples_per_row(num_tuples_per_row);
  header_.set_uncompressed_size(uncompressed_size);
  header_.set_compression_type(
      is_compressed ? CompressionTypePB::LZ4 : CompressionTypePB::NONE);
  header_.set_has_tuple_offsets_sidecar(need_tuple_offsets);
}

void OutboundRowBatch::Reset() {
  header_.Clear();
  tuple_offsets_.clear();
  tuple_data_offset_ = 0;
  // Do not clear tuple_data_ to avoid unnecessary delete + allocate.
}

Status OutboundRowBatch::AppendRowWithDedup(
    const TupleRow* row, const TupleRow* prev_row, DedupMap* distinct_tuples,
    const RowDescriptor* row_desc) {
  DCHECK(row != nullptr);
  int num_tuples = row_desc->num_tuples_no_inline();
  vector<TupleDescriptor*>::const_iterator desc =
      row_desc->tuple_descriptors().begin();
  for (int j = 0; j < num_tuples; ++desc, ++j) {
    Tuple* tuple = row->GetTuple(j);
      if (UNLIKELY(tuple == nullptr)) {
        // NULLs are encoded as -1
        tuple_offsets_.push_back(-1);
        // LOG(INFO) << j << " new offset: " << tuple_offsets_.back();
        continue;
      } else if (prev_row != nullptr && UNLIKELY(prev_row->GetTuple(j) == tuple)) {
        // Fast tuple deduplication for adjacent rows.
        DCHECK_GT(tuple_offsets_.size(), 0);
        int prev_tuple_idx = tuple_offsets_.size() - num_tuples;
        DCHECK_GE(prev_tuple_idx, 0);
        tuple_offsets_.push_back(tuple_offsets_[prev_tuple_idx]);
        // LOG(INFO) << j << " new offset: " << tuple_offsets_.back();
        continue;
      } else if (UNLIKELY(distinct_tuples != nullptr)) {
        if ((*desc)->byte_size() == 0) {
          // Zero-length tuples can be represented as nullptr.
          tuple_offsets_.push_back(-1);
          // LOG(INFO) << j <<" new offset: " << tuple_offsets_.back();
          continue;
        }
        int* dedupd_offset = distinct_tuples->FindOrInsert(tuple, tuple_data_offset_);
        if (*dedupd_offset != tuple_data_offset_) {
          // Repeat of tuple
          DCHECK_GE(*dedupd_offset, 0);
          tuple_offsets_.push_back(*dedupd_offset);
          // LOG(INFO) << j<<" new offset: " << tuple_offsets_.back();
          continue;
        }
      } /*else if (desc->ByteSize() == 0) {
        tuple_offsets_.push_back(-2);
        LOG(INFO) << j << " new offset: " << tuple_offsets_.back();
        continuel
      }*/
      // Record offset before creating copy (which increments offset and tuple_data)
      tuple_offsets_.push_back(tuple_data_offset_);
      // LOG(INFO) << j << " new offset: " << tuple_offsets_.back();
      RETURN_IF_ERROR(AppendTuple(tuple, *desc));
      DCHECK_LE(tuple_data_offset_, tuple_data_.size());
  }
  return Status::OK();
}

}
