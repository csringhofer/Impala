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

#ifndef IMPALA_RUNTIME_BUFFERED_TUPLE_STREAM_INLINE_H
#define IMPALA_RUNTIME_BUFFERED_TUPLE_STREAM_INLINE_H

#include "runtime/buffered-tuple-stream.h"

#include "runtime/descriptors.h"
#include "runtime/tuple-row.h"
#include "util/bit-util.h"

namespace impala {

inline int BufferedTupleStream::NullIndicatorBytesPerRow() const {
  DCHECK(has_nullable_tuple_);
  return BitUtil::RoundUpNumBytes(fixed_tuple_sizes_.size());
}

inline uint8_t* BufferedTupleStream::AddRowCustomBegin(int64_t size, Status* status) {
  DCHECK(!closed_);
  DCHECK(has_write_iterator());
  if (UNLIKELY(write_page_ == nullptr || write_ptr_ + size > write_end_ptr_)) {
    return AddRowCustomBeginSlow(size, status);
  }
  DCHECK(write_page_ != nullptr);
  DCHECK(write_page_->is_pinned());
  DCHECK_LE(write_ptr_ + size, write_end_ptr_);
  ++num_rows_;
  ++write_page_->num_rows;

  uint8_t* data = write_ptr_;
  write_ptr_ += size;
  return data;
}

inline void BufferedTupleStream::AddRowCustomEnd(int64_t size) {
  if (UNLIKELY(size > default_page_len_)) AddLargeRowCustomEnd(size);
}

inline void BufferedTupleStream::GetTupleRow(FlatRowPtr flat_row, TupleRow* row) const {
  DCHECK(row != nullptr);
  DCHECK(!closed_);
  DCHECK(is_pinned());
  DCHECK(!read_it_.attach_on_read_);
  uint8_t* data = flat_row;
  return has_nullable_tuple_ ? UnflattenTupleRow<true>(&data, row) :
                               UnflattenTupleRow<false>(&data, row);
}

template <bool HAS_NULLABLE_TUPLE>
inline void BufferedTupleStream::UnflattenTupleRow(uint8_t** data, TupleRow* row) const {
  const int tuples_per_row = desc_->tuple_descriptors().size();
  uint8_t* ptr = *data;
  if (HAS_NULLABLE_TUPLE) {
    // Stitch together the tuples from the page and the NULL ones.
    const uint8_t* null_indicators = ptr;
    ptr += NullIndicatorBytesPerRow();
    for (int i = 0; i < tuples_per_row; ++i) {
      const uint8_t* null_word = null_indicators + (i >> 3);
      const uint32_t null_pos = i & 7;
      const bool is_not_null = ((*null_word & (1 << (7 - null_pos))) == 0);
      row->SetTuple(
          i, reinterpret_cast<Tuple*>(reinterpret_cast<uint64_t>(ptr) * is_not_null));
      ptr += fixed_tuple_sizes_[i] * is_not_null;
    }
  } else {
    for (int i = 0; i < tuples_per_row; ++i) {
      row->SetTuple(i, reinterpret_cast<Tuple*>(ptr));
      ptr += fixed_tuple_sizes_[i];
    }
  }
  *data = ptr;
}

inline bool IR_ALWAYS_INLINE BufferedTupleStream::AddRowInline(
    TupleRow* row, bool has_var_len_data, Status* status) noexcept {
  DCHECK(!closed_);
  DCHECK(has_write_iterator());
  if (UNLIKELY(write_page_ == nullptr || 
      !DeepCopyInternal(row, &write_ptr_, write_end_ptr_, has_var_len_data))) {
    return AddRowSlow(row, status);
  }
  DCHECK_LT(num_rows_, INT64_MAX);
  DCHECK_LT(write_page_->num_rows, INT64_MAX);
  ++num_rows_;
  ++write_page_->num_rows;
  return true;
}

// TODO: consider codegening this.
// TODO: in case of duplicate tuples, this can redundantly serialize data.
inline bool IR_ALWAYS_INLINE BufferedTupleStream::DeepCopyInternal(
    TupleRow* row, uint8_t** data, const uint8_t* data_end, bool has_var_len_data) noexcept {
  uint8_t* pos = *data;
  const uint64_t tuples_per_row = desc_->num_tuples_no_inline();
  const bool has_nullable_tuple = desc_->has_nullable_tuple_no_inline();
  // Copy the not NULL fixed len tuples. For the NULL tuples just update the NULL tuple
  // indicator.
  if (has_nullable_tuple) {
    int null_indicator_bytes = NullIndicatorBytesPerRow();
    if (UNLIKELY(pos + null_indicator_bytes > data_end)) return false;
    uint8_t* null_indicators = pos;
    pos += NullIndicatorBytesPerRow();
    memset(null_indicators, 0, null_indicator_bytes);
    for (int i = 0; i < tuples_per_row; ++i) {
      uint8_t* null_word = null_indicators + (i >> 3);
      const uint32_t null_pos = i & 7;
      const int tuple_size = fixed_tuple_sizes_[i];
      Tuple* t = row->GetTuple(i);
      const uint8_t mask = 1 << (7 - null_pos);
      if (t != nullptr) {
        if (UNLIKELY(pos + tuple_size > data_end)) return false;
        memcpy(pos, t, tuple_size);
        pos += tuple_size;
      } else {
        *null_word |= mask;
      }
    }
  } else {
    // If we know that there are no nullable tuples no need to set the nullability flags.
    for (int i = 0; i < tuples_per_row; ++i) {
      const int tuple_size = i == 0 ? desc_->first_tuple_size_no_inline() : fixed_tuple_sizes_[i];
      if (UNLIKELY(pos + tuple_size > data_end)) return false;
      Tuple* t = row->GetTuple(i);
      // TODO: Once IMPALA-1306 (Avoid passing empty tuples of non-materialized slots)
      // is delivered, the check below should become DCHECK(t != nullptr).
      DCHECK(t != nullptr || tuple_size == 0);
      memcpy(pos, t, tuple_size);
      pos += tuple_size;
    }
  }

  if (!has_var_len_data) {
    *data = pos;
    return true;
  }

  // Copy inlined string slots. Note: we do not need to convert the string ptrs to offsets
  // on the write path, only on the read. The tuple data is immediately followed
  // by the string data so only the len information is necessary.
  for (int i = 0; i < inlined_string_slots_.size(); ++i) {
    const Tuple* tuple = row->GetTuple(inlined_string_slots_[i].first);
    if (has_nullable_tuple && tuple == nullptr) continue;
    if (UNLIKELY(!CopyStrings(tuple, inlined_string_slots_[i].second, &pos, data_end)))
      return false;
  }

  // Copy inlined collection slots. We copy collection data in a well-defined order so
  // we do not need to convert pointers to offsets on the write path.
  for (int i = 0; i < inlined_coll_slots_.size(); ++i) {
    const Tuple* tuple = row->GetTuple(inlined_coll_slots_[i].first);
    if (has_nullable_tuple && tuple == nullptr) continue;
    if (UNLIKELY(!CopyCollections(tuple, inlined_coll_slots_[i].second, &pos, data_end)))
      return false;
  }
  *data = pos;
  return true;
}


}

#endif
