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

#pragma once

using parquet::Encoding;

namespace impala {

template <typename InternalType_, parquet::Type::type PARQUET_TYPE_, bool MATERIALIZED_>
struct ParquetTypeDecoderBase {
public:
  typedef InternalType_ InternalType;
  static constexpr parquet::Type::type PARQUET_TYPE = PARQUET_TYPE_;
  static constexpr bool MATERIALIZED = MATERIALIZED_;

  /// Most column readers never require conversion, so we can avoid branches by
  /// returning constant false. Column readers for types that require conversion
  /// must specialize this function.
  bool NeedsConversionInline() const {
    return false;
  }

  /// Similar to NeedsConversion(), most column readers do not require validation,
  /// so to avoid branches, we return constant false. In general, types where not
  /// all possible bit representations of the data type are valid should be
  /// validated.
  bool NeedsValidationInline() const {
    return false;
  }

  int Size() { InternalType dummy; return ParquetPlainEncoder::ByteSize(dummy); }

  void Init(HdfsParquetScanner* parent, const SchemaNode& node,
      const SlotDescriptor* slot_desc){
  }

  int Decode(const uint8_t* buffer, const uint8_t* buffer_end, InternalType* v) {
    return ParquetPlainEncoder::Decode<InternalType, PARQUET_TYPE>(
            buffer, buffer_end, -1, v);
  }

  int64_t DecodeBatch(const uint8_t* buffer,
      const uint8_t* buffer_end, int64_t num_values, int64_t stride, InternalType* v) {
    return ParquetPlainEncoder::DecodeBatch<InternalType, PARQUET_TYPE>(
        buffer, buffer_end, -1, num_values, stride, v);
  }

  /// Converts and writes 'src' into 'slot' based on desc_->type()
  bool ConvertSlot(const InternalType* src, void* slot) {
    DCHECK(false);
    return false;
  }

  TErrorCode::type ValidateValue(InternalType* val) const {
    DCHECK(false);
    return TErrorCode::GENERAL;
  }
};

template <typename InternalType_, parquet::Type::type PARQUET_TYPE>
struct ParquetTypeDecoder : public ParquetTypeDecoderBase<InternalType_, PARQUET_TYPE, true>
{ };

struct ParquetCountStarDecoder : public ParquetTypeDecoderBase<int8_t, parquet::Type::INT32, false>
{ };

template <parquet::Type::type PARQUET_TYPE>
struct ParquetTimestampDecoderBase: public ParquetTypeDecoder<TimestampValue, PARQUET_TYPE> {
public:
  void Init(HdfsParquetScanner* parent, const SchemaNode& node, const SlotDescriptor* slot_desc) {
    DCHECK_EQ(slot_desc->type().type, TYPE_TIMESTAMP);
    timestamp_decoder_ = parent->CreateTimestampDecoder(*node.element);
  }

  bool NeedsValidationInline() const {
    return true;
  }

  bool NeedsConversionInline() const {
    return timestamp_decoder_.NeedsConversion();
  }

  bool ConvertSlot(const TimestampValue* src, void* slot) {
    DCHECK(NeedsConversionInline());
    TimestampValue* dst_ts = reinterpret_cast<TimestampValue*>(slot);
    *dst_ts = *src;
    // TODO: IMPALA-7862: converting timestamps after validating them can move them out of
    // range. We should either validate after conversion or require conversion to produce an
    // in-range value.
    timestamp_decoder_.ConvertToLocalTime(static_cast<TimestampValue*>(dst_ts));
    return true;
  }

  /// Contains extra data needed for Timestamp decoding.
  ParquetTimestampDecoder timestamp_decoder_;
};

struct ParquetInt64TimestampDecoder : public ParquetTimestampDecoderBase<parquet::Type::INT64>
{
public:
  int Decode(const uint8_t* buffer, const uint8_t* buffer_end, TimestampValue* v) {
    return timestamp_decoder_.Decode<parquet::Type::INT64>(buffer, buffer_end, v);
  }

  int64_t DecodeBatch(const uint8_t* buffer,
      const uint8_t* buffer_end, int64_t num_values, int64_t stride, TimestampValue* v) {
    return
        timestamp_decoder_.DecodeBatch<parquet::Type::INT64>(
            buffer, buffer_end, num_values, stride, v);
  }

  TErrorCode::type ValidateValue(TimestampValue* val) const {
    DCHECK(NeedsValidationInline());
    // The range was already checked during the int64_t->TimestampValue conversion, which
    // sets the date to invalid if it was out of range.
    if (UNLIKELY(!val->HasDate())) {
      return TErrorCode::PARQUET_TIMESTAMP_OUT_OF_RANGE;
    }
    DCHECK(TimestampValue::IsValidDate(val->date()));
    DCHECK(TimestampValue::IsValidTime(val->time()));
    return TErrorCode::OK;
  }
};

struct ParquetInt96TimestampDecoder : public ParquetTimestampDecoderBase<parquet::Type::INT96>
{
public:
  TErrorCode::type ValidateValue(TimestampValue* val) const {
    DCHECK(NeedsValidationInline());
    if (UNLIKELY(!TimestampValue::IsValidDate(val->date())
        || !TimestampValue::IsValidTime(val->time()))) {
      // If both are corrupt, invalid time takes precedence over invalid date, because
      // invalid date may come from a more or less functional encoder that does not respect
      // the 1400..9999 limit, while an invalid time is a good indicator of buggy encoder
      // or memory garbage.
      return TimestampValue::IsValidTime(val->time())
          ? TErrorCode::PARQUET_TIMESTAMP_OUT_OF_RANGE
          : TErrorCode::PARQUET_TIMESTAMP_INVALID_TIME_OF_DAY;
    }
    return TErrorCode::OK;
  }
};

struct ParquetBooleanDecoder : public ParquetTypeDecoder<bool, parquet::Type::BOOLEAN>
{  };

struct ParquetStringDecoder : public ParquetTypeDecoder<StringValue, parquet::Type::BYTE_ARRAY>
{
  int fixed_size_len_;
  int char_len_;
  bool needs_conversion_;

  void Init(HdfsParquetScanner* parent, const SchemaNode& node, const SlotDescriptor* slot_desc) {
    if (slot_desc->type().type == TYPE_VARCHAR) {
      fixed_size_len_ = slot_desc->type().len;
    } else {
      fixed_size_len_ = -1;
    }

    if (slot_desc->type().type == TYPE_CHAR) {
      needs_conversion_ = true;
      char_len_ = slot_desc->type().len;
    } else {
      needs_conversion_ = false;
      char_len_ = -1;
    }

    needs_conversion_ = slot_desc->type().type == TYPE_CHAR;
  }

  int Size() { return char_len_; }

  bool NeedsConversionInline() const {
    return needs_conversion_;
  }

  int Decode(const uint8_t* buffer, const uint8_t* buffer_end, InternalType* v) {
    return ParquetPlainEncoder::Decode<InternalType, PARQUET_TYPE>(
            buffer, buffer_end, fixed_size_len_, v);
  }

  int64_t DecodeBatch(const uint8_t* buffer,
      const uint8_t* buffer_end, int64_t num_values, int64_t stride, InternalType* v) {
    return ParquetPlainEncoder::DecodeBatch<InternalType, PARQUET_TYPE>(
        buffer, buffer_end, fixed_size_len_, num_values, stride, v);
  }

  bool ConvertSlot(const StringValue* src, void* slot) {
    DCHECK(needs_conversion_);
    int unpadded_len = std::min(char_len_, src->len);
    char* dst_char = reinterpret_cast<char*>(slot);
    memcpy(dst_char, src->ptr, unpadded_len);
    StringValue::PadWithSpaces(dst_char, char_len_, unpadded_len);
    return true;
  }
};

template <typename InternalType_>
struct ParquetFixedLenByteArrayDecimalDecoder : public ParquetTypeDecoder<InternalType_, parquet::Type::FIXED_LEN_BYTE_ARRAY> {
public:
  int fixed_len_size_;

  void Init(HdfsParquetScanner* parent, const SchemaNode& node, const SlotDescriptor* slot_desc) {
    DCHECK_EQ(slot_desc->type().type, TYPE_DECIMAL);
    fixed_len_size_ = node.element->type_length;
    DCHECK_GT(fixed_len_size_, 0);
  }

  int Size() { return fixed_len_size_; }

  int Decode(const uint8_t* buffer, const uint8_t* buffer_end, InternalType_* v) {
    return ParquetPlainEncoder::Decode<InternalType_, parquet::Type::FIXED_LEN_BYTE_ARRAY>(
            buffer, buffer_end, fixed_len_size_, v);
  }

  int64_t DecodeBatch(const uint8_t* buffer,
      const uint8_t* buffer_end, int64_t num_values, int64_t stride, InternalType_* v) {
    return ParquetPlainEncoder::DecodeBatch<InternalType_, parquet::Type::FIXED_LEN_BYTE_ARRAY>(
        buffer, buffer_end, fixed_len_size_, num_values, stride, v);
  }
};

template <typename T, class TypeDecoder>
bool DecodePlainParquetPageToVector(uint8_t* values, int size, TypeDecoder& type_decoder,
    std::vector<T>& results) {
  uint8_t* it = values;
  uint8_t* end = values + size;
  while (it < end) {
    T value;
    int decoded_len = type_decoder.Decode(it, end, &value);
    if (UNLIKELY(decoded_len < 0)) {
      return false;
    }
    it += decoded_len;
    results.push_back(value);
  }
  return true;
}

} // namespace impala
