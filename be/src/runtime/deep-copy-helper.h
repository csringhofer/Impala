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

#include <vector>

#include "codegen/impala-ir.h"
#include "gutil/macros.h"
#include "runtime/collection-value.h"
#include "runtime/descriptors.h"
#include "runtime/mem-pool.h"
#include "runtime/tuple-row.h"
#include "util/ubsan.h"

namespace llvm {
class Function;
class Constant;
}

namespace impala {

struct CollectionValue;
class RuntimeState;
class RowDescriptor;
class StringValue;
class ScalarExpr;
class ScalarExprEvaluator;
class TupleDescriptor;
class TupleRow;

/// Minimal struct to hold slot offset information from a SlotDescriptor. Designed
/// to simplify constant substitution in CodegenCopyStrings() and allow more efficient
/// interpretation in CopyStrings().
struct SlotOffsets {
  NullIndicatorOffset null_indicator_offset;
  int tuple_offset;

  /// Generate an LLVM Constant containing the offset values of this SlotOffsets instance.
  /// Needs to be updated if the layout of this struct changes.
  llvm::Constant* ToIR(LlvmCodeGen* codegen) const;

  static const char* LLVM_CLASS_NAME;
};

struct TupleDeepCopyInfo {
  int tuple_size = 0;
  int first_string_slot = 0; // First string slot index in DeepCopyHelper.string_slots_
  int num_string_slots = 0;
  bool nullable = false;
  bool has_collection_slots = false;

  /// Generate an LLVM Constant containing the offset values of this TupleDeepcopyInfo
  /// instance. Needs to be updated if the layout of this struct changes.
  llvm::Constant* ToIR(LlvmCodeGen* codegen) const;

  static const char* LLVM_CLASS_NAME;
};

class DeepCopyHelper {
  vector<TupleDeepCopyInfo> tuple_info_;
  vector<SlotOffsets> string_slots_;
  int num_tuples_;

public:
  DeepCopyHelper(const RowDescriptor* row_desc);

  int IR_NO_INLINE num_tuples_no_inline() const { return num_tuples_; }

  const TupleDeepCopyInfo* IR_NO_INLINE tuple_info_no_inline() const {
    return tuple_info_.data();
  }

  const SlotOffsets* IR_NO_INLINE string_slots_no_inline() const {
    return string_slots_.data();
  }

  llvm::Constant* GetTuplesAsIrConstant(LlvmCodeGen* codegen, const char* global_name) const;
  llvm::Constant* GetStringSlotsAsIrConstant(
      LlvmCodeGen* codegen, const char* global_name) const;

  //void ReplaceWithConstantsInIR( Codegen* codegen, int expected_num_tuples_calls,
  //    int expected_tuples_call, int expected_string_slots_calls);
};

}
