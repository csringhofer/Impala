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

#include "runtime/deep-copy-helper.h"

#include <vector>
#include "llvm/IR/Function.h"

#include "codegen/llvm-codegen.h"
#include "runtime/descriptors.h"

#include "common/names.h"

namespace impala {

const char* SlotOffsets::LLVM_CLASS_NAME = "struct.impala::SlotOffsets";
const char* TupleDeepCopyInfo::LLVM_CLASS_NAME = "struct.impala::TupleDeepCopyInfo";

llvm::Constant* SlotOffsets::ToIR(LlvmCodeGen* codegen) const {
  return llvm::ConstantStruct::get(
      codegen->GetStructType<SlotOffsets>(),
      {null_indicator_offset.ToIR(codegen),
          codegen->GetI32Constant(tuple_offset)});
}

llvm::Constant* TupleDeepCopyInfo::ToIR(LlvmCodeGen* codegen) const {
  return llvm::ConstantStruct::get(
      codegen->GetStructType<TupleDeepCopyInfo>(), {
          codegen->GetI32Constant(tuple_size),
          codegen->GetI32Constant(first_string_slot),
          codegen->GetI32Constant(num_string_slots),
          codegen->GetBoolConstant(nullable),
          codegen->GetBoolConstant(has_collection_slots)
          });
}

DeepCopyHelper::DeepCopyHelper(const RowDescriptor* row_desc) {
  num_tuples_ = row_desc->tuple_descriptors().size();
  tuple_info_.resize(num_tuples_);
  for (int i = 0; i < num_tuples_; i++) {
    const TupleDescriptor* tuple_desc = row_desc->tuple_descriptors()[i];
    TupleDeepCopyInfo& tuple = tuple_info_[i];
    tuple.tuple_size = tuple_desc->byte_size();
    LOG(INFO) << "tuple size: " << tuple.tuple_size;
    tuple.first_string_slot = string_slots_.size();
    tuple.num_string_slots = tuple_desc->string_slots().size();
    tuple.nullable = row_desc->TupleIsNullable(i);
    tuple.has_collection_slots = !tuple_desc->collection_slots().empty();
    string_slots_.resize(string_slots_.size() +  tuple.num_string_slots);
    for (int j = 0; j < tuple.num_string_slots; j++) {
      SlotOffsets& string_slot = string_slots_[tuple.first_string_slot + j];
      const SlotDescriptor* slot_desc = tuple_desc->string_slots()[j];
      string_slot.null_indicator_offset = slot_desc->null_indicator_offset();
      string_slot.tuple_offset = slot_desc->tuple_offset();
    }
  }
  if (string_slots_.empty()) string_slots_.emplace_back(); // add dummy to avoid empty array
}

//void DeepCopyHelper::ReplaceWithConstantsInIR(Codegen* codegen, int expected_num_tuples_calls,
//    int expected_tuples_call, int expected_string_slots_calls) {
//
//}

llvm::Constant* DeepCopyHelper::GetTuplesAsIrConstant(
    LlvmCodeGen* codegen, const char* global_name) const {
  DCHECK_GT(num_tuples_, 0);
  vector<llvm::Constant*> tuple_ir_constants;
  for (const TupleDeepCopyInfo& tuple : tuple_info_) {
    tuple_ir_constants.push_back(tuple.ToIR(codegen));
  }
  llvm::StructType* tuple_type = codegen->GetStructType<TupleDeepCopyInfo>();
  return codegen->ConstantsToGVArrayPtr(tuple_type, tuple_ir_constants, global_name);
}

llvm::Constant* DeepCopyHelper::GetStringSlotsAsIrConstant(
    LlvmCodeGen* codegen, const char* global_name) const {
  DCHECK_GT(num_tuples_, 0);
  if (string_slots_.size() == 0) return codegen->null_ptr_value();
  vector<llvm::Constant*> slot_offset_ir_constants;
  for (const SlotOffsets& slot : string_slots_) {
    slot_offset_ir_constants.push_back(slot.ToIR(codegen));
  }
  llvm::StructType* slot_offsets_type = codegen->GetStructType<SlotOffsets>();
  llvm::Constant* constant_slot_offsets = codegen->ConstantsToGVArrayPtr(
      slot_offsets_type, slot_offset_ir_constants, global_name);

  return constant_slot_offsets;
}

} // namespace impala