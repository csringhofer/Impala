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

#include "runtime/outbound-row-batch.h"

#include "runtime/descriptors.h"
#include "runtime/tuple.h"
#include "runtime/tuple-row.h"

namespace impala {

void OutboundRowBatch::AppendRow(const TupleRow* row, const RowDescriptor* row_desc) {
  DCHECK(row != nullptr);
  vector<TupleDescriptor*>::const_iterator desc =
      row_desc->tuple_descriptors().begin();
  for (int j = 0; desc != row_desc->tuple_descriptors().end(); ++desc, ++j) {
    Tuple* tuple = row->GetTuple(j);
    if (UNLIKELY(tuple == nullptr)) {
      // NULLs are encoded as -1
      tuple_offsets_.push_back(-1);
      continue;
    } 
    // Record offset before creating copy (which increments offset and tuple_data)
    tuple_offsets_.push_back(tuple_data_offset_);
    char* tuple_data = const_cast<char*>(tuple_data_.data() + tuple_data_offset_);
    DCHECK(tuple != nullptr);
    DCHECK(*desc != nullptr);
    DCHECK_GE(tuple_data_size_,0);
    DCHECK_GE(tuple_data_.size(), 0);
    tuple->DeepCopy(**desc, &tuple_data, &tuple_data_offset_, /* convert_ptrs */ true);
    DCHECK_LE(tuple_data_offset_, tuple_data_.size());
  }
}

}
