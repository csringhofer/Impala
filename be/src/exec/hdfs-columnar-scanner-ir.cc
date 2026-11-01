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

#include <algorithm>

#include "exec/exec-node.inline.h"
#include "exec/hdfs-columnar-scanner.h"
#include "runtime/row-batch.h"
#include "exec/scratch-tuple-batch.h"

namespace impala {

int HdfsColumnarScanner::ProcessScratchBatch(RowBatch* dst_batch) {
  DCHECK(scratch_batch_ != nullptr);
  ScalarExprEvaluator* const* conjunct_evals = conjunct_evals_->data();
  const int num_conjuncts = conjunct_evals_->size();

  // Start/end/current iterators over the output rows.
  Tuple** output_row_start =
      reinterpret_cast<Tuple**>(dst_batch->GetRow(dst_batch->num_rows()));
  Tuple** output_row_end =
      output_row_start + (dst_batch->capacity() - dst_batch->num_rows());
  Tuple** output_row = output_row_start;

  // Start/end/current iterators over the scratch tuples.
  uint8_t* scratch_tuple_start = scratch_batch_->CurrTuple();
  uint8_t* scratch_tuple_end = scratch_batch_->TupleEnd();
  uint8_t* scratch_tuple = scratch_tuple_start;
  const int tuple_size = scratch_batch_->tuple_byte_size;

  // Loop until the scratch batch is exhausted or the output batch is full.
  // Do not use batch_->AtCapacity() in this loop because it is not necessary
  // to perform the memory capacity check.
  bool* is_selected = scratch_batch_->selected_rows.get() + scratch_batch_->tuple_idx;
  if (filter_ctxs_.empty()) {
    while (scratch_tuple != scratch_tuple_end) {
      *output_row = reinterpret_cast<Tuple*>(scratch_tuple);
      scratch_tuple += tuple_size;
      // Evaluate the conjuncts.
      if (!ExecNode::EvalConjuncts(conjunct_evals, num_conjuncts,
          reinterpret_cast<TupleRow*>(output_row))) {
        *is_selected++ = false;
        continue;
      }
      // Row survived the conjuncts.
      *is_selected++ = true;
      ++output_row;
      if (output_row == output_row_end) break;
    }
  } else {
    DCHECK_GT(tuple_size, 0);
    // Indexes of the tuples of the current batch that passed the runtime filters.
    int sel[RUNTIME_FILTER_BATCH_SIZE];
    uint32_t hashes[RUNTIME_FILTER_BATCH_SIZE];
    while (scratch_tuple != scratch_tuple_end && output_row != output_row_end) {
      // Each tuple produces at most one output row, so a batch that is not larger than
      // the remaining capacity of 'dst_batch' is always processed completely.
      const int num_tuples = std::min<int64_t>({RUNTIME_FILTER_BATCH_SIZE,
          (scratch_tuple_end - scratch_tuple) / tuple_size,
          output_row_end - output_row});
      for (int i = 0; i < num_tuples; ++i) sel[i] = i;
      const int num_passed =
          EvalRuntimeFiltersOnRows(scratch_tuple, tuple_size, sel, num_tuples, hashes);
      memset(is_selected, 0, num_tuples * sizeof(bool));
      // Evaluate the conjuncts on the tuples that passed the runtime filters.
      for (int i = 0; i < num_passed; ++i) {
        const int idx = sel[i];
        *output_row = reinterpret_cast<Tuple*>(scratch_tuple + idx * tuple_size);
        if (!ExecNode::EvalConjuncts(conjunct_evals, num_conjuncts,
            reinterpret_cast<TupleRow*>(output_row))) {
          continue;
        }
        // Row survived runtime filters and conjuncts.
        is_selected[idx] = true;
        ++output_row;
      }
      scratch_tuple += num_tuples * tuple_size;
      is_selected += num_tuples;
    }
  }
  scratch_batch_->tuple_idx += (scratch_tuple - scratch_tuple_start) / tuple_size;
  return output_row - output_row_start;
}

}
