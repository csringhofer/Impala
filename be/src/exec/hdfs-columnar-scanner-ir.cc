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
  // If any Bloom filter is large enough, the buckets of the next
  // RUNTIME_FILTER_PREFETCH_BATCH_SIZE tuples are prefetched before evaluating the
  // filters on them, so that their cache misses overlap. 'prefetched_end' is the end of
  // the tuples prefetched so far.
  const bool prefetch_filters = UpdateRuntimeFilterPrefetch();
  uint8_t* prefetched_end = scratch_tuple;
  while (scratch_tuple != scratch_tuple_end) {
    if (prefetch_filters && scratch_tuple == prefetched_end) {
      const int64_t num_tuples = std::min<int64_t>(RUNTIME_FILTER_PREFETCH_BATCH_SIZE,
          (scratch_tuple_end - scratch_tuple) / tuple_size);
      prefetched_end = scratch_tuple + num_tuples * tuple_size;
      for (uint8_t* tuple = scratch_tuple; tuple != prefetched_end; tuple += tuple_size) {
        Tuple* row = reinterpret_cast<Tuple*>(tuple);
        PrefetchRuntimeFilters(reinterpret_cast<TupleRow*>(&row));
      }
    }
    *output_row = reinterpret_cast<Tuple*>(scratch_tuple);
    scratch_tuple += tuple_size;
    // Evaluate runtime filters and conjuncts. Short-circuit the evaluation if
    // the filters/conjuncts are empty to avoid function calls.
    if (!EvalRuntimeFilters(reinterpret_cast<TupleRow*>(output_row))) {
      *is_selected++ = false;
      continue;
    }
    if (!ExecNode::EvalConjuncts(conjunct_evals, num_conjuncts,
        reinterpret_cast<TupleRow*>(output_row))) {
      *is_selected++ = false;
      continue;
    }
    // Row survived runtime filters and conjuncts.
    *is_selected++ = true;
    ++output_row;
    if (output_row == output_row_end) break;
  }
  scratch_batch_->tuple_idx += (scratch_tuple - scratch_tuple_start) / tuple_size;
  return output_row - output_row_start;
}

}
