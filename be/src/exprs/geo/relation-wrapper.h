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

#include <cstdint>

#include "exprs/anyval-util.h"
#include "exprs/geo/common.h"
#include "exprs/geo/geometry-wrapper.h"
#include "exprs/geo/utils.h"
#include "common/names.h"
#include "udf/udf.h"

namespace impala::geo {


class RelationWrapper {

public:
  template <class TPredicate>
  static BooleanVal EvalBinBin(
      FunctionContext* ctx, const StringVal& lhs_geom, const StringVal& rhs_geom);

  template <class TPredicate>
  static BooleanVal EvalBinWkt(
      FunctionContext* ctx, const StringVal& lhs_geom, const StringVal& rhs_wkt);

  template <class TPredicate>
  static BooleanVal EvalWktBin(
      FunctionContext* ctx, const StringVal& lhs_wkt, const StringVal& rhs_geom);

  template <class TPredicate>
  static BooleanVal EvalWktWkt(
      FunctionContext* ctx, const StringVal& lhs_wkt, const StringVal& rhs_wkt);

private:
  template <class TPredicate>
  BooleanVal Eval(FunctionContext* ctx);

  template <class TPredicate, class lhs_geometry_t>
  BooleanVal EvalInner(FunctionContext* ctx, const lhs_geometry_t& lhs);

  template <class TPredicate, class lhs_geometry_t, class rhs_geometry_t>
  BooleanVal EvalInner2(FunctionContext* ctx, const lhs_geometry_t& lhs, const rhs_geometry_t& rhs);

  GeometryWrapper lhs_, rhs_;
  OGCType lhsType_, rhsType_;
};

// TODO: this may not be the ideal way to handle predicates
//       while most predicates behave similarly, there are 3 special cases:
//  - st_disjoint can't use bbox check (or rtree) for early filtering
//  - st_within and st_contains are assymetric and not supported for all type pairs
//    by boost
// All predicates are implemented by boost, with the exception of st_contains which
// uses st_within with swapped arguments as a workaround.
struct RelationPredicate {
  static constexpr bool RESULT_IF_BBOX_DOES_NOT_INTERSECT = false;
};

struct ContainsPredicate: public RelationPredicate {
  template <class lhs_geometry_t, class rhs_geometry_t>
  static bool Eval(const lhs_geometry_t& lhs, const rhs_geometry_t& rhs);
};

struct CrossesPredicate: public RelationPredicate {
  template <class lhs_geometry_t, class rhs_geometry_t>
  static bool Eval(const lhs_geometry_t& lhs, const rhs_geometry_t& rhs);
};

struct DisjointPredicate: public RelationPredicate {
  static constexpr bool RESULT_IF_BBOX_DOES_NOT_INTERSECT = true;

  template <class lhs_geometry_t, class rhs_geometry_t>
  static bool Eval(const lhs_geometry_t& lhs, const rhs_geometry_t& rhs);
};

struct EqualsPredicate: public RelationPredicate {
  template <class lhs_geometry_t, class rhs_geometry_t>
  static bool Eval(const lhs_geometry_t& lhs, const rhs_geometry_t& rhs);
};

struct IntersectsPredicate: public RelationPredicate  {
  template <class lhs_geometry_t, class rhs_geometry_t>
  static bool Eval(const lhs_geometry_t& lhs, const rhs_geometry_t& rhs);
};

struct OverlapsPredicate: public RelationPredicate  {
  template <class lhs_geometry_t, class rhs_geometry_t>
  static bool Eval(const lhs_geometry_t& lhs, const rhs_geometry_t& rhs);
};

struct WithinPredicate: public RelationPredicate  {
  template <class lhs_geometry_t, class rhs_geometry_t>
  static bool Eval(const lhs_geometry_t& lhs, const rhs_geometry_t& rhs);
};

struct TouchesPredicate: public RelationPredicate  {
  template <class lhs_geometry_t, class rhs_geometry_t>
  static bool Eval(const lhs_geometry_t& lhs, const rhs_geometry_t& rhs);
};

} // namespace impala
