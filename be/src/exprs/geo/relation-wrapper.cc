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

#include "exprs/geo/relation-wrapper.h"

#include "exprs/geo/shape-format.h"
#include "exprs/geo/wkt.h"

namespace impala::geo {


template <class TPredicate>
BooleanVal RelationWrapper::EvalBinBin(
    FunctionContext* ctx, const StringVal& lhs_geom, const StringVal& rhs_geom) {
  RelationWrapper rel;
  if (!ParseHeader(ctx, lhs_geom, &rel.lhsType_) ||
      !ParseHeader(ctx, rhs_geom, &rel.rhsType_)) {
    return BooleanVal::null();
  }

  if (!BBoxIntersects(lhs_geom, rhs_geom, rel.lhsType_, rel.rhsType_)) {
    return BooleanVal(TPredicate::RESULT_IF_BBOX_DOES_NOT_INTERSECT);
  }

  if (!rel.lhs_.FromEsriBinary(ctx, lhs_geom, rel.lhsType_) ||
      !rel.rhs_.FromEsriBinary(ctx, rhs_geom, rel.rhsType_)) {
    return BooleanVal::null();
  }

  return rel.Eval<TPredicate>(ctx);
}

template <class TPredicate>
BooleanVal RelationWrapper::EvalBinWkt(
    FunctionContext* ctx, const StringVal& lhs_geom, const StringVal& rhs_wkt) {
  RelationWrapper rel;
  rel.rhsType_ = GetTypeFromWkt(rhs_wkt);
  if (!ParseHeader(ctx, lhs_geom, &rel.lhsType_)) {
    return BooleanVal::null();
  }

  if (!rel.lhs_.FromEsriBinary(ctx, lhs_geom, rel.lhsType_) ||
      !rel.rhs_.FromWkt(ctx, rhs_wkt, rel.rhsType_)) {
    return BooleanVal::null();
  }
  // TODO: could skip rhs deserialization if bbox doesn't intersect

  return rel.Eval<TPredicate>(ctx);
}

template <class TPredicate>
BooleanVal RelationWrapper::EvalWktBin(
    FunctionContext* ctx, const StringVal& lhs_wkt, const StringVal& rhs_geom) {
  RelationWrapper rel;
  rel.lhsType_ = GetTypeFromWkt(lhs_wkt);
  if (!ParseHeader(ctx, rhs_geom, &rel.rhsType_)) {
    return BooleanVal::null();
  }

  if (!rel.lhs_.FromWkt(ctx, lhs_wkt, rel.lhsType_) ||
      !rel.rhs_.FromEsriBinary(ctx, rhs_geom, rel.rhsType_)) {
    return BooleanVal::null();
  }
  // TODO: could skip lhs deserialization if bbox doesn't intersect

  return rel.Eval<TPredicate>(ctx);
}

template <class TPredicate>
BooleanVal RelationWrapper::EvalWktWkt(
    FunctionContext* ctx, const StringVal& lhs_wkt, const StringVal& rhs_wkt) {
  RelationWrapper rel;
  rel.lhsType_ = GetTypeFromWkt(lhs_wkt);
  rel.rhsType_ = GetTypeFromWkt(rhs_wkt);

  if (!rel.lhs_.FromWkt(ctx, lhs_wkt, rel.lhsType_) ||
      !rel.rhs_.FromWkt(ctx, rhs_wkt, rel.rhsType_)) {
    return BooleanVal::null();
  }

  return rel.Eval<TPredicate>(ctx);
}

// 2 function per predicate to turn 2 OGCType parameters to templated parameters with
// the geometry type.
// TODO: this may become more convulated when predicates are added that to no support
//       all geometry type combinations
template <class lhs_geometry_t, class rhs_geometry_t>
bool ContainsPredicate::Eval(const lhs_geometry_t& lhs, const rhs_geometry_t& rhs) {
  // boost::geometry has no 'contains' function, so its converse, 'within' is used
  // with swapped lhs/rhs.
  return WithinPredicate::Eval(rhs, lhs);
}


template <class lhs_geometry_t, class rhs_geometry_t>
bool CrossesPredicate::Eval(const lhs_geometry_t& lhs, const rhs_geometry_t& rhs) {
  return boost::geometry::crosses(lhs, rhs);
}

template <class lhs_geometry_t, class rhs_geometry_t>
bool DisjointPredicate::Eval(const lhs_geometry_t& lhs, const rhs_geometry_t& rhs) {
  return boost::geometry::disjoint(lhs, rhs);
}

template <class lhs_geometry_t, class rhs_geometry_t>
bool EqualsPredicate::Eval(const lhs_geometry_t& lhs, const rhs_geometry_t& rhs) {
  return boost::geometry::equals(lhs, rhs);
}

template <class lhs_geometry_t, class rhs_geometry_t>
bool IntersectsPredicate::Eval(const lhs_geometry_t& lhs, const rhs_geometry_t& rhs) {
  return boost::geometry::intersects(lhs, rhs);
}

template <class lhs_geometry_t, class rhs_geometry_t>
bool OverlapsPredicate::Eval(const lhs_geometry_t& lhs, const rhs_geometry_t& rhs) {
  return boost::geometry::overlaps(lhs, rhs);
}

template <class lhs_geometry_t, class rhs_geometry_t>
bool TouchesPredicate::Eval(const lhs_geometry_t& lhs, const rhs_geometry_t& rhs) {
  return boost::geometry::touches(lhs, rhs);
}

template <class lhs_geometry_t, class rhs_geometry_t>
bool WithinPredicate::Eval(const lhs_geometry_t& lhs, const rhs_geometry_t& rhs) {
  // Use constexpr if to return false for unsupported type pairs. Generally
  // a shape of higher dimension cannot be "within" a shape of lower dimenson, e.g.
  // a polygon cannot be "within" a line or point. boost::geometry would fail to compile
  // for these unsupported cases.
  if constexpr (boost::geometry::topological_dimension<lhs_geometry_t>()
      <= boost::geometry::topological_dimension<rhs_geometry_t>()) {
    return boost::geometry::within(lhs, rhs);
  } else {
    return false;
  }
}

// Using rtree to store the bounding boxes of subpolygons seems to help with intersecting
// multipolygons:
// select c1.name, count(*) num_neighbours2
// from
//   countries_bin_parquet c1 join countries_bin_parquet c2 join countries_bin_parquet c3
// where c1.name != c2.name and c2.name != c3.name and c3.name != c1.name
//     and st_intersects(c1.geometry, c2.geometry)
//     and st_intersects(c2.geometry, c3.geometry)
//     and not st_intersects(c3.geometry, c1.geometry)
// group by c1.name order by num_neighbours2 desc limit 5;
//
// 5.3s -> 1.5 s
// Not sure why boost doesn't apply this by default. The optimization may fit the
// "countries" benchmark very well as remote islands can make bounding boxes much larger.

// TODO: this is only used for IntersectsPeredicate, others could also profit from it
// TODO: this is only used multipolygons, multiline and multistring could also profit
//       from it

typedef std::pair<box2d, uint32_t> rtree_val_t;
using rtree_t = boost::geometry::index::rtree<rtree_val_t, boost::geometry::index::quadratic<16>>;

void buildRTree(const multi_polygon2d mpoly, rtree_t* rtree) {
  for (int i = 0; i < mpoly.size(); i++) {
    box2d mbr;
    boost::geometry::envelope(mpoly[i], mbr);
    rtree->insert(rtree_val_t(mbr, i));
  }
}

bool treeAssistedIntersect(const multi_polygon2d& mpoly, const polygon2d& poly, const rtree_t& rtree) {
  box2d mbr;
  boost::geometry::envelope(poly, mbr);
  auto it = rtree.qbegin(boost::geometry::index::intersects(mbr));
  for (; it != rtree.qend(); it++) {
    const polygon2d& poly2 = mpoly[it->second];
    if (boost::geometry::intersects(poly, poly2)) return true;
  }
  return false;
}

template <>
bool IntersectsPredicate::Eval(const multi_polygon2d& lhs, const multi_polygon2d& rhs) {
  const multi_polygon2d& smaller = lhs.size() < rhs.size() ? lhs : rhs;
  const multi_polygon2d& bigger = lhs.size() < rhs.size() ? rhs : lhs;
  // Build rtree from smaller multi polygon to reduce log(n) part.
  rtree_t rtree;
  buildRTree(smaller, &rtree);

  for (const polygon2d& poly: bigger) {
    if (treeAssistedIntersect(smaller, poly, rtree)) return true;
  }
  return false;
}

template <>
bool IntersectsPredicate::Eval(const polygon2d& lhs, const multi_polygon2d& rhs) {
  rtree_t rtree;
  buildRTree(rhs, &rtree);
  return treeAssistedIntersect(rhs, lhs, rtree);
}

template <>
bool IntersectsPredicate::Eval(const multi_polygon2d& lhs, const polygon2d& rhs) {
  rtree_t rtree;
  buildRTree(lhs, &rtree);
  return treeAssistedIntersect(lhs, rhs, rtree);
}

template <class TPredicate, class lhs_geometry_t, class rhs_geometry_t>
BooleanVal RelationWrapper::EvalInner2(FunctionContext* ctx, const lhs_geometry_t& lhs, const rhs_geometry_t& rhs) {
  return BooleanVal(TPredicate::Eval(lhs, rhs));
}

template <class TPredicate, class lhs_geometry_t>
BooleanVal RelationWrapper::EvalInner(FunctionContext* ctx, const lhs_geometry_t& lhs) {
  switch (rhsType_) {
    case ST_POINT:
      return EvalInner2<TPredicate>(ctx, lhs, rhs_.point_);
    case ST_LINESTRING:
      return EvalInner2<TPredicate>(ctx, lhs, rhs_.linestring_);
    case ST_POLYGON:
      return EvalInner2<TPredicate>(ctx, lhs, rhs_.polygon_);
    case ST_MULTIPOINT:
      return EvalInner2<TPredicate>(ctx, lhs, rhs_.multi_point_);
    case ST_MULTILINESTRING:
      return EvalInner2<TPredicate>(ctx, lhs, rhs_.multi_linestring_);
    case ST_MULTIPOLYGON:
      return EvalInner2<TPredicate>(ctx, lhs, rhs_.multi_polygon_);
    default:
      ctx->SetError("Geometry type not supported.");
      return BooleanVal::null();
  }
}

template <class TPredicate>
BooleanVal RelationWrapper::Eval(FunctionContext* ctx) {
  try {
    switch (lhsType_) {
      case ST_POINT:
        return EvalInner<TPredicate>(ctx, lhs_.point_);
      case ST_LINESTRING:
        return EvalInner<TPredicate>(ctx, lhs_.linestring_);
      case ST_POLYGON:
        return EvalInner<TPredicate>(ctx, lhs_.polygon_);
      case ST_MULTIPOINT:
        return EvalInner<TPredicate>(ctx, lhs_.multi_point_);
      case ST_MULTILINESTRING:
        return EvalInner<TPredicate>(ctx, lhs_.multi_linestring_);
      case ST_MULTIPOLYGON:
        return EvalInner<TPredicate>(ctx, lhs_.multi_polygon_);
      default:
        ctx->SetError("Geometry type not supported.");
        return BooleanVal::null();
    }
  } catch(boost::geometry::exception& ex) {
    ctx->SetError(ex.what());
    return BooleanVal::null();
  }
}

#define DEFINE_RELATION_PREDICATE(relation_name)                              \
template BooleanVal RelationWrapper::EvalBinBin<relation_name##Predicate>(    \
    FunctionContext*, const StringVal&, const StringVal&);                    \
template BooleanVal RelationWrapper::EvalBinWkt<relation_name##Predicate>(    \
    FunctionContext*, const StringVal&, const StringVal&);                    \
template BooleanVal RelationWrapper::EvalWktBin<relation_name##Predicate>(    \
    FunctionContext*, const StringVal&, const StringVal&);                    \
template BooleanVal RelationWrapper::EvalWktWkt<relation_name##Predicate>(    \
    FunctionContext*, const StringVal&, const StringVal&);

DEFINE_RELATION_PREDICATE(Contains)
DEFINE_RELATION_PREDICATE(Crosses)
DEFINE_RELATION_PREDICATE(Disjoint)
DEFINE_RELATION_PREDICATE(Equals)
DEFINE_RELATION_PREDICATE(Intersects)
DEFINE_RELATION_PREDICATE(Overlaps)
DEFINE_RELATION_PREDICATE(Touches)
DEFINE_RELATION_PREDICATE(Within)

#undef DEFINE_RELATION_PREDICATE

} // namespace impala
