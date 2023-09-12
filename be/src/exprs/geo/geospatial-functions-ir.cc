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

#include "exprs/geo/geospatial-functions.h"

#include <cmath>
#include <limits>

#include <boost/algorithm/string/replace.hpp>
#include <boost/geometry/geometry.hpp>

#include "exprs/geo/common.h"
#include "exprs/geo/shape-format.h"
#include "exprs/geo/poly-line-shape-format.h"
#include "exprs/geo/multi-point-shape-format.h"
#include "exprs/geo/geometry-wrapper.h"
#include "exprs/geo/relation-wrapper.h"
#include "exprs/geo/utils.h"
#include "exprs/geo/wkt.h"
#include "runtime/string-value.inline.h"
#include "udf/udf-internal.h"
#include "udf/udf.h"

#include "common/names.h"

namespace impala::geo {

IntVal GeospatialFunctions::st_Srid(FunctionContext* ctx, const StringVal& geom) {
  OGCType ogc_type;
  if (!ParseHeader(ctx, geom, &ogc_type)) return IntVal::null();
  return getSrid(geom);
}

DoubleVal GeospatialFunctions::st_X(FunctionContext* ctx, const StringVal& geom) {
  OGCType ogc_type;
  if (!ParseHeader(ctx, geom, &ogc_type)) return DoubleVal::null();
  if (ogc_type != ST_POINT) return DoubleVal::null(); // Only valid for ST_POINT.
  return DoubleVal(getMinX(geom));
}

DoubleVal GeospatialFunctions::st_Y(FunctionContext* ctx, const StringVal& geom) {
  OGCType ogc_type;
  if (!ParseHeader(ctx, geom, &ogc_type)) return DoubleVal::null();
  if (ogc_type != ST_POINT) return DoubleVal::null();  // Only valid for ST_POINT.
  return DoubleVal(getMinY(geom));
}

DoubleVal GeospatialFunctions::st_MinX(FunctionContext* ctx, const StringVal& geom) {
  OGCType ogc_type;
  if (!ParseHeader(ctx, geom, &ogc_type)) return DoubleVal::null();
  return DoubleVal(getMinX(geom));
}

DoubleVal GeospatialFunctions::st_MinY(FunctionContext* ctx, const StringVal& geom) {
  OGCType ogc_type;
  if (!ParseHeader(ctx, geom, &ogc_type)) return DoubleVal::null();
  return DoubleVal(getMinY(geom));
}

DoubleVal GeospatialFunctions::st_MaxX(FunctionContext* ctx, const StringVal& geom) {
  OGCType ogc_type;
  if (!ParseHeader(ctx, geom, &ogc_type)) return DoubleVal::null();
  if (ogc_type == ST_POINT) return DoubleVal(getMinX(geom));
  return DoubleVal(getMaxX(geom));
}

DoubleVal GeospatialFunctions::st_MaxY(FunctionContext* ctx, const StringVal& geom) {
  OGCType ogc_type;
  if (!ParseHeader(ctx, geom, &ogc_type)) return DoubleVal::null();
  if (ogc_type == ST_POINT) return DoubleVal(getMinY(geom));
  return DoubleVal(getMaxY(geom));
}

StringVal GeospatialFunctions::st_GeometryType(FunctionContext* ctx,
    const StringVal& geom) {
  OGCType ogc_type;
  if (!ParseHeader(ctx, geom, &ogc_type)) return StringVal::null();
  const char* name = getGeometryType(ogc_type);

  return StringVal(name);
}

StringVal GeospatialFunctions::st_SetSrid(FunctionContext* ctx, const StringVal& geom,
    const IntVal& srid) {
  if (srid.is_null) return geom;
  OGCType ogc_type;
  if (!ParseHeader(ctx, geom, &ogc_type)) return StringVal::null();

  StringVal res = StringVal::CopyFrom(ctx, geom.ptr, geom.len);
  setSrid(res, srid.val);
  return res;
}

StringVal GeospatialFunctions::st_Point(FunctionContext* ctx, const DoubleVal& x,
    const DoubleVal& y) {
  if (x.is_null) return StringVal::null();
  if (y.is_null) return StringVal::null();
  return createStPoint(ctx, x.val, y.val, 0);
}

StringVal GeospatialFunctions::st_Point(FunctionContext* ctx, const StringVal& wkt) {
  point2d p;
  if (wktToPoint(ctx, wkt, p)) {
    return createStPoint(ctx, p.x(), p.y(), 0);
  } else {
    return StringVal::null();
  }
}

StringVal GeospatialFunctions::st_LineString(FunctionContext* ctx, const StringVal& wkt) {
  GeometryWrapper wrapper;
  if (!wrapper.FromWkt(ctx, wkt, ST_LINESTRING)) return StringVal::null();;
  return wrapper.ToEsriBinary(ctx, ST_LINESTRING);
}

// Variable argument list constructor.
StringVal GeospatialFunctions::st_LineString(FunctionContext* ctx, int num_coords, const DoubleVal* coords) {
  GeometryWrapper wrapper;
  if (!wrapper.FromCoordinates(ctx, num_coords, coords, ST_LINESTRING)) return StringVal::null();
  return wrapper.ToEsriBinary(ctx, ST_LINESTRING);
}

StringVal GeospatialFunctions::st_MultiLineString(FunctionContext* ctx, const StringVal& wkt) {
  GeometryWrapper wrapper;
  if (!wrapper.FromWkt(ctx, wkt, ST_MULTILINESTRING)) return StringVal::null();
  return wrapper.ToEsriBinary(ctx, ST_MULTILINESTRING);
}

StringVal GeospatialFunctions::st_Polygon(FunctionContext* ctx, const StringVal& wkt) {
  GeometryWrapper wrapper;
  if (!wrapper.FromWkt(ctx, wkt, ST_POLYGON)) return StringVal::null();
  return wrapper.ToEsriBinary(ctx, ST_POLYGON);
}

// Variable argument list constructor.
StringVal GeospatialFunctions::st_Polygon(FunctionContext* ctx, int num_coords, const DoubleVal* coords) {
  GeometryWrapper wrapper;
  if (!wrapper.FromCoordinates(ctx, num_coords, coords, ST_POLYGON)) return StringVal::null();
  return wrapper.ToEsriBinary(ctx, ST_POLYGON);
}

StringVal GeospatialFunctions::st_MultiPolygon(FunctionContext* ctx, const StringVal& wkt) {
  GeometryWrapper wrapper;
  if (!wrapper.FromWkt(ctx, wkt, ST_MULTIPOLYGON)) return StringVal::null();
  return wrapper.ToEsriBinary(ctx, ST_MULTIPOLYGON);
}


StringVal GeospatialFunctions::st_MultiPoint(FunctionContext* ctx, const StringVal& wkt) {
  GeometryWrapper wrapper;
  if (!wrapper.FromWkt(ctx, wkt, ST_MULTIPOINT)) return StringVal::null();
  return wrapper.ToEsriBinary(ctx, ST_MULTIPOINT);
}

// Variable argument list constructor.
StringVal GeospatialFunctions::st_MultiPoint(FunctionContext* ctx, int num_coords, const DoubleVal* coords) {
  GeometryWrapper wrapper;
  if (!wrapper.FromCoordinates(ctx, num_coords, coords, ST_MULTIPOINT)) return StringVal::null();
  return wrapper.ToEsriBinary(ctx, ST_MULTIPOINT);
}

// Predicates
BooleanVal GeospatialFunctions::st_EnvIntersects(
    FunctionContext* ctx, const StringVal& lhs_geom,const StringVal& rhs_geom) {
  OGCType lhs_type, rhs_type;
  // TODO: compare srid? The ESRI UDF does it, but it is not done in other relations:
  //   https://github.com/apache/hive/blob/9eeab40173479c74b6fbf6657c3472b81ce4efcd/ql/src/java/org/apache/hadoop/hive/ql/udf/esri/ST_EnvIntersects.java#L63
  if (!ParseHeader(ctx, lhs_geom, &lhs_type) || !ParseHeader(ctx, rhs_geom, &rhs_type)) {
    return BooleanVal::null();
  }
  bool result = BBoxIntersects(lhs_geom, rhs_geom, lhs_type, rhs_type);
  return BooleanVal(result);
}

#define DEFINE_RELATION(relation_name)                                           \
BooleanVal GeospatialFunctions::st_##relation_name##_Binary_Binary(              \
    FunctionContext* ctx, const StringVal& lhs, const StringVal& rhs) {          \
  return RelationWrapper::EvalBinBin<relation_name##Predicate>(ctx, lhs, rhs);   \
}                                                                                \
BooleanVal GeospatialFunctions::st_##relation_name##_Wkt_Binary(                 \
    FunctionContext* ctx, const StringVal& lhs, const StringVal& rhs) {          \
  return RelationWrapper::EvalWktBin<relation_name##Predicate>(ctx, lhs, rhs);   \
}                                                                                \
BooleanVal GeospatialFunctions::st_##relation_name##_Binary_Wkt(                 \
    FunctionContext* ctx, const StringVal& lhs, const StringVal& rhs) {          \
  return RelationWrapper::EvalBinWkt<relation_name##Predicate>(ctx, lhs, rhs);   \
}                                                                                \
BooleanVal GeospatialFunctions::st_##relation_name##_Wkt_Wkt(                    \
    FunctionContext* ctx, const StringVal& lhs, const StringVal& rhs) {          \
  return RelationWrapper::EvalWktWkt<relation_name##Predicate>(ctx, lhs, rhs);   \
}

DEFINE_RELATION(Contains)
DEFINE_RELATION(Crosses)
DEFINE_RELATION(Disjoint)
DEFINE_RELATION(Equals)
DEFINE_RELATION(Intersects)
DEFINE_RELATION(Overlaps)
DEFINE_RELATION(Touches)
DEFINE_RELATION(Within)

#undef DEFINE_RELATION

/*
// Intersects overloads.
BooleanVal GeospatialFunctions::st_Intersects_Binary_Binary(
    FunctionContext* ctx, const StringVal& lhs_geom, const StringVal& rhs_geom) {
  return RelationWrapper::EvalBinBin<IntersectsPredicate>(ctx, lhs_geom, rhs_geom);
}

BooleanVal GeospatialFunctions::st_Intersects_Wkt_Binary(
    FunctionContext* ctx, const StringVal& lhs_wkt, const StringVal& rhs_geom) {
  return RelationWrapper::EvalWktBin<IntersectsPredicate>(ctx, lhs_wkt, rhs_geom);
}

BooleanVal GeospatialFunctions::st_Intersects_Binary_Wkt(
    FunctionContext* ctx, const StringVal& lhs_geom, const StringVal& rhs_wkt) {
  return RelationWrapper::EvalBinWkt<IntersectsPredicate>(ctx, lhs_geom, rhs_wkt);
}

BooleanVal GeospatialFunctions::st_Intersects_Wkt_Wkt(
    FunctionContext* ctx, const StringVal& lhs_wkt, const StringVal& rhs_wkt) {
  return RelationWrapper::EvalWktWkt<IntersectsPredicate>(ctx, lhs_wkt, rhs_wkt);
}

// Overlaps overloads.
BooleanVal GeospatialFunctions::st_Overlaps_Binary_Binary(
    FunctionContext* ctx, const StringVal& lhs_geom, const StringVal& rhs_geom) {
  return RelationWrapper::EvalBinBin<OverlapsPredicate>(ctx, lhs_geom, rhs_geom);
}

BooleanVal GeospatialFunctions::st_Overlaps_Wkt_Binary(
    FunctionContext* ctx, const StringVal& lhs_wkt, const StringVal& rhs_geom) {
  return RelationWrapper::EvalWktBin<OverlapsPredicate>(ctx, lhs_wkt, rhs_geom);
}

BooleanVal GeospatialFunctions::st_Overlaps_Binary_Wkt(
    FunctionContext* ctx, const StringVal& lhs_geom, const StringVal& rhs_wkt) {
  return RelationWrapper::EvalBinWkt<OverlapsPredicate>(ctx, lhs_geom, rhs_wkt);
}

BooleanVal GeospatialFunctions::st_Overlaps_Wkt_Wkt(
    FunctionContext* ctx, const StringVal& lhs_wkt, const StringVal& rhs_wkt) {
  return RelationWrapper::EvalWktWkt<OverlapsPredicate>(ctx, lhs_wkt, rhs_wkt);
}

// Touches overloads.
BooleanVal GeospatialFunctions::st_Touches_Binary_Binary(
    FunctionContext* ctx, const StringVal& lhs_geom, const StringVal& rhs_geom) {
  return RelationWrapper::EvalBinBin<TouchesPredicate>(ctx, lhs_geom, rhs_geom);
}

BooleanVal GeospatialFunctions::st_Touches_Wkt_Binary(
    FunctionContext* ctx, const StringVal& lhs_wkt, const StringVal& rhs_geom) {
  return RelationWrapper::EvalWktBin<TouchesPredicate>(ctx, lhs_wkt, rhs_geom);
}

BooleanVal GeospatialFunctions::st_Touches_Binary_Wkt(
    FunctionContext* ctx, const StringVal& lhs_geom, const StringVal& rhs_wkt) {
  return RelationWrapper::EvalBinWkt<TouchesPredicate>(ctx, lhs_geom, rhs_wkt);
}

BooleanVal GeospatialFunctions::st_Touches_Wkt_Wkt(
    FunctionContext* ctx, const StringVal& lhs_wkt, const StringVal& rhs_wkt) {
  return RelationWrapper::EvalWktWkt<TouchesPredicate>(ctx, lhs_wkt, rhs_wkt);
}

// Crosses overloads.
BooleanVal GeospatialFunctions::st_Crosses_Binary_Binary(
    FunctionContext* ctx, const StringVal& lhs_geom, const StringVal& rhs_geom) {
  return RelationWrapper::EvalBinBin<CrossesPredicate>(ctx, lhs_geom, rhs_geom);
}

BooleanVal GeospatialFunctions::st_Crosses_Wkt_Binary(
    FunctionContext* ctx, const StringVal& lhs_wkt, const StringVal& rhs_geom) {
  return RelationWrapper::EvalWktBin<CrossesPredicate>(ctx, lhs_wkt, rhs_geom);
}

BooleanVal GeospatialFunctions::st_Crosses_Binary_Wkt(
    FunctionContext* ctx, const StringVal& lhs_geom, const StringVal& rhs_wkt) {
  return RelationWrapper::EvalBinWkt<CrossesPredicate>(ctx, lhs_geom, rhs_wkt);
}

BooleanVal GeospatialFunctions::st_Crosses_Wkt_Wkt(
    FunctionContext* ctx, const StringVal& lhs_wkt, const StringVal& rhs_wkt) {
  return RelationWrapper::EvalWktWkt<CrossesPredicate>(ctx, lhs_wkt, rhs_wkt);
}
*/
// Transformations
StringVal GeospatialFunctions::st_Envelope(FunctionContext* ctx, const StringVal& geom) {
  OGCType ogc_type;
  if (!ParseHeader(ctx, geom, &ogc_type)) return StringVal::null();

  box2d envelope = getBBox(geom, ogc_type);
  return PolyLineShapeFormat::Write(ctx, envelope);
}

StringVal GeospatialFunctions::st_AsText(FunctionContext* ctx, const StringVal& geom) {
  OGCType ogc_type;
  if (!ParseHeader(ctx, geom, &ogc_type)) return StringVal::null();

  GeometryWrapper wrapper;
  wrapper.FromEsriBinary(ctx, geom, ogc_type);

  string wkt = wrapper.ToWkt(ctx, ogc_type);
  if (wkt.empty()) return StringVal::null();

  return AnyValUtil::FromString(ctx, wkt);
}

StringVal GeospatialFunctions::st_GeomFromText(FunctionContext* ctx, const StringVal& wkt) {
  OGCType ogc_type = GetTypeFromWkt(wkt);

  GeometryWrapper wrapper;
  if (!wrapper.FromWkt(ctx, wkt, ogc_type)) return StringVal::null();

  return wrapper.ToEsriBinary(ctx, ogc_type);
}

StringVal GeospatialFunctions::st_GeomFromText(
    FunctionContext* ctx, const StringVal& wkt, const IntVal& srid) {
  if (srid.is_null) return StringVal::null();
  OGCType ogc_type = GetTypeFromWkt(wkt);

  GeometryWrapper wrapper;
  if (!wrapper.FromWkt(ctx, wkt, ogc_type)) return StringVal::null();

  return wrapper.ToEsriBinary(ctx, ogc_type, srid.val);
}


BigIntVal GeospatialFunctions::st_BinGeom(FunctionContext* ctx, const BigIntVal& bin_size,
    const StringVal& geom) {
  if (bin_size.is_null) return BigIntVal::null();
  OGCType ogc_type;
  if (!ParseHeader(ctx, geom, &ogc_type)) return BigIntVal::null();

  const double x = getMinX(geom);
  const double y = getMinY(geom);
  return getBinId(bin_size.val, x, y);
}

BigIntVal GeospatialFunctions::st_BinGeom(FunctionContext* ctx, const DoubleVal& bin_size,
    const StringVal& geom) {
  if (bin_size.is_null) return BigIntVal::null();
  OGCType ogc_type;
  if (!ParseHeader(ctx, geom, &ogc_type)) return BigIntVal::null();

  const double x = getMinX(geom);
  const double y = getMinY(geom);
  return getBinId(bin_size.val, x, y);
}

BigIntVal GeospatialFunctions::st_BinWkt(FunctionContext* ctx, const BigIntVal& bin_size,
    const StringVal& wkt) {
  if (bin_size.is_null || wkt.is_null) return BigIntVal::null();

  point2d p;
  if (!wktToPoint(ctx, wkt, p)) return BigIntVal::null();
  return getBinId(bin_size.val, p.x(), p.y());
}

BigIntVal GeospatialFunctions::st_BinWkt(FunctionContext* ctx, const DoubleVal& bin_size,
    const StringVal& wkt) {
  if (bin_size.is_null || wkt.is_null) return BigIntVal::null();

  point2d p;
  if (!wktToPoint(ctx, wkt, p)) return BigIntVal::null();
  return getBinId(bin_size.val, p.x(), p.y());
}

StringVal GeospatialFunctions::st_BinenvelopeBinId(FunctionContext* ctx, const BigIntVal& bin_size,
    const BigIntVal& bin_id) {
  if (bin_size.is_null || bin_id.is_null) return StringVal::null();
  box2d envelope = getBinEnvelope(bin_size.val, bin_id.val);
  return PolyLineShapeFormat::Write(ctx, envelope);
}

StringVal GeospatialFunctions::st_BinenvelopeBinId(FunctionContext* ctx, const DoubleVal& bin_size,
    const BigIntVal& bin_id) {
  if (bin_size.is_null || bin_id.is_null) return StringVal::null();
  box2d envelope = getBinEnvelope(bin_size.val, bin_id.val);
  return PolyLineShapeFormat::Write(ctx, envelope);
}

StringVal GeospatialFunctions::st_BinenvelopeGeom(FunctionContext* ctx, const BigIntVal& bin_size,
    const StringVal& geom) {
  if (bin_size.is_null) return StringVal::null();
  OGCType ogc_type;
  if (!ParseHeader(ctx, geom, &ogc_type)) return StringVal::null();
  if (ogc_type != ST_POINT) return StringVal::null();

  const double x = getMinX(geom);
  const double y = getMinY(geom);

  box2d envelope = getBinEnvelope(bin_size.val, x, y);
  return PolyLineShapeFormat::Write(ctx, envelope);
}

StringVal GeospatialFunctions::st_BinenvelopeGeom(FunctionContext* ctx, const DoubleVal& bin_size,
    const StringVal& geom) {
  if (bin_size.is_null) return StringVal::null();
  OGCType ogc_type;
  if (!ParseHeader(ctx, geom, &ogc_type)) return StringVal::null();
  if (ogc_type != ST_POINT) return StringVal::null();

  const double x = getMinX(geom);
  const double y = getMinY(geom);

  box2d envelope = getBinEnvelope(bin_size.val, x, y);
  return PolyLineShapeFormat::Write(ctx, envelope);
}

StringVal GeospatialFunctions::st_BinenvelopeWkt(FunctionContext* ctx, const BigIntVal& bin_size,
    const StringVal& wkt) {
  if (bin_size.is_null || wkt.is_null) return StringVal::null();
  if (GetTypeFromWkt(wkt) != ST_POINT) return StringVal::null();
  point2d point;
  wktToPoint(ctx, wkt, point);

  box2d envelope = getBinEnvelope(bin_size.val, point.x(), point.y());
  return PolyLineShapeFormat::Write(ctx, envelope);
}

StringVal GeospatialFunctions::st_BinenvelopeWkt(FunctionContext* ctx, const DoubleVal& bin_size,
    const StringVal& wkt) {
  if (bin_size.is_null || wkt.is_null) return StringVal::null();
  if (GetTypeFromWkt(wkt) != ST_POINT) return StringVal::null();
  point2d point;
  wktToPoint(ctx, wkt, point);

  box2d envelope = getBinEnvelope(bin_size.val, point.x(), point.y());
  return PolyLineShapeFormat::Write(ctx, envelope);
}

}
