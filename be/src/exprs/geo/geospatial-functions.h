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

#include <map>

#include "common/status.h"
#include "udf/udf.h"

namespace impala::geo {

using impala_udf::FunctionContext;
using impala_udf::BooleanVal;
using impala_udf::IntVal;
using impala_udf::BigIntVal;
using impala_udf::DoubleVal;
using impala_udf::StringVal;

class Expr;
class OpcodeRegistry;
struct StringValue;
class TupleRow;

class GeospatialFunctions {
 public:
  // Accessors
  static IntVal st_Srid(FunctionContext* ctx, const StringVal& geom);
  static DoubleVal st_X(FunctionContext* ctx, const StringVal& geom);
  static DoubleVal st_Y(FunctionContext* ctx, const StringVal& geom);
  static DoubleVal st_MinX(FunctionContext* ctx, const StringVal& geom);
  static DoubleVal st_MinY(FunctionContext* ctx, const StringVal& geom);
  static DoubleVal st_MaxX(FunctionContext* ctx, const StringVal& geom);
  static DoubleVal st_MaxY(FunctionContext* ctx, const StringVal& geom);
  static StringVal st_GeometryType(FunctionContext* ctx, const StringVal& geom);

  static StringVal st_SetSrid(FunctionContext* ctx, const StringVal& geom,
      const IntVal& srid);

  // Constructors
  static StringVal st_Point(FunctionContext* ctx, const DoubleVal& x, const DoubleVal& y);
  static StringVal st_Point(FunctionContext* ctx, const StringVal& wkt);
  static StringVal st_LineString(FunctionContext* ctx, const StringVal& wkt);
  static StringVal st_MultiPoint(FunctionContext* ctx, const StringVal& wkt);
  static StringVal st_MultiLineString(FunctionContext* ctx, const StringVal& wkt);
  static StringVal st_Polygon(FunctionContext* ctx, const StringVal& wkt);
  static StringVal st_MultiPolygon(FunctionContext* ctx, const StringVal& wkt);

  // Vararg constuctors
  static StringVal st_LineString(FunctionContext* ctx, int num_coords,
      const DoubleVal* points);
  static StringVal st_MultiPoint(FunctionContext* ctx, int num_coords,
      const DoubleVal* points);
  static StringVal st_Polygon(FunctionContext* ctx, int num_coords,
      const DoubleVal* points);

  // Predicates
  static BooleanVal st_EnvIntersects(
      FunctionContext* ctx, const StringVal& lhs,const StringVal& rhs);

// Use a macro to define the 4 overloads for each relation.
#define DECLARE_RELATION(relation_name)                                 \
  static BooleanVal st_##relation_name##_Binary_Binary(                 \
      FunctionContext* ctx, const StringVal& lhs,const StringVal& rhs); \
  static BooleanVal st_##relation_name##_Wkt_Binary(                    \
      FunctionContext* ctx, const StringVal& lhs,const StringVal& rhs); \
  static BooleanVal st_##relation_name##_Binary_Wkt(                    \
      FunctionContext* ctx, const StringVal& lhs,const StringVal& rhs); \
  static BooleanVal st_##relation_name##_Wkt_Wkt(                       \
      FunctionContext* ctx, const StringVal& lhs,const StringVal& rhs);

  DECLARE_RELATION(Contains)
  DECLARE_RELATION(Crosses)
  DECLARE_RELATION(Disjoint)
  DECLARE_RELATION(Equals)
  DECLARE_RELATION(Intersects)
  DECLARE_RELATION(Overlaps)
  DECLARE_RELATION(Touches)
  DECLARE_RELATION(Within)

#undef DECLARE_RELATION

  // Transformations
  static StringVal st_Envelope(FunctionContext* ctx, const StringVal& geom);

  // Conversion functions
  static StringVal st_AsText(FunctionContext* ctx, const StringVal& wkt);
  static StringVal st_GeomFromText(FunctionContext* ctx, const StringVal& wkt);
  static StringVal st_GeomFromText(
      FunctionContext* ctx, const StringVal& wkt, const IntVal& srid);

  // Binning functions
  //   Used to split the space into cells identified with a BIGINT.
  static BigIntVal st_BinGeom(FunctionContext* ctx, const BigIntVal& bin_size,
      const StringVal& geom);
  static BigIntVal st_BinGeom(FunctionContext* ctx, const DoubleVal& bin_size,
      const StringVal& geom);
  static BigIntVal st_BinWkt(FunctionContext* ctx, const BigIntVal& bin_size,
      const StringVal& geom);
  static BigIntVal st_BinWkt(FunctionContext* ctx, const DoubleVal& bin_size,
      const StringVal& geom);

  static StringVal st_BinenvelopeBinId(FunctionContext* ctx, const BigIntVal& bin_size,
      const BigIntVal& bin_id);
  static StringVal st_BinenvelopeBinId(FunctionContext* ctx, const DoubleVal& bin_size,
      const BigIntVal& bin_id);
  static StringVal st_BinenvelopeGeom(FunctionContext* ctx, const BigIntVal& bin_size,
      const StringVal& geom);
  static StringVal st_BinenvelopeGeom(FunctionContext* ctx, const DoubleVal& bin_size,
      const StringVal& geom);
  static StringVal st_BinenvelopeWkt(FunctionContext* ctx, const BigIntVal& bin_size,
      const StringVal& wkr);
  static StringVal st_BinenvelopeWkt(FunctionContext* ctx, const DoubleVal& bin_size,
      const StringVal& wkt);


};

}// namespace impala
