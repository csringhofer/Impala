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
#include "exprs/geo/multi-point-shape-format.h"
#include "exprs/geo/poly-line-shape-format.h"
#include "exprs/geo/utils.h"
#include "common/names.h"
#include "udf/udf.h"

namespace impala::geo {


class RelationWrapper;

// types: https://github.com/Esri/geometry-api-java/blob/d9ed3598b72029c9ebde024e0e616933cff81db2/src/main/java/com/esri/core/geometry/Geometry.java#L49
// serialized formats: https://github.com/Esri/geometry-api-java/blob/master/src/main/java/com/esri/core/geometry/OperatorExportToESRIShapeCursor.java#L404
class GeometryWrapper {

public:

  // (de)serialization functions.
  // These functions clear and overwrite on of thwe geometry members above.

  // Read / Write ESRI's binary format.
  bool FromEsriBinary(FunctionContext* ctx, const StringVal& geom, OGCType ogcType);
  StringVal ToEsriBinary(FunctionContext* ctx, OGCType ogcType);
  StringVal ToEsriBinary(FunctionContext* ctx, OGCType ogcType, int srid);

  // Read / Write WKT.
  // Note that WKT cannot encode SRID.
  bool FromWkt(FunctionContext* ctx, StringVal wkt, OGCType ogcType);
  string ToWkt(FunctionContext* ctx, OGCType ogcType);

  // Create linestring / polygon (without inner rings) / multi point from coordinate list.
  // Number of coordinates must be even.
  bool FromCoordinates(
      FunctionContext* ctx, int num_coords, const DoubleVal* coords, OGCType ogcType);

private:
  friend class RelationWrapper;

  // ST_POINT / ShapeType.ShapePoint
  point2d point_;

  // ST_LINESTRING / ShapeType.ShapePolyline
  linestring2d linestring_;

  // ST_POLYGON / ShapeType.ShapePolyline
  polygon2d polygon_;

  // ST_MULTIPOINT / ShapeType.ShapeMultiPoint
  multipoint2d multi_point_;

  // ST_MULTILINESTRING / ShapeType.ShapeGeneralPolyline
  multi_linestring2d multi_linestring_;

  // ST_MULTIPOLYGON / ShapeType.ShapeGeneralPolyline
  multi_polygon2d multi_polygon_;
};

} // namespace impala
