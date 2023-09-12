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

#include "exprs/geo/common.h"

namespace impala::geo {

constexpr int ESRI_TYPE_SIZE = 4;
constexpr int ESRI_TYPE_OFFSET = 5;

constexpr int X1_OFFSET = 9;
constexpr int Y1_OFFSET = X1_OFFSET + sizeof(double);
constexpr int X2_OFFSET = Y1_OFFSET + sizeof(double);
constexpr int Y2_OFFSET = X2_OFFSET + sizeof(double);

constexpr int MIN_GEOM_SIZE = 9;
constexpr int MIN_POINT_SIZE = 25;
constexpr int MIN_NON_POINT_SIZE = 41;

static_assert(ESRI_TYPE_OFFSET == OGC_TYPE_OFFSET + OGC_TYPE_SIZE);
static_assert(X1_OFFSET == ESRI_TYPE_OFFSET + ESRI_TYPE_SIZE);
static_assert(MIN_GEOM_SIZE == SRID_SIZE + OGC_TYPE_SIZE + ESRI_TYPE_SIZE);
static_assert(MIN_POINT_SIZE ==  MIN_GEOM_SIZE + 2 * sizeof(double));
static_assert(MIN_NON_POINT_SIZE ==  MIN_POINT_SIZE + 2 * sizeof(double));


// See https://github.com/Esri/geometry-api-java/blob/d9ed3598b72029c9ebde024e0e616933cff81db2/src/main/java/com/esri/core/geometry/ShapeType.java#L27
enum EsriType: uint32_t {
  ShapeNull = 0,
  ShapePoint = 1,
  ShapePointM = 21,
  ShapePointZM = 11,
  ShapePointZ = 9,
  ShapeMultiPoint = 8,
  ShapeMultiPointM = 28,
  ShapeMultiPointZM = 18,
  ShapeMultiPointZ = 20,
  ShapePolyline = 3,
  ShapePolylineM = 23,
  ShapePolylineZM = 13,
  ShapePolylineZ = 10,
  ShapePolygon = 5,
  ShapePolygonM = 25,
  ShapePolygonZM = 15,
  ShapePolygonZ = 19,
  ShapeMultiPatchM = 31,
  ShapeMultiPatch = 32,
  ShapeGeneralPolyline = 50,
  ShapeGeneralPolygon = 51,
  ShapeGeneralPoint = 52,
  ShapeGeneralMultiPoint = 53,
  ShapeGeneralMultiPatch = 54,
  ShapeTypeLast = 55
};

constexpr std::array<EsriType, ST_MULTIPOLYGON + 1> OGCTypeToEsriType = {{
  ShapeNull,       // UNKNOWN
  ShapePoint,      // ST_POINT
  ShapePolyline,   // ST_LINESTRING
  ShapePolygon,    // ST_POLYGON
  ShapeMultiPoint, // ST_MULTIPOINT
  ShapePolyline,   // ST_MULTILINESTRING
  ShapePolygon     // ST_MULTIPOLYGON
}};

inline EsriType getEsriType(const StringVal& geom) {
  static_assert(ESRI_TYPE_SIZE == sizeof(EsriType));
  return readFromGeom<EsriType>(geom, ESRI_TYPE_OFFSET);
}

inline double getMinX(const StringVal& geom) {
  return readFromGeom<double>(geom, X1_OFFSET);
}

inline double getMinY(const StringVal& geom) {
  return readFromGeom<double>(geom, Y1_OFFSET);
}

inline double getMaxX(const StringVal& geom) {
  return readFromGeom<double>(geom, X2_OFFSET);
}

inline double getMaxY(const StringVal& geom) {
  return readFromGeom<double>(geom, Y2_OFFSET);
}

inline void setEsriType(StringVal& geom, EsriType esri_type) {
  static_assert(ESRI_TYPE_SIZE == sizeof(EsriType));
  writeToGeom<EsriType>(esri_type, geom, ESRI_TYPE_OFFSET);
}

inline void setMinX(StringVal& geom, double x) {
  writeToGeom<double>(x, geom, X1_OFFSET);
}

inline void setMinY(StringVal& geom, double y) {
  writeToGeom<double>(y, geom, Y1_OFFSET);
}

inline void setMaxX(StringVal& geom, double x) {
  writeToGeom<double>(x, geom, X2_OFFSET);
}

inline void setMaxY(StringVal& geom, double y) {
  writeToGeom<double>(y, geom, Y2_OFFSET);
}

// TODO: We could separate the reading of the outer (OGC) and the inner header (Esri).
inline bool ParseHeader(FunctionContext* ctx, const StringVal& geom, OGCType* ogc_type) {
  DCHECK(ogc_type != nullptr);

  if (geom.is_null) return false;

  if (geom.len < MIN_GEOM_SIZE) {
    ctx->SetError("Geometry size too small.");
    return false;
  }

  const OGCType unchecked_ogc_type = getOGCType(geom);
  if (unchecked_ogc_type < UNKNOWN || unchecked_ogc_type > ST_MULTIPOLYGON) {
    ctx->SetError("Invalid geometry type.");
    return false;
  }

  if (unchecked_ogc_type == UNKNOWN) {
    ctx->SetError("Geometry type UNKNOWN.");
    return false;
  }

  if (unchecked_ogc_type == ST_POINT) {
    if (geom.len < MIN_POINT_SIZE) {
      ctx->SetError("Geometry size too small for ST_POINT type.");
      return false;
    }
  } else {
    if (geom.len < MIN_NON_POINT_SIZE) {
      ctx->SetError("Geometry size too small for non ST_POINT type.");
      return false;
    }
  }

  const EsriType esri_type = getEsriType(geom);
  DCHECK_LT(unchecked_ogc_type, OGCTypeToEsriType.size());
  const EsriType expected_esri_type = OGCTypeToEsriType[unchecked_ogc_type];
  if (expected_esri_type != esri_type) {
    // TODO: To test it we need to create a table with 3D types, we cannot create them
    // with native constructors.
    ctx->SetError(strings::Substitute(
          "Invalid geometry: OGCType and EsriType do not match. "
          "Because the OGCType is $0, expected EsriType $1, found $2.",
          OGCTypeToStr[unchecked_ogc_type], expected_esri_type, esri_type).c_str());
  }

  *ogc_type = static_cast<OGCType>(unchecked_ogc_type);
  return true;
}

// TODO: Separate?
inline void writeHeader(StringVal& res, OGCType ogc_type, const box2d& bounding_rect, uint32_t srid = 0) {
  setSrid(res, srid);
  setOGCType(res, ogc_type);
  setEsriType(res, OGCTypeToEsriType[ogc_type]);
  setMinX(res, bounding_rect.min_corner().x());
  setMinY(res, bounding_rect.min_corner().y());
  setMaxX(res, bounding_rect.max_corner().x());
  setMaxY(res, bounding_rect.max_corner().y());
}

inline StringVal createStPoint(FunctionContext* ctx, double x, double y,
    uint32_t srid = 0) {
  StringVal res(ctx, MIN_POINT_SIZE);

  setSrid(res, srid);
  setOGCType(res, ST_POINT);
  setEsriType(res, ShapePoint);
  setMinX(res, x);
  setMinY(res, y);

  return res;
}

inline box2d getBBox(const StringVal& geom, OGCType type) {
  bool is_lhs_point = type == ST_POINT;
  double xmin = getMinX(geom);
  double ymin = getMinY(geom);
  double xmax = is_lhs_point ? xmin : getMaxX(geom);
  double ymax = is_lhs_point ? ymin : getMaxY(geom);

  point2d min_corner(xmin, ymin);
  point2d max_corner(xmax, ymax);

  return box2d(min_corner, max_corner);
}

inline bool BBoxIntersects(const StringVal& lhs_geom, const StringVal rhs_geom,
  OGCType lhs_type, OGCType rhs_type) {
  bool is_lhs_point = lhs_type == ST_POINT;
  double xmin1 = getMinX(lhs_geom);
  double ymin1 = getMinY(lhs_geom);
  double xmax1 = is_lhs_point ? xmin1 : getMaxX(lhs_geom);
  double ymax1 = is_lhs_point ? ymin1 : getMaxY(lhs_geom);

  bool is_rhs_point = rhs_type == ST_POINT;
  double xmin2 = getMinX(rhs_geom);
  double ymin2 = getMinY(rhs_geom);
  double xmax2 = is_rhs_point ? xmin2 : getMaxX(rhs_geom);
  double ymax2 = is_rhs_point ? ymin2 : getMaxY(rhs_geom);

  if (xmax1 < xmin2 || xmax2 < xmin1 || ymax1 < ymin2 || ymax2 < ymin1 ) return false;
  return true;
}

} // namespace impala
