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

#include <boost/geometry/geometry.hpp>

#include "exprs/geo/formatted-double.h"
#include "udf/udf.h"
#include "util/bit-util.h"

namespace impala::geo {

using impala_udf::FunctionContext;
using impala_udf::StringVal;

constexpr int SRID_SIZE = 4;
constexpr int OGC_TYPE_SIZE = 1;

constexpr int SRID_OFFSET = 0;
constexpr int OGC_TYPE_OFFSET = 4;

static_assert(OGC_TYPE_OFFSET == SRID_SIZE);

// TODO: This file is included in impala-ir.cc, and these typedefs will be visible in the
// impala namespace in files included after this. This is not very good, we could put
// these into a class as member typedefs or in an impala::geometry namespace.
using point2d = boost::geometry::model::d2::point_xy<FormattedDouble>;
using box2d = boost::geometry::model::box<point2d>;
using linestring2d = boost::geometry::model::linestring<point2d>;
using polygon2d = boost::geometry::model::polygon<point2d, true>;
using multipoint2d = boost::geometry::model::multi_point<point2d>;
using multi_linestring2d = boost::geometry::model::multi_linestring<linestring2d>;
using multi_polygon2d = boost::geometry::model::multi_polygon<polygon2d>;

// see https://github.com/Esri/spatial-framework-for-hadoop/blob/7226df669cbfaaf1edbfac0461acd1af45e12b81/hive/src/main/java/com/esri/hadoop/hive/GeometryUtils.java#L21
enum OGCType {
    UNKNOWN = 0,
		ST_POINT = 1,
		ST_LINESTRING = 2,
		ST_POLYGON = 3,
		ST_MULTIPOINT = 4,
		ST_MULTILINESTRING = 5,
		ST_MULTIPOLYGON = 6
};

constexpr std::array<const char*, ST_MULTIPOLYGON + 1> OGCTypeToStr = {{
    "UNKNOWN",
    "ST_POINT",
    "ST_LINESTRING",
    "ST_POLYGON",
    "ST_MULTIPOINT",
    "ST_MULTILINESTRING",
    "ST_MULTIPOLYGON"
}};

constexpr std::array<const char*, ST_MULTIPOLYGON + 1> OgcTypeToWktPrefix = {{
    "UNKNOWN",
    "POINT",
    "LINESTRING",
    "POLYGON",
    "MULTIPOINT",
    "MULTILINESTRING",
    "MULTIPOLYGON"
}};

template <class T>
T readFromGeom(const StringVal& geom, int offset) {
  DCHECK_GE(geom.len, offset + sizeof(T));
  return *reinterpret_cast<T*>(geom.ptr + offset);
}

template <class T>
void writeToGeom(const T& val, StringVal& geom, int offset) {
  DCHECK_GE(geom.len, offset + sizeof(T));
  T* ptr = reinterpret_cast<T*>(geom.ptr + offset);
  *ptr = val;
}

inline uint32_t getSrid(const StringVal& geom) {
  static_assert(SRID_SIZE == sizeof(uint32_t));

  // SRID is in big endian format in 'geom', but Impala only supports little endian so we
  // have to convert it.
#ifndef IS_LITTLE_ENDIAN
  static_assert(false, "Only the little endian byte order is supported.");
#endif
  const uint32_t srid_bytes = readFromGeom<uint32_t>(geom, SRID_OFFSET);
  return BitUtil::ByteSwap(srid_bytes);
}

inline OGCType getOGCType(const StringVal& geom) {
  static_assert(OGC_TYPE_SIZE == sizeof(char));
  const char res = readFromGeom<char>(geom, OGC_TYPE_OFFSET);
  return static_cast<OGCType>(res);
}

inline constexpr const char* getGeometryType(OGCType ogc_type) {
  return OGCTypeToStr[ogc_type];
}

inline void setSrid(StringVal& geom, uint32_t srid) {
  static_assert(SRID_SIZE == sizeof(uint32_t));

  // SRID is in big endian format in 'geom', but Impala only supports little endian so we
  // have to convert it.
#ifndef IS_LITTLE_ENDIAN
  static_assert(false, "Only the little endian byte order is supported.");
#endif
  const uint32_t srid_bytes = BitUtil::ByteSwap(srid);
  writeToGeom<uint32_t>(srid_bytes, geom, SRID_OFFSET);
}

inline void setOGCType(StringVal& geom, OGCType ogc_type) {
  writeToGeom<char>(ogc_type, geom, OGC_TYPE_OFFSET);
}

template <>
inline void writeToGeom<point2d>(const point2d& point, StringVal& geom, int offset) {
  DCHECK_GE(geom.len, offset + 2 * sizeof(double));
  writeToGeom<double>(point.x(), geom, offset);
  writeToGeom<double>(point.y(), geom, offset + sizeof(double));
}

} // namespace impala
