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


namespace impala::geo {

template <class GeometryT>
bool fromWkt(const string& wkt, GeometryT& geom) {
    boost::geometry::read_wkt(wkt, geom);
    return true;
}

template <>
inline bool fromWkt(const string& wkt, polygon2d& geom) {
    boost::geometry::read_wkt(wkt, geom);
    // TODO: can we avoid correct()?
    boost::geometry::correct(geom);
    return true;
}

template <>
inline bool fromWkt(const string& wkt, multi_polygon2d& geom) {
    boost::geometry::read_wkt(wkt, geom);
    // TODO: can we avoid correct()?
    boost::geometry::correct(geom);
    return true;
}

// Throws exception if not successful.
template <>
inline bool fromWkt(const string& wkt, point2d& point) {
  // TODO: It is probably a bug in boost that it doesn't correctly handle 'point empty'.
  // TODO: 'point empty' should be tokenised, e.g. it should be case-insensitive and
  // whitespace insensitive etc.
  if (wkt == "point empty") {
    constexpr double nan = std::numeric_limits<double>::quiet_NaN();
    point = point2d(nan, nan);;
    return true;
  }
  boost::geometry::read_wkt(wkt, point);
  return true;
}

inline bool wktToPoint(FunctionContext* ctx, const StringVal& wkt, point2d& point) {
  std::string s = AnyValUtil::ToString(wkt);
  try {
    fromWkt(s, point);
    return true;
  } catch (boost::geometry::exception& e) {
    ctx->SetError(e.what());
    return false;
  }
}

inline OGCType GetTypeFromWkt(const StringVal& wkt) {
  // TODO: ugly
  if (wkt.is_null) return UNKNOWN;
  char* type_start = reinterpret_cast<char*>(wkt.ptr);
  char* end = type_start + wkt.len;
  while (type_start != end && *type_start == ' ') type_start++; // skip spaces
  if (type_start == end) return UNKNOWN;
  char* type_end = type_start;
  while (type_end != end && *type_end != ' ' && *type_end != '(') type_end++;
  string geom_type(type_start, type_end - type_start);
  boost::algorithm::to_upper(geom_type);
  for (int t = ST_POINT; t <= ST_MULTIPOLYGON; t++) {
    if (geom_type == OgcTypeToWktPrefix[t]) return (OGCType) t;
  }
  return UNKNOWN;
}

} // namespace impala
