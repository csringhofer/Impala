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

#include "exprs/geo/geometry-wrapper.h"

#include <boost/algorithm/string/replace.hpp>

#include "exprs/geo/multi-point-shape-format.h"
#include "exprs/geo/poly-line-shape-format.h"
#include "exprs/geo/wkt.h"

namespace impala::geo {

bool GeometryWrapper::FromEsriBinary(FunctionContext* ctx, const StringVal& geom, OGCType ogcType) {
  switch (ogcType) {
    case ST_POINT:
      this->point_ = point2d(getMinX(geom), getMinY(geom));
      break;
    case ST_LINESTRING:
      return PolyLineShapeFormat::Read(ctx, geom, &linestring_, nullptr, nullptr);
    case ST_MULTIPOINT:
      return MultiPointShapeFormat::Read(ctx, geom, multi_point_);
    case ST_MULTILINESTRING:
      return PolyLineShapeFormat::Read(ctx, geom, nullptr, &multi_linestring_, nullptr);
    case ST_POLYGON: {
      vector<vector<point2d>> rings;
      if(!PolyLineShapeFormat::Read(ctx, geom, nullptr, nullptr, &rings)) return false;
      return RingsToPolygon(ctx, rings, polygon_);
    }
    case ST_MULTIPOLYGON: {
      vector<vector<point2d>> rings;
      if(!PolyLineShapeFormat::Read(ctx, geom, nullptr, nullptr, &rings)) return false;
      return RingsToMultiPolygon(ctx, rings, multi_polygon_);
    }
    default:
      ctx->SetError("Geometry type not supported.");
      return false;
  }
  return true;
}

StringVal GeometryWrapper::ToEsriBinary(FunctionContext* ctx, OGCType ogcType, int srid) {
  StringVal result = ToEsriBinary(ctx, ogcType);
  if (!result.is_null) setSrid(result, srid);
  return result;
}

StringVal GeometryWrapper::ToEsriBinary(FunctionContext* ctx, OGCType ogcType) {
  switch (ogcType) {
    case ST_POINT:
      return createStPoint(ctx, this->point_.x(), this->point_.y(), 0);
    case ST_LINESTRING:
      return PolyLineShapeFormat::Write(ctx, linestring_);
    case ST_POLYGON:
      return PolyLineShapeFormat::Write(ctx, polygon_);
    case ST_MULTIPOINT:
      return MultiPointShapeFormat::Write(ctx, multi_point_);
    case ST_MULTILINESTRING:
      return PolyLineShapeFormat::Write(ctx, multi_linestring_);
    case ST_MULTIPOLYGON:
      return PolyLineShapeFormat::Write(ctx, multi_polygon_);
    default:
      ctx->SetError("Geometry type not supported.");
      return StringVal::null();
  }
}

string GeometryWrapper::ToWkt(FunctionContext* ctx, OGCType ogcType) {
  string result;
  try {
    stringstream ss;
    //ss.precision(15);
    switch (ogcType) {
      case ST_POINT:
        ss << boost::geometry::wkt(point_);
        break;
      case ST_LINESTRING:
        ss << boost::geometry::wkt(linestring_);
        break;
      case ST_MULTIPOINT:
        ss << boost::geometry::wkt(multi_point_);
        break;
      case ST_MULTILINESTRING:
        ss << boost::geometry::wkt(multi_linestring_);
        break;
      case ST_POLYGON:
        // Reverse rings to conform with ESRI's WKT output.
        //ReverseRings(this->polygon);
        std::reverse(polygon_.outer().begin(), polygon_.outer().end());
        ss << boost::geometry::wkt(polygon_);
        break;
      case ST_MULTIPOLYGON:
        // Reverse rings to conform with ESRI's WKT output.
        //ReverseRings(this->multi_polygon);
        for (polygon2d& polygon: multi_polygon_) {
          std::reverse(polygon.outer().begin(), polygon.outer().end());
        }
        ss << boost::geometry::wkt(multi_polygon_);
        break;
      default:
        string msg = Substitute(
            "GeometryWrapper::ToWkt: Geometry type $0 not supported.", ogcType);
        ctx->SetError(msg.c_str());
        return "";
    }
    result = ss.str();
    // boost::geometry::wkt adds less spaces to the WKT result compared to ESRI's Java
    // implementation, though the results are semantically equivalent. Add spaces here to
    // avoid having different outputs.
    boost::replace_first(result, "(", " (");
    boost::replace_all(result, ",", ", ");
  } catch(boost::geometry::exception& ex) {
    ctx->SetError(ex.what());
  }
  return result;
}


bool GeometryWrapper::FromWkt(FunctionContext* ctx, StringVal wkt, OGCType ogcType) {
  std::string s = AnyValUtil::ToString(wkt);
  try {
    switch (ogcType) {
      case ST_POINT:
        return fromWkt(s, point_);
      case ST_LINESTRING:
        return fromWkt(s, linestring_);
      case ST_POLYGON:
        return fromWkt(s, polygon_);
      case ST_MULTIPOINT:
        return fromWkt(s, multi_point_);
      case ST_MULTILINESTRING:
        return fromWkt(s, multi_linestring_);
      case ST_MULTIPOLYGON:
        return fromWkt(s, multi_polygon_);
      default:
        string msg = Substitute("GeometryWrapper::FromWkt: Geometry type $0 not supported.", ogcType);
        ctx->SetError(msg.c_str());
        return false;
    }
  } catch(boost::geometry::exception& ex) {
    ctx->SetError(ex.what());
    return false;
  }
}

bool CoordinatesToPoints(FunctionContext* ctx, int num_coords, const DoubleVal* coords, vector<point2d>& points) {
  points.reserve(num_coords / 2);
  for (int i = 0; i < num_coords; i += 2) {
    const DoubleVal* x = coords + i ;
    const DoubleVal* y = coords + i + 1;
    if (x->is_null || y->is_null) {
      ctx->SetError("Null coordinate");
      return false;
    }
    points.emplace_back(x->val, y->val);
  }
  return true;
}

bool GeometryWrapper::FromCoordinates(FunctionContext* ctx, int num_coords, const DoubleVal* coords, OGCType ogcType) {
   if (num_coords == 0 || num_coords % 2 != 0) {
    ctx->SetError("Invalid number of coordinates");
    return false;
  }
  switch (ogcType) {
    case ST_LINESTRING:
      this->linestring_.clear();
      return CoordinatesToPoints(ctx, num_coords, coords, linestring_);
    case ST_MULTIPOINT:
      this->multi_point_.clear();
      return CoordinatesToPoints(ctx, num_coords, coords, multi_point_);
    case ST_POLYGON:
      // In the variable argument constructor the coords represent the outer ring.
      this->polygon_.clear();
      if (!CoordinatesToPoints(ctx, num_coords, coords, polygon_.outer())) return false;
      boost::geometry::correct(polygon_);
      return true;
    case ST_MULTILINESTRING:
    case ST_MULTIPOLYGON:
    case ST_POINT:
    default:
      ctx->SetError("Geometry type not supported.");
      return false;
  }
}

} // namespace impala
