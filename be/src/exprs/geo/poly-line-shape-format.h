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

#include "exprs/geo/common.h"
#include "exprs/geo/shape-format.h"

namespace impala::geo {

class PolyLineShapeFormat {
  static constexpr int NUM_PARTS_OFFSET = MIN_NON_POINT_SIZE ;
  static constexpr int NUM_PARTS_SIZE = 4;
  static constexpr int NUM_POINTS_OFFSET = NUM_PARTS_OFFSET + NUM_PARTS_SIZE;
  static constexpr int NUM_POINTS_SIZE = 4;
  static constexpr int PARTS_ARRAY_OFFSET = NUM_POINTS_OFFSET + NUM_POINTS_SIZE;

  // 44 comes from shapefile spec
  // TODO: Insert a link to the shapefile spec.
  static_assert(PARTS_ARRAY_OFFSET == ESRI_TYPE_OFFSET + 44);

  template<class GeometryT>
  static StringVal WriteHeader(FunctionContext* ctx, const GeometryT& geom,
      OGCType ogcType, int num_parts, int num_points);

  static void WritePolygonRings(const polygon2d& polygon, int point_array_offset, StringVal& result,
      int* parts_written, int* points_written);

public:
  static bool Read(FunctionContext* ctx, const StringVal& geom,
    linestring2d* linestring, multi_linestring2d* multi_linestring,
    vector<vector<point2d>>* rings);

  //static int WriteNextPart(StringValconst vector<point2d>& points, int* point_offset);
  static StringVal Write(FunctionContext* ctx, const linestring2d& linestring);
  static StringVal Write(FunctionContext* ctx, const multi_linestring2d& multi_linestring);
  static StringVal Write(FunctionContext* ctx, const polygon2d& polygon);
  static StringVal Write(FunctionContext* ctx, const multi_polygon2d& mpolygon);
  static StringVal Write(FunctionContext* ctx, const box2d& box);
};

} // namespace impala
