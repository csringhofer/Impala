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

#include "exprs/geo/multi-point-shape-format.h"

namespace impala::geo {


bool MultiPointShapeFormat::Read(FunctionContext* ctx, const StringVal& geom, multipoint2d& out) {
  if (geom.len < POINT_ARRAY_OFFSET) {
    ctx->SetError("Multipoint lenght too small.");
    return false;
  }
  int num_points = readFromGeom<uint32_t>(geom, NUM_POINTS_OFFSET);
  if (geom.len != POINT_ARRAY_OFFSET + num_points * 16) {
    ctx->SetError("Multipoint has invalid length.");
    return false;
  }
  out.clear();
  for (int i = 0; i < num_points; i++) {
    int offset = POINT_ARRAY_OFFSET + i * 16;
    boost::geometry::append(out, point2d(readFromGeom<double>(geom, offset),
        readFromGeom<double>(geom, offset + 8)));
  }

  return true;
}

StringVal MultiPointShapeFormat::Write(FunctionContext* ctx, const multipoint2d& mpoint) {
  int num_points = mpoint.size();
  if (num_points == 0) {
    ctx->SetError("Empty multipoint.");
    return StringVal::null();
  }

  int size = MIN_NON_POINT_SIZE + NUM_POINT_SIZE + 16 * num_points;
  StringVal result(ctx, size);

  box2d bounding_rect;
  boost::geometry::envelope(mpoint, bounding_rect);
  writeHeader(result, ST_MULTIPOINT, bounding_rect);

  writeToGeom<uint32_t>(num_points, result, NUM_POINTS_OFFSET);
  for (int i = 0; i < num_points; i++) {
    writeToGeom<double>(mpoint[i].x(), result, POINT_ARRAY_OFFSET + i * 16);
    writeToGeom<double>(mpoint[i].y(), result, POINT_ARRAY_OFFSET + i * 16 + 8);
  }
  return result;
}

} // namespace impala
