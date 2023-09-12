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
#include <sstream>

#include "exprs/geo/common.h"

namespace impala::geo {

// The implementation of getBinId() and getBinEnvelope() is taken from
// https://github.com/Esri/spatial-framework-for-hadoop/blob/7226df669cbfaaf1edbfac0461acd1af45e12b81/hive/src/main/java/com/esri/hadoop/hive/BinUtils.java#L5
struct BinStructure {
  double extent_max;
  double extent_min;
  int64_t num_cols;
};

inline BinStructure calculateBinStructure(double bin_size) {
  BinStructure res;

  // Absolute max number of rows/columns we can have.
  const int64_t max_bins_per_axis = std::sqrt(std::numeric_limits<int64_t>::max());

  // A smaller bin_size gives us a smaller extent width and height that can be addressed
  // by a single 64 bit long.
  const double size = (bin_size < 1) ? max_bins_per_axis * bin_size : max_bins_per_axis;

  res.extent_max = size / 2;
  res.extent_min = res.extent_max - size;
  res.num_cols = std::ceil(size / bin_size);

  return res;
}

inline int64_t getBinId(double bin_size, double x, double y) {
  BinStructure bin_struct = calculateBinStructure(bin_size);

  const int64_t down = (bin_struct.extent_max - y) / bin_size;
  const int64_t over = (x - bin_struct.extent_min) / bin_size;

  return (down * bin_struct.num_cols) + over;
}

inline box2d getBinEnvelope(double bin_size, int64_t bin_id) {
  BinStructure bin_struct = calculateBinStructure(bin_size);
  const int64_t down = bin_id / bin_struct.num_cols;
  const int64_t over = bin_id % bin_struct.num_cols;

  const double xmin = bin_struct.extent_min + (over * bin_size);
  const double xmax = xmin + bin_size;

  const double ymax = bin_struct.extent_max - (down * bin_size);
  const double ymin = ymax - bin_size;

  const point2d min_corner(xmin, ymin);
  const point2d max_corner(xmax, ymax);

  return box2d(min_corner, max_corner);
}

inline box2d getBinEnvelope(double bin_size, double x, double y) {
  BinStructure bin_struct = calculateBinStructure(bin_size);
  const double down = (bin_struct.extent_max - y) / bin_size;
  const double over = (x - bin_struct.extent_min) / bin_size;

  const double xmin = bin_struct.extent_min + (over * bin_size);
  const double xmax = xmin + bin_size;

  const double ymax = bin_struct.extent_max - (down * bin_size);
  const double ymin = ymax - bin_size;

  const point2d min_corner(xmin, ymin);
  const point2d max_corner(xmax, ymax);

  return box2d(min_corner, max_corner);
}

// Return true if the last point repeats the first.
inline bool IsClosed(const vector<point2d>& ring) {
  DCHECK_GE(ring.size(), 2);
  return (ring.front().x() == ring.back().x()) && (ring.front().y() == ring.back().y());
}

inline bool IsClockWise(const vector<point2d>& ring, bool is_closed) {
  // Ignore last point if it repeats the first one.
  int num_points = is_closed ? ring.size() - 1: ring.size();
  DCHECK_GE(num_points, 3);

  // algorithm: https://en.wikipedia.org/wiki/Curve_orientation#Practical_considerations
  // Find point with smallest y (with smallest x, if y is equal). This point is part of
  // the convex hull and with its two neighbours can be used to determine whether the
  // ring is clocwise. We assume that the ring is part of a "simple" polygon, e.g it has
  // no intersections.
  double bottomleft_x = ring[0].x();
  double bottomleft_y = ring[0].y();
  int bottomleft_index = 0;
  for (int i = 1; i < num_points; i++) {
    const point2d& p = ring[i];
    if (p.y() < bottomleft_y || (p.y() == bottomleft_y && p.x() < bottomleft_x)) {
      bottomleft_x = p.x();
      bottomleft_y = p.y();
      bottomleft_index = i;
    }
  }
  point2d a = ring[bottomleft_index == 0 ? num_points - 1: bottomleft_index - 1];
  point2d b = ring[bottomleft_index];
  point2d c = ring[(bottomleft_index + 1) % num_points];
  double determinant =
      (b.x() - a.x()) * (c.y() - a.y()) - (c.x() - a.x()) * (b.y()-a.y());

  return determinant < 0;
}

// Checks whether the ring is closed and clockwise.
// Returns false if the ring is malformed.
inline bool CheckRing(FunctionContext* ctx, const vector<point2d>& ring,
    bool* is_closed, bool* is_clockwise) {
  if (ring.size() < 3) {
    ctx->SetError("CheckRing: ring should have at least 3 vertices");
    return false;
  }
  *is_closed = IsClosed(ring);
  if (*is_closed && ring.size() < 4){
    ctx->SetError("CheckRing: closed ring should have at least 4 vertices");
    return false;
  }
  *is_clockwise = IsClockWise(ring, *is_closed);
  return true;
}

inline bool RingsToPolygon(FunctionContext* ctx, vector<vector<point2d>>& rings, polygon2d& polygon) {
  if (rings.empty()) {
    ctx->SetError("Empty polygon.");
    return false;
  }
  polygon.clear();

  bool outer_found = false;
  for (int i = 0; i < rings.size(); i++) {
    bool is_closed, is_clockwise;
    if (!CheckRing(ctx, rings[i], &is_closed, &is_clockwise)) return false;
    if (is_clockwise) {
      if (outer_found) {
        ctx->SetError("More than one outer (clockwise) rings.");
        return false;
      }
      outer_found = true;
      std::swap(polygon.outer(), rings[i]);
    } else {
      polygon.inners().emplace_back();
      std::swap(polygon.inners().back(), rings[i]);
    }
  }
  boost::geometry::correct(polygon);
  return true;
}

inline bool RingsToMultiPolygon(FunctionContext* ctx, vector<vector<point2d>>& rings, multi_polygon2d& mpolygon) {
  if (rings.empty()) {
    ctx->SetError("Empty polygon.");
    return false;
  }
  mpolygon.clear();
  for (int i = 0; i < rings.size(); i++) {
    bool is_closed, is_clockwise;
    if (!CheckRing(ctx, rings[i], &is_closed, &is_clockwise)) return false;
    // Assume that each polygon starts with a clockwise (outer) ring and continues with
    // 0 or more conunter clockwise (inner) rings.
    if (is_clockwise) {
      mpolygon.emplace_back();
      std::swap(mpolygon.back().outer(), rings[i]);
    } else {
      if (i == 0) {
        ctx->SetError("First ring should be outer (clockwise).");
        return false;
      }
      mpolygon.back().inners().emplace_back();
      std::swap(mpolygon.back().inners().back(), rings[i]);
    }
  }
  boost::geometry::correct(mpolygon);
  return true;
}

} // namespace impala
