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

#include "exprs/geo/poly-line-shape-format.h"

#include "common/names.h"

namespace impala::geo {

bool PolyLineShapeFormat::Read(FunctionContext* ctx, const StringVal& geom,
    linestring2d* linestring, multi_linestring2d* multi_linestring,
    vector<vector<point2d>>* rings) {
  bool is_line = linestring != nullptr;
  bool is_multi_line = multi_linestring != nullptr;
  bool is_polygon = rings != nullptr;

  // The same format (PolyLine) is used to encode linestring and multi linestring.
  // Only one of 'linestring' and 'multi_linestring' must be non-null and the PolyLine
  // will be deserialized to that shape.
  if (is_multi_line) {
    DCHECK_EQ(linestring, nullptr);
    DCHECK_EQ(rings, nullptr);
  }
  if (is_polygon) {
    DCHECK_EQ(linestring, nullptr);
    DCHECK_EQ(multi_linestring, nullptr);
  }
  if (is_line) {
    DCHECK_NE(linestring, nullptr);
  }

  if (geom.len < PARTS_ARRAY_OFFSET) {
    ctx->SetError("PolyLine lenght too small.");
    return false;
  }
  int num_parts = readFromGeom<uint32_t>(geom, NUM_PARTS_OFFSET);
  if (num_parts < 0 || (is_line && num_parts > 1)) {
    ctx->SetError("PolyLine has invalid num_parts.");
    return false;
  }

  int num_points = readFromGeom<uint32_t>(geom, NUM_POINTS_OFFSET);
  int point_array_offset = PARTS_ARRAY_OFFSET + num_parts * 4;
  if (geom.len != point_array_offset +  num_points * 16) {
    ctx->SetError("PolyLine has invalid length.");
    return false;
  }

  if (is_multi_line) {
    // TODO: Is this a vector of vectors? This could make clear + reallocation expensive.
    multi_linestring->clear();
  } else if (is_line) {
    linestring->clear();
  }

  for (int part = 0; part < num_parts; part++) {
    int first_point_offset = PARTS_ARRAY_OFFSET + 4 * part;
    int first_point = readFromGeom<uint32_t>(geom, first_point_offset);
    // Last point of part is either the one before the next part or the last point
    // if this is the last part.
    int last_point = (part == num_parts - 1) ? (num_points - 1) :
       (readFromGeom<uint32_t>(geom, first_point_offset + 4) - 1);
    if (first_point < 0 || first_point >= num_points || last_point < 0
        || last_point >= num_points) {
      string msg = Substitute("PolyLine has invalid part index. First: $0 last: $1 num_point $2 num_parts: $3",
        first_point, last_point, num_points, num_parts);
      ctx->SetError(msg.c_str());
      return false;
    }
    if (is_polygon) {
      rings->emplace_back();
    } else if (is_multi_line) {
      multi_linestring->emplace_back();
    }
    for (int i = first_point; i <= last_point; i++) {
      int offset = point_array_offset + i * 16;
      point2d p(readFromGeom<double>(geom, offset),
          readFromGeom<double>(geom, offset + 8));
      if (is_polygon) {
        rings->back().push_back(p);
      } else if(is_multi_line) {
        multi_linestring->back().push_back(p);
      } else {
        linestring->push_back(p);
      }
    }
  }

  return true;
}

void WritePoints(const vector<point2d>& points, StringVal& result, int offset) {
  for (int i = 0; i < points.size(); i++) {
    writeToGeom<double>(points[i].x(), result, offset + i * 16);
    writeToGeom<double>(points[i].y(), result, offset + i * 16 + 8);
  }
}

template<class GeometryT>
StringVal PolyLineShapeFormat::WriteHeader(
    FunctionContext* ctx, const GeometryT& geom, OGCType ogcType, int num_parts, int num_points) {
  int size = MIN_NON_POINT_SIZE + NUM_PARTS_SIZE + NUM_POINTS_SIZE + 4 * num_parts + 16 * num_points;
  StringVal result(ctx, size);

  if (result.len != size) {
    ctx->SetError("couldn't allocate memory");
    return StringVal::null();
  }

  box2d bounding_rect;
  boost::geometry::envelope(geom, bounding_rect);
  writeHeader(result, ogcType, bounding_rect);

  writeToGeom<uint32_t>(num_parts, result, NUM_PARTS_OFFSET);
  writeToGeom<uint32_t>(num_points, result, NUM_POINTS_OFFSET);

  return result;
}

StringVal PolyLineShapeFormat::Write(FunctionContext* ctx, const linestring2d& linestring) {
  int num_points = linestring.size();
  if (num_points == 0) {
    ctx->SetError("Empty multipoint.");
    return StringVal::null();
  }

  StringVal result = WriteHeader(ctx, linestring, ST_LINESTRING, 1, num_points);

  writeToGeom<uint32_t>(0, result, PARTS_ARRAY_OFFSET);

  const int point_array_offset = PARTS_ARRAY_OFFSET + 4;
  WritePoints(linestring, result, point_array_offset);

  return result;
}

StringVal PolyLineShapeFormat::Write(FunctionContext* ctx, const multi_linestring2d& multi_linestring) {
  int num_parts = multi_linestring.size();
  int num_points = 0;
  for (const linestring2d& line: multi_linestring) {
    num_points += line.size();
  }

  StringVal result = WriteHeader(ctx, multi_linestring, ST_MULTILINESTRING, num_parts, num_points);

  int point_array_offset = PARTS_ARRAY_OFFSET + num_parts * 4;
  int points_written = 0;
  for (int part = 0; part < num_parts; part++) {
    writeToGeom<uint32_t>(points_written, result, PARTS_ARRAY_OFFSET + part * 4);
    WritePoints(multi_linestring[part], result, point_array_offset + points_written * 16);
    points_written += multi_linestring[part].size();
  }

  return result;
}

void IncreasePartAndPointCount(const polygon2d& polygon, int* part_count, int* point_count) {
  *part_count += 1 + polygon.inners().size();
  *point_count += polygon.outer().size();
  for (const auto& ring: polygon.inners()) {
    *point_count += ring.size();
  }
}

void PolyLineShapeFormat::WritePolygonRings(const polygon2d& polygon, int point_array_offset, StringVal& result,
    int* parts_written, int* points_written) {
  writeToGeom<uint32_t>(*points_written, result, PARTS_ARRAY_OFFSET + *parts_written * 4);
  WritePoints(polygon.outer(), result, point_array_offset + *points_written * 16);
  (*parts_written)++;
  *points_written += polygon.outer().size();

  for (const vector<point2d>& ring: polygon.inners()) {
    writeToGeom<uint32_t>(*points_written, result, PARTS_ARRAY_OFFSET + *parts_written * 4);
    WritePoints(ring, result, point_array_offset + *points_written * 16);
    (*parts_written)++;
    *points_written += ring.size();
  }
}

StringVal PolyLineShapeFormat::Write(FunctionContext* ctx, const polygon2d& polygon) {
  int num_parts = 0;
  int num_points = 0;
  IncreasePartAndPointCount(polygon, &num_parts, &num_points);

  StringVal result = WriteHeader(ctx, polygon, ST_POLYGON, num_parts, num_points);

  int parts_written = 0;
  int points_written = 0;
  int point_array_offset = PARTS_ARRAY_OFFSET + num_parts * 4;
  WritePolygonRings(polygon, point_array_offset, result, &parts_written, &points_written);

  return result;
}

StringVal PolyLineShapeFormat::Write(FunctionContext* ctx, const multi_polygon2d& mpolygon) {
  int num_parts = 0;
  int num_points = 0;
  for (const polygon2d& polygon: mpolygon) {
    IncreasePartAndPointCount(polygon, &num_parts, &num_points);
  }

  StringVal result = WriteHeader(ctx, mpolygon, ST_MULTIPOLYGON, num_parts, num_points);
  int parts_written = 0;
  int points_written = 0;
  int point_array_offset = PARTS_ARRAY_OFFSET + num_parts * 4;
  for (const polygon2d& polygon: mpolygon) {
    WritePolygonRings(polygon, point_array_offset, result, &parts_written, &points_written);
  }

  return result;
}

StringVal PolyLineShapeFormat::Write(FunctionContext* ctx, const box2d& box) {
  constexpr int num_parts = 1;
  constexpr int num_points = 5;

  StringVal result = WriteHeader(ctx, box, ST_POLYGON, num_parts, num_points);

  int point_array_offset = PARTS_ARRAY_OFFSET + num_parts * 4;

  writeToGeom<uint32_t>(0, result, PARTS_ARRAY_OFFSET);

  const double xmin = box.min_corner().x();
  const double ymin = box.min_corner().y();

  const double xmax = box.max_corner().x();
  const double ymax = box.max_corner().y();

  constexpr int point_written_length = 2 * sizeof(double);

  writeToGeom<point2d>(point2d(xmin, ymin), result, point_array_offset);
  point_array_offset += point_written_length;

  writeToGeom<point2d>(point2d(xmin, ymax), result, point_array_offset);
  point_array_offset += point_written_length;

  writeToGeom<point2d>(point2d(xmax, ymax), result, point_array_offset);
  point_array_offset += point_written_length;

  writeToGeom<point2d>(point2d(xmax, ymin), result, point_array_offset);
  point_array_offset += point_written_length;

  writeToGeom<point2d>(point2d(xmin, ymin), result, point_array_offset);
  point_array_offset += point_written_length;

  return result;
}

} // namespace impala
