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

class MultiPointShapeFormat {
  static constexpr int NUM_POINTS_OFFSET = MIN_NON_POINT_SIZE;
  static constexpr int NUM_POINT_SIZE = 4;
  static constexpr int POINT_ARRAY_OFFSET = MIN_NON_POINT_SIZE + NUM_POINT_SIZE;

  // 40 comes from shapefile spec
  static_assert(POINT_ARRAY_OFFSET == ESRI_TYPE_OFFSET + 40);


public:
  static bool Read(FunctionContext* ctx, const StringVal& geom, multipoint2d& out);
  static StringVal Write(FunctionContext* ctx, const multipoint2d& mpoint);
};

} // namespace impala
