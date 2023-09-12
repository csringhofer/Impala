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

#include <iostream>

#include <gutil/strings/numbers.h>

namespace impala::geo {

// This class is used as coordinate type instead of doubles in boost geometries.
// The only change compared to builtin 'double' should be in printing with <<. This was
// needed to get compatible output during WKT conversion with the Java implementation.
//
// The precision of doubles in WKT doesn't seen consistant between different
// engines:
//
// In PostGis:
// select ST_AsText(ST_Intersection(
//   ST_GeomFromText('POLYGON ((2 0, 2 3, 3 0, 2 0))'),
//   ST_GeomFromEWkt('POLYGON ((1 1, 4 1, 4 4, 1 4, 1 1))')));
//
//                st_astext
//--------------------------------------------
//   POLYGON((2 3,2.666666666666666 1,2 1,2 3))
//
// With ESRI Hive the doule is different:
//   2.6666666666666665
//
// Setting precision in ostream is not enough to get the same output:
//   2.666666666666667   (precision 16)
//   2.6666666666666665  (precision 17)
//   2.66666666666666652 (precision 18)
//
// Using gutil's SimpleDtoa() seems to behave exactly the same way as ESRI's Java
// implementation and lets st_AsText() pass all tests. See the comment of SimpleDtoa() in
// gutil for more info. Using SimpleDtoa() should also ensure that no precision is lost
// during binary->WKT->binary conversion.
//
// This may need to be revisited if other geospatial tests suites are also run as we may
// hit similar incompatibility with other database engines.
class FormattedDouble {
 public:
  FormattedDouble(double val): val_(val) {}

  FormattedDouble() = default;
  FormattedDouble(const FormattedDouble&) = default;
  FormattedDouble& operator=(const FormattedDouble&) = default;

  operator double() const {
    return val_;
  }

  FormattedDouble& operator+=(const FormattedDouble& rhs) {
    val_ += rhs.val_;
    return *this;
  }

  FormattedDouble operator-() const {
    return -val_;
  }

  FormattedDouble& operator/=(const FormattedDouble& rhs) {
    val_ /= rhs.val_;
    return *this;
  }

 private:
  double val_;
};

inline std::istream& operator>>(std::istream& lhs, FormattedDouble& rhs) {
  double d;
  lhs >> d;
  rhs = d;
  return lhs;
}

inline std::ostream& operator<<(std::ostream& lhs, const FormattedDouble& rhs) {
  // Trying to be consistent with this:
  // https://github.com/Esri/geometry-api-java/blob/d9ed3598b72029c9ebde024e0e616933cff81db2/src/main/java/com/esri/core/geometry/OperatorExportToWktLocal.java#L797
  lhs << SimpleDtoa(rhs);
  return lhs;
}

} // namespace impala
