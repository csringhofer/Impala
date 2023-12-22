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

#include <math.h>
#include <time.h>
#include <limits>
#include <map>
#include <string>

#include <boost/algorithm/string.hpp>
#include <boost/date_time/c_local_time_adjustor.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/regex.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/unordered_map.hpp>

#include <openssl/sha.h>

#include "codegen/llvm-codegen.h"
#include "common/init.h"
#include "common/object-pool.h"
#include "exprs/anyval-util.h"
#include "exprs/is-null-predicate.h"
#include "exprs/like-predicate.h"
#include "exprs/literal.h"
#include "exprs/null-literal.h"
#include "exprs/scalar-expr-evaluator.h"
#include "exprs/scalar-expr.h"
#include "exprs/string-functions.h"
#include "exprs/timestamp-functions.h"
#include "gen-cpp/Exprs_types.h"
#include "gen-cpp/ImpalaInternalService_types.h"
#include "gen-cpp/hive_metastore_types.h"
#include "rpc/thrift-client.h"
#include "rpc/thrift-server.h"
#include "runtime/date-value.h"
#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "runtime/multi-precision.h"
#include "runtime/raw-value.inline.h"
#include "runtime/runtime-state.h"
#include "runtime/string-value.h"
#include "runtime/timestamp-parse-util.h"
#include "runtime/timestamp-value.h"
#include "runtime/timestamp-value.inline.h"
#include "service/fe-support.h"
#include "service/impala-server.h"
#include "statestore/statestore.h"
#include "testutil/gtest-util.h"
#include "testutil/impalad-query-executor.h"
#include "testutil/in-process-servers.h"
#include "udf/udf-test-harness.h"
#include "util/asan.h"
#include "util/debug-util.h"
#include "util/decimal-util.h"
#include "util/metrics.h"
#include "util/openssl-util.h"
#include "util/string-parser.h"
#include "util/string-util.h"
#include "util/test-info.h"
#include "utility-functions.h"
#include "gutil/strings/strcat.h"

#include "common/names.h"

namespace posix_time = boost::posix_time;
using boost::bad_lexical_cast;
using boost::date_time::c_local_adjustor;
using boost::posix_time::from_time_t;
using boost::posix_time::ptime;
using boost::posix_time::to_tm;
using std::numeric_limits;
using namespace Apache::Hadoop::Hive;

namespace impala {

class ExprTestBase : public testing::TestWithParam<std::tuple<bool, bool>> {
 protected:
  ImpaladQueryExecutor* executor_;

  // Pool for objects to be destroyed during test teardown.
  ObjectPool pool_;

  // Maps from enum value of primitive integer type to the minimum value that is
  // outside of the next smaller-resolution type. For example the value for type
  // TYPE_SMALLINT is numeric_limits<int8_t>::max()+1. There is a GREATEST test in
  // the MathFunctions tests that requires this to be an ordered map.
  typedef map<int, int64_t> IntValMap;
  IntValMap min_int_values_;

  // Maps from primitive float type to smallest positive value that is larger
  // than the largest value of the next smaller-resolution type.
  typedef unordered_map<int, double> FloatValMap;
  FloatValMap min_float_values_;

  // Maps from enum value of primitive type to a string representation of a default
  // value for testing. For int and float types the strings represent the corresponding
  // min values (in the maps above). For non-numeric types the default values are listed
  // below.
  unordered_map<int, string> default_type_strs_;
  string default_bool_str_;
  string default_string_str_;
  string default_timestamp_str_;
  string default_decimal_str_;
  string default_date_str_;
  // Corresponding default values.
  bool default_bool_val_;
  string default_string_val_;
  TimestampValue default_timestamp_val_;
  DateValue default_date_val_;

  bool disable_codegen_;
  bool enable_expr_rewrites_;

  virtual void SetUpExprTestBase(ImpaladQueryExecutor* executor) {
    executor_ = executor;
    disable_codegen_ = std::get<0>(GetParam());
    enable_expr_rewrites_ = std::get<1>(GetParam());
    LOG(INFO) << Substitute(
      "Test case: disable_codegen=$0  enable_expr_rewrites=$1",
      disable_codegen_, enable_expr_rewrites_);
    executor_->ClearExecOptions();
    executor_->PushExecOption(Substitute("DISABLE_CODEGEN=$0",
        disable_codegen_ ? 1 : 0));
    executor_->PushExecOption(Substitute("ENABLE_EXPR_REWRITES=$0",
        enable_expr_rewrites_ ? 1 : 0));

    // The following have no effect when codegen is disabled, but don't
    // harm anything either. They generally prevent the planner from doing
    // anything clever here.
    executor_->PushExecOption("EXEC_SINGLE_NODE_ROWS_THRESHOLD=0");
    executor_->PushExecOption("DISABLE_CODEGEN_ROWS_THRESHOLD=0");

    // Some tests select rows that take a long time to materialize (e.g.
    // "select length(unhex(repeat('a', 1024 * 1024 * 1024)))") so set the client fetch
    // timeout to a high value.
    executor_->PushExecOption("FETCH_ROWS_TIMEOUT_MS=100000");

    min_int_values_[TYPE_TINYINT] = 1;
    min_int_values_[TYPE_SMALLINT] =
        static_cast<int64_t>(numeric_limits<int8_t>::max()) + 1;
    min_int_values_[TYPE_INT] = static_cast<int64_t>(numeric_limits<int16_t>::max()) + 1;
    min_int_values_[TYPE_BIGINT] =
        static_cast<int64_t>(numeric_limits<int32_t>::max()) + 1;

    min_float_values_[TYPE_FLOAT] = 1.1;
    min_float_values_[TYPE_DOUBLE] =
        static_cast<double>(numeric_limits<float>::max()) + 1.1;

    // Set up default test types, values, and strings.
    default_bool_str_ = "false";
    default_string_str_ = "'abc'";
    default_timestamp_str_ = "cast('2011-01-01 09:01:01' as timestamp)";
    default_decimal_str_ = "1.23";
    default_date_str_ = "cast('2011-01-01' as date)";
    default_bool_val_ = false;
    default_string_val_ = "abc";
    default_timestamp_val_ = TimestampValue::FromUnixTime(1293872461, UTCPTR);
    default_date_val_ = DateValue(2011, 1, 1);
    default_type_strs_[TYPE_TINYINT] =
        lexical_cast<string>(min_int_values_[TYPE_TINYINT]);
    default_type_strs_[TYPE_SMALLINT] =
        lexical_cast<string>(min_int_values_[TYPE_SMALLINT]);
    default_type_strs_[TYPE_INT] =
        lexical_cast<string>(min_int_values_[TYPE_INT]);
    default_type_strs_[TYPE_BIGINT] =
        lexical_cast<string>(min_int_values_[TYPE_BIGINT]);
    // Don't use lexical cast here because it results
    // in a string 1.1000000000000001 that messes up the tests.
    stringstream ss;
    ss << "cast("
       << lexical_cast<string>(min_float_values_[TYPE_FLOAT]) << " as float)";
    default_type_strs_[TYPE_FLOAT] = ss.str();
    ss.str("");
    ss << "cast("
       << lexical_cast<string>(min_float_values_[TYPE_FLOAT]) << " as double)";
    default_type_strs_[TYPE_DOUBLE] = ss.str();
    default_type_strs_[TYPE_BOOLEAN] = default_bool_str_;
    default_type_strs_[TYPE_STRING] = default_string_str_;
    default_type_strs_[TYPE_TIMESTAMP] = default_timestamp_str_;
    default_type_strs_[TYPE_DECIMAL] = default_decimal_str_;
    default_type_strs_[TYPE_DATE] = default_date_str_;
  }

  virtual void TearDown() { pool_.Clear(); }

  string GetValue(const string& expr, const ColumnType& expr_type,
      bool expect_error = false) {
    string stmt = "select " + expr;
    vector<FieldSchema> result_types;
    Status status = executor_->Exec(stmt, &result_types);
    if (!status.ok()) {
      EXPECT_TRUE(expect_error) << "stmt: " << stmt << "\nerror: " << status.GetDetail();
      return "";
    }
    string result_row;
    status = executor_->FetchResult(&result_row);
    if (expect_error) {
      EXPECT_FALSE(status.ok()) << "Expected error\nstmt: " << stmt;
      return "";
    }
    EXPECT_TRUE(status.ok()) << "stmt: " << stmt << "\nerror: " << status.GetDetail();
    string odbcType = TypeToOdbcString(expr_type.ToThrift());
    // ColumnType cannot be BINARY, so STRING is used instead.
    string expectedType =
        result_types[0].type == "binary" ? "string" : result_types[0].type;
    EXPECT_EQ(odbcType, expectedType) << expr;
    return result_row;
  }

  template <typename T>
  inline T ConvertValue(const string& value);

  void TestStringValue(const string& expr, const string& expected_result) {
    EXPECT_EQ(expected_result, GetValue(expr, ColumnType(TYPE_STRING))) << expr;
  }

  void TestCharValue(const string& expr, const string& expected_result,
                     const ColumnType& type) {
    EXPECT_EQ(expected_result, GetValue(expr, type)) << expr;
  }

  string TestStringValueRegex(const string& expr, const string& regex) {
    const string results = GetValue(expr, ColumnType(TYPE_STRING));
    static const boost::regex e(regex);
    const bool is_regex_match = regex_match(results, e);
    EXPECT_TRUE(is_regex_match);
    return results;
  }

  template <class T> void TestValue(const string& expr, PrimitiveType expr_type,
                                    const T& expected_result) {
    return TestValue(expr, ColumnType(expr_type), expected_result);
  }

  template <class T> void TestValue(const string& expr, const ColumnType& expr_type,
                                    const T& expected_result) {
    const string result = GetValue(expr, expr_type);

    switch (expr_type.type) {
      case TYPE_BOOLEAN:
        EXPECT_EQ(expected_result, ConvertValue<bool>(result)) << expr;
        break;
      case TYPE_TINYINT:
        EXPECT_EQ(expected_result, ConvertValue<int8_t>(result)) << expr;
        break;
      case TYPE_SMALLINT:
        EXPECT_EQ(expected_result, ConvertValue<int16_t>(result)) << expr;
        break;
      case TYPE_INT:
        EXPECT_EQ(expected_result, ConvertValue<int32_t>(result)) << expr;
        break;
      case TYPE_BIGINT:
        EXPECT_EQ(expected_result, ConvertValue<int64_t>(result)) << expr;
        break;
      case TYPE_FLOAT: {
        // Converting the float back from a string is inaccurate so convert
        // the expected result to a string.
        // In case the expected_result was passed in as an int or double, convert it.
        string expected_str;
        float expected_float;
        expected_float = static_cast<float>(expected_result);
        RawValue::PrintValue(reinterpret_cast<const void*>(&expected_float),
                             ColumnType(TYPE_FLOAT), -1, &expected_str);
        EXPECT_EQ(expected_str, result) << expr;
        break;
      }
      case TYPE_DOUBLE: {
        string expected_str;
        double expected_double;
        expected_double = static_cast<double>(expected_result);
        RawValue::PrintValue(reinterpret_cast<const void*>(&expected_double),
                             ColumnType(TYPE_DOUBLE), -1, &expected_str);
        EXPECT_EQ(expected_str, result) << expr;
        break;
      }
      default:
        ASSERT_TRUE(false) << "invalid TestValue() type: " << expr_type;
    }
  }

  void TestIsNull(const string& expr, PrimitiveType expr_type) {
    return TestIsNull(expr, ColumnType(expr_type));
  }

  void TestIsNull(const string& expr, const ColumnType& expr_type) {
    EXPECT_TRUE(GetValue(expr, expr_type) == "NULL") << expr;
  }

  template <class T>
  void TestValueOrError(const string& expr, PrimitiveType expr_type,
      const T& expected_result, bool expect_error) {
    if (expect_error) {
      TestError(expr);
    } else {
      TestValue(expr, expr_type, expected_result);
    }
  }

  void TestIsNotNull(const string& expr, PrimitiveType expr_type) {
    return TestIsNotNull(expr, ColumnType(expr_type));
  }

  void TestIsNotNull(const string& expr, const ColumnType& expr_type) {
    EXPECT_TRUE(GetValue(expr, expr_type) != "NULL") << expr;
  }

  void TestError(const string& expr) {
    GetValue(expr, ColumnType(INVALID_TYPE), /* expect_error */ true);
  }

  void TestNonOkStatus(const string& expr) {
    string stmt = "select " + expr;
    vector<FieldSchema> result_types;
    Status status = executor_->Exec(stmt, &result_types);
    ASSERT_FALSE(status.ok()) << "stmt: " << stmt << "\nunexpected Status::OK.";
  }

  // "Execute 'expr' and check that the returned error ends with 'error_string'"
  void TestErrorString(const string& expr, const string& error_string) {
    string stmt = "select " + expr;
    vector<FieldSchema> result_types;
    string result_row;
    Status status = executor_->Exec(stmt, &result_types);
    status = executor_->FetchResult(&result_row);
    ASSERT_FALSE(status.ok());
    ASSERT_TRUE(EndsWith(status.msg().msg(), error_string)) << "Actual: '"
        << status.msg().msg() << "'" << endl << "Expected: '" << error_string << "'";
  }


  void inline TestTimestampValue(const string& expr, const TimestampValue& expected_result);

  void inline TestDateValue(const string& expr, const DateValue& expected_result);

  template<typename T>
  inline TimestampValue CreateTestTimestamp(T val);

  // Parse the given string representation of a value into 'val' of type T.
  // Returns false on parse failure.
  template<class T>
  inline bool ParseString(const string& str, T* val);
};

template<>
inline TimestampValue ExprTestBase::CreateTestTimestamp(const string& val) {
  return TimestampValue::ParseSimpleDateFormat(val);
}

template<>
inline TimestampValue ExprTestBase::CreateTestTimestamp(float val) {
  return TimestampValue::FromSubsecondUnixTime(val, UTCPTR);
}

template<>
inline TimestampValue ExprTestBase::CreateTestTimestamp(double val) {
  return TimestampValue::FromSubsecondUnixTime(val, UTCPTR);
}

template<>
inline TimestampValue ExprTestBase::CreateTestTimestamp(int val) {
  return TimestampValue::FromUnixTime(val, UTCPTR);
}

template<>
inline TimestampValue ExprTestBase::CreateTestTimestamp(int64_t val) {
  return TimestampValue::FromUnixTime(val, UTCPTR);
}

template <>
inline bool ExprTestBase::ConvertValue<bool>(const string& value) {
  if (value.compare("false") == 0) {
    return false;
  } else {
    DCHECK(value.compare("true") == 0) << value;
    return true;
  }
}

template <>
inline int8_t ExprTestBase::ConvertValue<int8_t>(const string& value) {
  StringParser::ParseResult result;
  return StringParser::StringToInt<int8_t>(value.data(), value.size(), &result);
}

template <>
inline int16_t ExprTestBase::ConvertValue<int16_t>(const string& value) {
  StringParser::ParseResult result;
  return StringParser::StringToInt<int16_t>(value.data(), value.size(), &result);
}

template <>
inline int32_t ExprTestBase::ConvertValue<int32_t>(const string& value) {
  StringParser::ParseResult result;
  return StringParser::StringToInt<int32_t>(value.data(), value.size(), &result);
}

template <>
inline int64_t ExprTestBase::ConvertValue<int64_t>(const string& value) {
  StringParser::ParseResult result;
  return StringParser::StringToInt<int64_t>(value.data(), value.size(), &result);
}

template <>
inline double ExprTestBase::ConvertValue<double>(const string& value) {
  StringParser::ParseResult result;
  return StringParser::StringToFloat<double>(value.data(), value.size(), &result);
}

template <>
inline TimestampValue ExprTestBase::ConvertValue<TimestampValue>(const string& value) {
  return TimestampValue::ParseSimpleDateFormat(value.data(), value.size());
}

template <>
inline DateValue ExprTestBase::ConvertValue<DateValue>(const string& value) {
  return DateValue::ParseSimpleDateFormat(value.data(), value.size());
}

// We can't put this into TestValue() because GTest can't resolve
// the ambiguity in TimestampValue::operator==, even with the appropriate casts.
void ExprTestBase::TestTimestampValue(const string& expr, const TimestampValue& expected_result) {
  EXPECT_EQ(expected_result,
      ConvertValue<TimestampValue>(GetValue(expr, ColumnType(TYPE_TIMESTAMP))));
}


// We can't put this into TestValue() because GTest can't resolve
// the ambiguity in DateValue::operator==, even with the appropriate casts.
void ExprTestBase::TestDateValue(const string& expr, const DateValue& expected_result) {
  EXPECT_EQ(expected_result,
      ConvertValue<DateValue>(GetValue(expr, ColumnType(TYPE_DATE))));
}

template<class T>
inline bool ExprTestBase::ParseString(const string& str, T* val) {
  istringstream stream(str);
  stream >> *val;
  return !stream.fail();
}

template<>
inline bool ExprTestBase::ParseString(const string& str, TimestampValue* val) {
  boost::gregorian::date date;
  boost::posix_time::time_duration time;
  bool success = TimestampParser::ParseSimpleDateFormat(str.data(), str.length(), &date,
      &time);
  val->set_date(date);
  val->set_time(time);
  return success;
}

template<>
inline bool ExprTestBase::ParseString(const string& str, DateValue* val) {
  return DateParser::ParseSimpleDateFormat(str.c_str(), str.length(), false, val);
}

} // namespace impala
