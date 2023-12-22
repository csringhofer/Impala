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

#include "exprs/expr-test-common.h"

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
#include "exprs/timezone_db.h"
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

DECLARE_bool(abort_on_config_error);
DECLARE_bool(disable_optimization_passes);
DECLARE_string(hdfs_zone_info_zip);

namespace posix_time = boost::posix_time;
using boost::bad_lexical_cast;
using boost::date_time::c_local_adjustor;
using boost::posix_time::from_time_t;
using boost::posix_time::ptime;
using boost::posix_time::to_tm;
using std::numeric_limits;
using namespace Apache::Hadoop::Hive;
using namespace impala;

namespace impala {
// America/Anguilla timezone does not observe DST.
// Use this timezone in tests where DST changes may cause problems.
const char* TEST_TZ_WITHOUT_DST = "America/Anguilla";

namespace {
scoped_ptr<ImpaladQueryExecutor> global_executor;
scoped_ptr<MetricGroup> statestore_metrics(new MetricGroup("statestore_metrics"));
Statestore* statestore;
}

// Override the time zone for the duration of the scope. The time zone is overridden
// using an environment variable there is no risk of making a permanent system change
// and no special permissions are needed. This is not thread-safe.
class ScopedTimeZoneOverride {
 public:
  ScopedTimeZoneOverride(const char* tz_name)
      : original_tz_name_(getenv("TZ")),
        new_tz_name_(tz_name),
        new_tz_(TimezoneDatabase::FindTimezone(new_tz_name_)) {
    EXPECT_NE(nullptr, new_tz_) << "Could not find " << new_tz_name_ << " time zone.";
    setenv("TZ", new_tz_name_, true);
    tzset();
  }

  ~ScopedTimeZoneOverride() {
    if (original_tz_name_ == nullptr) {
      unsetenv("TZ");
    } else {
      setenv("TZ", original_tz_name_, true);
    }
    tzset();
  }

  const Timezone& GetTimezone() const { return *new_tz_; }

 private:
  const char* const original_tz_name_;
  const char* const new_tz_name_;
  const Timezone* new_tz_;
};

class ScopedExecOption {
 ImpaladQueryExecutor* executor_;
 public:
  ScopedExecOption(ImpaladQueryExecutor* executor, string option_string)
      : executor_(executor) {
    executor->PushExecOption(option_string);
  }

  ~ScopedExecOption() {
    executor_->PopExecOption();
  }
};

class TimestampExprTest : public ExprTestBase {
 public:
  // Run once (independent of parameter values).
  static void SetUpTestCase() {
    InitFeSupport(false);
    ABORT_IF_ERROR(impala::LlvmCodeGen::InitializeLlvm());

    // The host running this test might have an out-of-date tzdata package installed.
    // To avoid tzdata related issues, we will load time-zone db from the testdata
    // directory.
    FLAGS_hdfs_zone_info_zip = Substitute("file://$0/testdata/tzdb/2017c.zip",
        getenv("IMPALA_HOME"));
    ABORT_IF_ERROR(TimezoneDatabase::Initialize());

    // Disable llvm optimization passes if the env var is no set to true. Running without
    // the optimizations makes the tests run much faster.
    char* optimizations = getenv("EXPR_TEST_ENABLE_OPTIMIZATIONS");
    if (optimizations != NULL && strcmp(optimizations, "true") == 0) {
      cout << "Running with optimization passes." << endl;
      FLAGS_disable_optimization_passes = false;
    } else {
      cout << "Running without optimization passes." << endl;
      FLAGS_disable_optimization_passes = true;
    }

    // Create an in-process Impala server and in-process backends for test environment
    // without any startup validation check
    FLAGS_abort_on_config_error = false;
    VLOG_CONNECTION << "creating test env";
    VLOG_CONNECTION << "starting backends";
    statestore = new Statestore(statestore_metrics.get());
    IGNORE_LEAKING_OBJECT(statestore);

    // Pass in 0 to have the statestore use an ephemeral port for the service.
    ABORT_IF_ERROR(statestore->Init(0));
    InProcessImpalaServer* impala_server;
    ABORT_IF_ERROR(InProcessImpalaServer::StartWithEphemeralPorts(
        FLAGS_hostname, statestore->port(), &impala_server));
    IGNORE_LEAKING_OBJECT(impala_server);

    global_executor.reset(
        new ImpaladQueryExecutor(FLAGS_hostname, impala_server->GetBeeswaxPort()));
    ABORT_IF_ERROR(global_executor->Setup());
  }

  static void TearDownTestCase() {
    // Teardown before global destructors to avoid a race where JvmMetricCache is
    // destroyed before the last query is closed.
    global_executor.reset();
  }

 protected:

  virtual void SetUp() {
    SetUpExprTestBase(global_executor.get());
  }

  virtual void TearDown() { pool_.Clear(); }

  // Tests that DST of the given timezone ends at 3am
  void TestAusDSTEndingForEastTimeZone(const string& time_zone);
  void TestAusDSTEndingForCentralTimeZone(const string& time_zone);


  void TestLastDayFunction() {
    // Test common months (with and without time component).
    TestTimestampValue("last_day('2003-01-02 04:24:04.1579')",
      TimestampValue::ParseSimpleDateFormat("2003-01-31 00:00:00", 19));
    TestTimestampValue("last_day('2003-02-02')",
      TimestampValue::ParseSimpleDateFormat("2003-02-28 00:00:00"));
    TestTimestampValue("last_day('2003-03-02 03:21:12.0058')",
      TimestampValue::ParseSimpleDateFormat("2003-03-31 00:00:00"));
    TestTimestampValue("last_day('2003-04-02')",
      TimestampValue::ParseSimpleDateFormat("2003-04-30 00:00:00"));
    TestTimestampValue("last_day('2003-05-02')",
      TimestampValue::ParseSimpleDateFormat("2003-05-31 00:00:00"));
    TestTimestampValue("last_day('2003-06-02')",
      TimestampValue::ParseSimpleDateFormat("2003-06-30 00:00:00"));
    TestTimestampValue("last_day('2003-07-02 00:01:01.125')",
      TimestampValue::ParseSimpleDateFormat("2003-07-31 00:00:00"));
    TestTimestampValue("last_day('2003-08-02')",
      TimestampValue::ParseSimpleDateFormat("2003-08-31 00:00:00"));
    TestTimestampValue("last_day('2003-09-02')",
      TimestampValue::ParseSimpleDateFormat("2003-09-30 00:00:00"));
    TestTimestampValue("last_day('2003-10-02')",
      TimestampValue::ParseSimpleDateFormat("2003-10-31 00:00:00"));
    TestTimestampValue("last_day('2003-11-02 12:30:16')",
      TimestampValue::ParseSimpleDateFormat("2003-11-30 00:00:00"));
    TestTimestampValue("last_day('2003-12-02')",
      TimestampValue::ParseSimpleDateFormat("2003-12-31 00:00:00"));

    // Test leap years and special cases.
    TestTimestampValue("last_day('2004-02-13')",
      TimestampValue::ParseSimpleDateFormat("2004-02-29 00:00:00"));
    TestTimestampValue("last_day('2008-02-13')",
      TimestampValue::ParseSimpleDateFormat("2008-02-29 00:00:00"));
    TestTimestampValue("last_day('2000-02-13')",
      TimestampValue::ParseSimpleDateFormat("2000-02-29 00:00:00"));
    TestTimestampValue("last_day('1900-02-13')",
      TimestampValue::ParseSimpleDateFormat("1900-02-28 00:00:00"));
    TestTimestampValue("last_day('2100-02-13')",
      TimestampValue::ParseSimpleDateFormat("2100-02-28 00:00:00"));

    // Test corner cases.
    TestTimestampValue("last_day('1400-01-01 00:00:00')",
      TimestampValue::ParseSimpleDateFormat("1400-01-31 00:00:00"));
    TestTimestampValue("last_day('9999-12-31 23:59:59')",
      TimestampValue::ParseSimpleDateFormat("9999-12-31 00:00:00"));

    // Test invalid input.
    TestIsNull("last_day('12202010')", TYPE_TIMESTAMP);
    TestIsNull("last_day('')", TYPE_TIMESTAMP);
    TestIsNull("last_day(NULL)", TYPE_TIMESTAMP);
    TestIsNull("last_day('02-13-2014')", TYPE_TIMESTAMP);
    TestIsNull("last_day('00:00:00')", TYPE_TIMESTAMP);
  }

  void TestNextDayFunction() {
    // Sequential test cases
    TestTimestampValue("next_day('2016-05-01','Sunday')",
      TimestampValue::ParseSimpleDateFormat("2016-05-08 00:00:00", 19));
    TestTimestampValue("next_day('2016-05-01','Monday')",
      TimestampValue::ParseSimpleDateFormat("2016-05-02 00:00:00", 19));
    TestTimestampValue("next_day('2016-05-01','Tuesday')",
      TimestampValue::ParseSimpleDateFormat("2016-05-03 00:00:00", 19));
    TestTimestampValue("next_day('2016-05-01','Wednesday')",
      TimestampValue::ParseSimpleDateFormat("2016-05-04 00:00:00", 19));
    TestTimestampValue("next_day('2016-05-01','Thursday')",
      TimestampValue::ParseSimpleDateFormat("2016-05-05 00:00:00", 19));
    TestTimestampValue("next_day('2016-05-01','Friday')",
      TimestampValue::ParseSimpleDateFormat("2016-05-06 00:00:00", 19));
    TestTimestampValue("next_day('2016-05-01','Saturday')",
      TimestampValue::ParseSimpleDateFormat("2016-05-07 00:00:00", 19));

    // Random test cases
    TestTimestampValue("next_day('1910-01-18','SunDay')",
      TimestampValue::ParseSimpleDateFormat("1910-01-23 00:00:00", 19));
    TestTimestampValue("next_day('1916-06-05', 'SUN')",
      TimestampValue::ParseSimpleDateFormat("1916-06-11 00:00:00", 19));
    TestTimestampValue("next_day('1932-11-08','monday')",
      TimestampValue::ParseSimpleDateFormat("1932-11-14 00:00:00", 19));
    TestTimestampValue("next_day('1933-09-11','Mon')",
      TimestampValue::ParseSimpleDateFormat("1933-09-18 00:00:00", 19));
    TestTimestampValue("next_day('1934-03-21','TUeSday')",
      TimestampValue::ParseSimpleDateFormat("1934-03-27 00:00:00", 19));
    TestTimestampValue("next_day('1954-02-25','tuE')",
      TimestampValue::ParseSimpleDateFormat("1954-03-02 00:00:00", 19));
    TestTimestampValue("next_day('1965-04-18','WeDneSdaY')",
      TimestampValue::ParseSimpleDateFormat("1965-04-21 00:00:00", 19));
    TestTimestampValue("next_day('1966-08-29','wed')",
      TimestampValue::ParseSimpleDateFormat("1966-08-31 00:00:00", 19));
    TestTimestampValue("next_day('1968-07-23','tHurSday')",
      TimestampValue::ParseSimpleDateFormat("1968-07-25 00:00:00", 19));
    TestTimestampValue("next_day('1969-05-28','thu')",
      TimestampValue::ParseSimpleDateFormat("1969-05-29 00:00:00", 19));
    TestTimestampValue("next_day('1989-10-12','fRIDay')",
      TimestampValue::ParseSimpleDateFormat("1989-10-13 00:00:00", 19));
    TestTimestampValue("next_day('1973-10-02','frI')",
      TimestampValue::ParseSimpleDateFormat("1973-10-05 00:00:00", 19));
    TestTimestampValue("next_day('2000-02-29','saTUrDaY')",
      TimestampValue::ParseSimpleDateFormat("2000-03-04 00:00:00", 19));
    TestTimestampValue("next_day('2013-04-12','sat')",
      TimestampValue::ParseSimpleDateFormat("2013-04-13 00:00:00", 19));
    TestTimestampValue("next_day('2013-12-25','Saturday')",
      TimestampValue::ParseSimpleDateFormat("2013-12-28 00:00:00", 19));

    // Explicit timestamp conversion tests
    TestTimestampValue("next_day(to_timestamp('12-27-2008', 'MM-dd-yyyy'), 'moN')",
      TimestampValue::ParseSimpleDateFormat("2008-12-29 00:00:00", 19));
    TestTimestampValue("next_day(to_timestamp('2007-20-10 11:22', 'yyyy-dd-MM HH:mm'),\
      'TUeSdaY')", TimestampValue::ParseSimpleDateFormat("2007-10-23 11:22:00", 19));
    TestTimestampValue("next_day(to_timestamp('18-11-2070 09:12', 'dd-MM-yyyy HH:mm'),\
      'WeDneSdaY')", TimestampValue::ParseSimpleDateFormat("2070-11-19 09:12:00", 19));
    TestTimestampValue("next_day(to_timestamp('12-1900-05', 'dd-yyyy-MM'), 'tHurSday')",
      TimestampValue::ParseSimpleDateFormat("1900-05-17 00:00:00", 19));
    TestTimestampValue("next_day(to_timestamp('08-1987-21', 'MM-yyyy-dd'), 'FRIDAY')",
      TimestampValue::ParseSimpleDateFormat("1987-08-28 00:00:00", 19));
    TestTimestampValue("next_day(to_timestamp('02-04-2001', 'dd-MM-yyyy'), 'SAT')",
      TimestampValue::ParseSimpleDateFormat("2001-04-07 00:00:00", 19));
    TestTimestampValue("next_day(to_timestamp('1970-01-31 00:00:00',\
      'yyyy-MM-dd HH:mm:ss'), 'SunDay')",
      TimestampValue::ParseSimpleDateFormat("1970-02-01 00:00:00", 19));

    // Invalid input: unacceptable date parameter
    TestIsNull("next_day('12202010','Saturday')", TYPE_TIMESTAMP);
    TestIsNull("next_day('2011 02 11','thu')", TYPE_TIMESTAMP);
    TestIsNull("next_day('09-19-2012xyz','monDay')", TYPE_TIMESTAMP);
    TestIsNull("next_day('000000000000000','wed')", TYPE_TIMESTAMP);
    TestIsNull("next_day('hell world!','fRiDaY')", TYPE_TIMESTAMP);
    TestIsNull("next_day('t1c7t0c9','sunDAY')", TYPE_TIMESTAMP);
    TestIsNull("next_day(NULL ,'sunDAY')", TYPE_TIMESTAMP);

    // Invalid input: wrong weekday parameter
    for (const string& day: { "s", "SA", "satu", "not-a-day" }) {
      const string expr = "next_day('2013-12-25','" + day + "')";
      TestError(expr);
    }
    TestError("next_day('2013-12-25', NULL)");
    TestError("next_day(NULL, NULL)");
  }

// This macro adds a scoped trace to provide the line number of the caller upon failure.
#define EXPECT_BETWEEN(start, value, end) { \
    SCOPED_TRACE(""); \
    ExpectBetween(start, value, end); \
  }

  template <typename T>
  void ExpectBetween(T start, T value, T end) {
    EXPECT_LE(start, value);
    EXPECT_LE(value, end);
  }

  // Test conversions of Timestamps to and from string/int with values related to the
  // Unix epoch. The caller should set the current time zone before calling.
  // 'unix_time_at_local_epoch' should be the expected value of the Unix time when it
  // was 1970-01-01 in the current time zone. 'local_time_at_unix_epoch' should be the
  // local time at the Unix epoch (1970-01-01 UTC).
  void TestTimestampUnixEpochConversions(int64_t unix_time_at_local_epoch,
      string local_time_at_unix_epoch) {
    TestValue("unix_timestamp(cast('" + local_time_at_unix_epoch + "' as timestamp))",
        TYPE_BIGINT, 0);
    TestValue("unix_timestamp('" + local_time_at_unix_epoch + "')", TYPE_BIGINT, 0);
    TestValue("unix_timestamp('" + local_time_at_unix_epoch +
        "', 'yyyy-MM-dd HH:mm:ss')", TYPE_BIGINT, 0);
    TestValue("unix_timestamp('1970-01-01', 'yyyy-MM-dd')", TYPE_BIGINT,
        unix_time_at_local_epoch);
    TestValue("unix_timestamp('1970-01-01 10:10:10', 'yyyy-MM-dd')", TYPE_BIGINT,
        unix_time_at_local_epoch);
    TestValue("unix_timestamp('" + local_time_at_unix_epoch
        + " extra text', 'yyyy-MM-dd HH:mm:ss')", TYPE_BIGINT, 0);
    TestStringValue("cast(cast(0 as timestamp) as string)", local_time_at_unix_epoch);
    TestStringValue("cast(cast(0 as timestamp) as string)", local_time_at_unix_epoch);
    TestStringValue("from_unixtime(0)", local_time_at_unix_epoch);
    TestStringValue("from_unixtime(cast(0 as bigint))", local_time_at_unix_epoch);
    TestIsNull("from_unixtime(NULL)", TYPE_STRING);
    TestStringValue("from_unixtime(0, 'yyyy-MM-dd HH:mm:ss')",
        local_time_at_unix_epoch);
    TestStringValue("from_unixtime(cast(0 as bigint), 'yyyy-MM-dd HH:mm:ss')",
        local_time_at_unix_epoch);
    TestStringValue("from_unixtime(" + lexical_cast<string>(unix_time_at_local_epoch)
        + ", 'yyyy-MM-dd')", "1970-01-01");
    TestStringValue("from_unixtime(cast(" + lexical_cast<string>(unix_time_at_local_epoch)
        + " as bigint), 'yyyy-MM-dd')", "1970-01-01");
    TestIsNull("to_timestamp(NULL)", TYPE_TIMESTAMP);
    TestIsNull("to_timestamp(NULL, 'yyyy-MM-dd')", TYPE_TIMESTAMP);
    TestIsNull("from_timestamp(NULL, 'yyyy-MM-dd')", TYPE_STRING);
    TestStringValue("cast(to_timestamp(" + lexical_cast<string>(unix_time_at_local_epoch)
        + ") as string)", "1970-01-01 00:00:00");
    TestStringValue("cast(to_timestamp('1970-01-01 00:00:00', 'yyyy-MM-dd HH:mm:ss') \
        as string)", "1970-01-01 00:00:00");
    TestStringValue("from_timestamp('1970-01-01 00:00:00', 'yyyy-MM-dd HH:mm:ss')",
        "1970-01-01 00:00:00");
  }

  // Verify that output of 'query' has the same precision and scale as 'expected_type'.
  // 'query' is an expression, optionally followed by a from clause which is needed
  // for testing aggregate expressions.
  void TestDecimalResultType(const string& query, const ColumnType& expected_type) {
    // For the case with from clause, we need to generate the "typeof query" by first
    // extracting the select list.
    size_t from_offset = query.find("from");
    string typeof_query;
    if (from_offset != string::npos) {
      int query_len = query.length();
      typeof_query = "typeof(" + query.substr(0, from_offset) + ")" +
          query.substr(from_offset, query_len - from_offset);
    } else {
      typeof_query = "typeof(" + query + ")";
    }
    const string typeof_result = GetValue(typeof_query, ColumnType(TYPE_STRING));
    EXPECT_EQ(expected_type.DebugString(), typeof_result) << typeof_query;
  }

  // Decimals don't work with TestValue.
  // TODO: figure out what operators need to be implemented to work with EXPECT_EQ
  template<typename T>
  void TestDecimalValue(const string& query, const T& expected_result,
      const ColumnType& expected_type) {
    // Verify precision and scale of the expression match the expected type.
    TestDecimalResultType(query, expected_type);
    // Verify the expression result matches the expected result, for the given the
    // precision and scale.
    const string value = GetValue(query, expected_type);
    StringParser::ParseResult result;
    // These require that we've passed the correct type to StringToDecimal(), so these
    // results are valid only when TestDecimalResultType() succeeded.
    switch (expected_type.GetByteSize()) {
      case 4:
        EXPECT_EQ(expected_result.value(), StringParser::StringToDecimal<int32_t>(
            value.data(), value.size(), expected_type, false, &result).value()) << query;
        break;
      case 8:
        EXPECT_EQ(expected_result.value(), StringParser::StringToDecimal<int64_t>(
            value.data(), value.size(), expected_type, false, &result).value()) << query;
        break;
      case 16:
        EXPECT_EQ(expected_result.value(), StringParser::StringToDecimal<int128_t>(
            value.data(), value.size(), expected_type, false, &result).value()) << query;
        break;
      default:
        EXPECT_TRUE(false) << expected_type << " " << expected_type.GetByteSize();
    }
    EXPECT_EQ(result, StringParser::PARSE_SUCCESS);
  }

  void TestValidTimestampValue(const string& expr);

  // Create a Literal expression out of 'str'. Adds the returned literal to pool_.
  Literal* CreateLiteral(const ColumnType& type, const string& str);
  Literal* CreateLiteral(PrimitiveType type, const string& str);
};

// Tests whether the returned TimestampValue is valid.
// We use this function for tests where the expected value is unknown, e.g., now().
void TimestampExprTest::TestValidTimestampValue(const string& expr) {
  EXPECT_TRUE(
      ConvertValue<TimestampValue>(GetValue(expr, ColumnType(TYPE_TIMESTAMP))).HasDate());
}

// Test casting from/to Date.
TEST_P(TimestampExprTest, CastDateExprs) {
  // From Date to Date
  TestStringValue("cast(cast(cast('2012-01-01' as date) as date) as string)",
      "2012-01-01");
  TestStringValue("cast(cast(date '2012-01-01' as date) as string)",
      "2012-01-01");

  // Test casting of lazy date and/or time format string to Date.
  TestDateValue("cast('2001-1-2' as date)", DateValue(2001, 1, 2));
  TestDateValue("cast('2001-01-3' as date)", DateValue(2001, 1, 3));
  TestDateValue("cast('2001-1-21' as date)", DateValue(2001, 1, 21));
  TestDateValue("cast('        2001-01-9         ' as date)",
      DateValue(2001, 1, 9));

  TestError("cast('2001-6' as date)");
  TestError("cast('01-1-21' as date)");
  TestError("cast('10/feb/10' as date)");
  TestError("cast('1909-foo1-2bar' as date)");
  TestError("cast('1909/1-/2' as date)");

  // Test various ways of truncating a "lazy" format to produce an invalid date.
  TestError("cast('1909-10- ' as date)");
  TestError("cast('1909-10-' as date)");
  TestError("cast('1909-10' as date)");
  TestError("cast('1909-' as date)");
  TestError("cast('1909' as date)");

  // Test missing number from format.
  TestError("cast('1909--2' as date)");
  TestError("cast('-10-2' as date)");

  // Test duplicate separators - should fail with an error because not a valid format.
  TestError("cast('1909--10-2' as date)");
  TestError("cast('1909-10--2' as date)");

  // Test numbers with too many digits in date - should fail with an error because not a
  // valid date.
  TestError("cast('19097-10-2' as date)");
  TestError("cast('1909-107-2' as date)");
  TestError("cast('1909-10-277' as date)");

  // Test correctly formatted invalid dates
  // Invalid month of year
  TestError("cast('2000-13-29' as date)");
  // Invalid day of month
  TestError("cast('2000-04-31' as date)");
  // 1900 is not a leap year: 1900-02-28 is valid but 1900-02-29 isn't
  TestDateValue("cast('1900-02-28' as date)", DateValue(1900, 2, 28));
  TestError("cast('1900-02-29' as date)");
  // 2000 is a leap year: 2000-02-29 is valid but 2000-02-30 isn't
  TestDateValue("cast('2000-02-29' as date)", DateValue(2000, 2, 29));
  TestError("cast('2000-02-30' as date)");

  // Test whitespace trimming mechanism when cast from string to date.
  TestDateValue("cast('  \t\r\n      2001-01-09   \t\r\n     ' as date)",
      DateValue(2001, 1, 9));
  TestDateValue("cast('  \t\r\n      2001-1-9    \t\r\n    ' as date)",
      DateValue(2001, 1, 9));

  // whitespace-only strings should fail with an error.
  TestError("cast(' ' as date)");
  TestError("cast('\n' as date)");
  TestError("cast('\t' as date)");
  TestError("cast('  \t\r\n' as date)");

  // Test String <-> Date boundary cases.
  TestDateValue("cast('0001-01-01' as date)", DateValue(1, 1, 1));
  TestStringValue("cast(date '0001-01-01' as string)", "0001-01-01");
  TestDateValue("cast('9999-12-31' as date)", DateValue(9999, 12, 31));
  TestStringValue("cast(date '9999-12-31' as string)", "9999-12-31");
  TestError("cast('10000-01-01' as date)");
  TestError("cast(date '10000-01-01' as string)");

  // Decimal <-> Date conversions are not allowed.
  TestError("cast(cast('2000-01-01' as date) as decimal(12, 4))");
  TestError("cast(cast(16868.1230 as decimal(12,4)) as date)");
  TestError("cast(cast(null as date) as decimal(12, 4))");
  TestError("cast(cast(null as decimal(12, 4)) as date)");

  // Date <-> Int conversions are not allowed.
  TestError("cast(cast('2000-01-01' as date) as int)");
  TestError("cast(10957 as date)");
  TestError("cast(cast(null as date) as int)");
  TestError("cast(cast(null as int) as date)");

  // Date <-> Double conversions are not allowed.
  TestError("cast(cast('2000-01-01' as date) as double)");
  TestError("cast(123.0 as date)");
  TestError("cast(cast(null as date) as double)");
  TestError("cast(cast(null as double) as date)");

  // Bigint <-> date conversions are not allowed.
  TestError("cast(cast(1234567890 as bigint) as date)");
  TestError("cast(cast('2000-01-01' as date) as bigint)");
  TestError("cast(cast(null as bigint) as date)");
  TestError("cast(cast(null as date) as bigint)");

  // Date <-> Timestamp
  TestDateValue("cast(cast('2000-09-27 01:12:32.546' as timestamp) as date)",
      DateValue(2000, 9, 27));
  TestDateValue("cast(cast('1960-01-01 23:59:59' as timestamp) as date)",
      DateValue(1960, 1, 1));
  TestDateValue("cast(cast('1400-01-01 00:00:00' as timestamp) as date)",
      DateValue(1400, 1, 1));
  TestDateValue("cast(cast('9999-12-31 23:59:59.999999999' as timestamp) as date)",
      DateValue(9999, 12, 31));

  TestTimestampValue("cast(cast('2000-09-27' as date) as timestamp)",
      TimestampValue::ParseSimpleDateFormat("2000-09-27", 10));
  TestTimestampValue("cast(date '2000-09-27' as timestamp)",
      TimestampValue::ParseSimpleDateFormat("2000-09-27", 10));
  TestTimestampValue("cast(cast('9999-12-31' as date) as timestamp)",
      TimestampValue::ParseSimpleDateFormat("9999-12-31", 10));
  TestTimestampValue("cast(date '9999-12-31' as timestamp)",
      TimestampValue::ParseSimpleDateFormat("9999-12-31", 10));
  TestTimestampValue("cast(cast('1400-01-01' as date) as timestamp)",
      TimestampValue::ParseSimpleDateFormat("1400-01-01", 10));
  TestTimestampValue("cast(DATE '1400-01-01' as timestamp)",
      TimestampValue::ParseSimpleDateFormat("1400-01-01", 10));
  TestError("cast(cast('1399-12-31' as date) as timestamp)");
  TestError("cast(date '1399-12-31' as timestamp)");

  TestIsNull("cast(cast(null as timestamp) as date)", TYPE_DATE);
  TestIsNull("cast(cast(null as date) as timestamp)", TYPE_TIMESTAMP);
}

TEST_P(TimestampExprTest, MoscowTimezoneConversion) {
#pragma push_macro("UTC_TO_MSC")
#pragma push_macro("MSC_TO_UTC")
#define UTC_TO_MSC(X) ("cast(from_utc_timestamp('" X "', 'Europe/Moscow') as string)")
#define MSC_TO_UTC(X) ("cast(to_utc_timestamp('" X "', 'Europe/Moscow') as string)")

  // IMPALA-4209: Moscow time change in 2011.
  // Last DST change before the transition.
  TestStringValue(UTC_TO_MSC("2010-10-30 22:59:59"), "2010-10-31 02:59:59");
  TestStringValue(UTC_TO_MSC("2010-10-30 23:00:00"), "2010-10-31 02:00:00");
  TestStringValue(MSC_TO_UTC("2010-10-31 01:59:59"), "2010-10-30 21:59:59");
  // Since 2am to 2:59:59.999...am MSC happens twice, the ambiguity gets resolved by
  // returning null.
  TestIsNull(MSC_TO_UTC("2010-10-31 02:00:00"), TYPE_STRING);
  TestIsNull(MSC_TO_UTC("2010-10-31 02:59:59"), TYPE_STRING);
  TestStringValue(MSC_TO_UTC("2010-10-31 03:00:00"), "2010-10-31 00:00:00");

  // Moscow time transitions to UTC+4.
  TestStringValue(UTC_TO_MSC("2011-03-26 22:59:59"), "2011-03-27 01:59:59");
  TestStringValue(UTC_TO_MSC("2011-03-26 23:00:00"), "2011-03-27 03:00:00");
  TestStringValue(MSC_TO_UTC("2011-03-27 01:59:59"), "2011-03-26 22:59:59");
  // Since 2am to 2:59:59.999...am MSC happens twice, the ambiguity gets resolved by
  // returning null.
  TestIsNull(MSC_TO_UTC("2011-03-27 02:00:00"), TYPE_STRING);
  TestIsNull(MSC_TO_UTC("2011-03-27 02:59:59"), TYPE_STRING);
  TestStringValue(MSC_TO_UTC("2011-03-27 03:00:00"), "2011-03-26 23:00:00");

  // No more DST after the transition.
  TestStringValue(UTC_TO_MSC("2011-12-20 09:00:00"), "2011-12-20 13:00:00");
  TestStringValue(UTC_TO_MSC("2012-06-20 09:00:00"), "2012-06-20 13:00:00");
  TestStringValue(UTC_TO_MSC("2012-12-20 09:00:00"), "2012-12-20 13:00:00");
  TestStringValue(MSC_TO_UTC("2011-12-20 13:00:00"), "2011-12-20 09:00:00");
  TestStringValue(MSC_TO_UTC("2012-06-20 13:00:00"), "2012-06-20 09:00:00");
  TestStringValue(MSC_TO_UTC("2012-12-20 13:00:00"), "2012-12-20 09:00:00");

  // IMPALA-4546: Moscow time change in 2014.
  // UTC+4 is changed to UTC+3
  TestStringValue(UTC_TO_MSC("2014-10-25 21:59:59"), "2014-10-26 01:59:59");
  TestStringValue(UTC_TO_MSC("2014-10-25 22:00:00"), "2014-10-26 01:00:00");
  TestStringValue(UTC_TO_MSC("2014-10-25 23:00:00"), "2014-10-26 02:00:00");
  TestStringValue(MSC_TO_UTC("2014-10-26 00:59:59"), "2014-10-25 20:59:59");
  // Since 1am to 1:59:59.999...am MSC happens twice, the ambiguity gets resolved by
  // returning null.
  TestIsNull(MSC_TO_UTC("2014-10-26 01:00:00"), TYPE_STRING);
  TestIsNull(MSC_TO_UTC("2014-10-26 01:59:59"), TYPE_STRING);
  TestStringValue(MSC_TO_UTC("2014-10-26 02:00:00"), "2014-10-25 23:00:00");

  // Still no DST after the transition.
  TestStringValue(UTC_TO_MSC("2014-12-20 09:00:00"), "2014-12-20 12:00:00");
  TestStringValue(UTC_TO_MSC("2015-06-20 09:00:00"), "2015-06-20 12:00:00");
  TestStringValue(UTC_TO_MSC("2015-12-20 09:00:00"), "2015-12-20 12:00:00");
  TestStringValue(MSC_TO_UTC("2014-12-20 12:00:00"), "2014-12-20 09:00:00");
  TestStringValue(MSC_TO_UTC("2015-06-20 12:00:00"), "2015-06-20 09:00:00");
  TestStringValue(MSC_TO_UTC("2015-12-20 12:00:00"), "2015-12-20 09:00:00");

  // Timestamp conversions of "dateless" times should return null (and not crash,
  // see IMPALA-5983).
  TestIsNull(UTC_TO_MSC("10:00:00"), TYPE_STRING);
  TestIsNull(MSC_TO_UTC("10:00:00"), TYPE_STRING);

#pragma pop_macro("MSC_TO_UTC")
#pragma pop_macro("UTC_TO_MSC")
}

void TimestampExprTest::TestAusDSTEndingForEastTimeZone(const string& time_zone) {
  // Timestamps between 02:00:00 and 02:59:59 inclusive on the ending day of DST are
  // ambiguous, hence excpecting NULL for timestamps in that range. Expect a UTC adjusted
  // timestamp otherwise.
  TestStringValue("cast(to_utc_timestamp('2018-04-01 01:59:59', '" + time_zone + "') "
      "as string)", "2018-03-31 14:59:59");
  TestStringValue("cast(to_utc_timestamp('2018-04-01 02:00:00', '" + time_zone + "') "
      "as string)", "NULL");
  TestStringValue("cast(to_utc_timestamp('2018-04-01 02:59:59', '" + time_zone + "') "
      "as string)", "NULL");
  TestStringValue("cast(to_utc_timestamp('2018-04-01 03:00:00', '" + time_zone + "') "
      "as string)", "2018-03-31 17:00:00");
}

void TimestampExprTest::TestAusDSTEndingForCentralTimeZone(const string& time_zone) {
  TestStringValue("cast(to_utc_timestamp('2018-04-01 01:59:59', '" + time_zone + "') "
      "as string)", "2018-03-31 15:29:59");
  TestStringValue("cast(to_utc_timestamp('2018-04-01 02:00:00', '" + time_zone + "') "
      "as string)", "NULL");
  TestStringValue("cast(to_utc_timestamp('2018-04-01 02:59:59', '" + time_zone + "') "
      "as string)", "NULL");
  TestStringValue("cast(to_utc_timestamp('2018-04-01 03:00:00', '" + time_zone + "') "
      "as string)", "2018-03-31 17:30:00");
}

// IMPALA-6699: Fix DST end time for Australian time-zones
TEST_P(TimestampExprTest, AusDSTEndingTests) {
  TestAusDSTEndingForEastTimeZone("AET");
  TestAusDSTEndingForEastTimeZone("Australia/ACT");
  TestAusDSTEndingForCentralTimeZone("Australia/Adelaide");
  TestAusDSTEndingForCentralTimeZone("Australia/Broken_Hill");
  TestAusDSTEndingForEastTimeZone("Australia/Canberra");
  TestAusDSTEndingForEastTimeZone("Australia/Currie");
  TestAusDSTEndingForEastTimeZone("Australia/Hobart");
  TestAusDSTEndingForEastTimeZone("Australia/Melbourne");
  TestAusDSTEndingForEastTimeZone("Australia/NSW");
  TestAusDSTEndingForCentralTimeZone("Australia/South");
  TestAusDSTEndingForEastTimeZone("Australia/Sydney");
  TestAusDSTEndingForEastTimeZone("Australia/Tasmania");
  TestAusDSTEndingForEastTimeZone("Australia/Victoria");
  TestAusDSTEndingForCentralTimeZone("Australia/Yancowinna");
}

TEST_P(TimestampExprTest, TimestampFunctions) {
  // Regression for IMPALA-1105
  TestIsNull("cast(cast('NOTATIMESTAMP' as timestamp) as string)", TYPE_STRING);

  TestStringValue("cast(cast('2012-01-01 09:10:11.123456789' as timestamp) as string)",
      "2012-01-01 09:10:11.123456789");
  // Add/sub years.
  TestStringValue("cast(date_add(cast('2012-01-01 09:10:11.123456789' "
      "as timestamp), interval 10 years) as string)",
      "2022-01-01 09:10:11.123456789");
  TestStringValue("cast(date_sub(cast('2012-01-01 09:10:11.123456789' "
      "as timestamp), interval 10 years) as string)",
      "2002-01-01 09:10:11.123456789");
  TestStringValue("cast(date_add(cast('2012-01-01 09:10:11.123456789' "
      "as timestamp), interval cast(10 as bigint) years) as string)",
      "2022-01-01 09:10:11.123456789");
  TestStringValue("cast(date_sub(cast('2012-01-01 09:10:11.123456789' "
      "as timestamp), interval cast(10 as bigint) years) as string)",
      "2002-01-01 09:10:11.123456789");
  // These return NULL because the resulting year is out of range.
  TestIsNull(
      "CAST('2005-10-11 00:00:00' AS TIMESTAMP) - INTERVAL 718 YEAR", TYPE_TIMESTAMP);
  TestIsNull(
      "CAST('2005-10-11 00:00:00' AS TIMESTAMP) + INTERVAL -718 YEAR", TYPE_TIMESTAMP);
  TestIsNull(
      "CAST('2005-10-11 00:00:00' AS TIMESTAMP) + INTERVAL 9718 YEAR", TYPE_TIMESTAMP);
  TestIsNull(
      "CAST('2005-10-11 00:00:00' AS TIMESTAMP) - INTERVAL -9718 YEAR", TYPE_TIMESTAMP);
  TestIsNull(
      "CAST('1405-01-11 00:00:00' AS TIMESTAMP) + INTERVAL -61 MONTH", TYPE_TIMESTAMP);
  TestIsNull(
      "CAST('1405-01-11 00:00:00' AS TIMESTAMP) - INTERVAL 61 MONTH", TYPE_TIMESTAMP);
  TestIsNull(
      "CAST('9995-12-11 00:00:00' AS TIMESTAMP) + INTERVAL 61 MONTH", TYPE_TIMESTAMP);
  TestIsNull(
      "CAST('9995-12-11 00:00:00' AS TIMESTAMP) - INTERVAL -61 MONTH", TYPE_TIMESTAMP);
  TestIsNull(
      "CAST('9999-12-31 21:00:00' AS TIMESTAMP) + INTERVAL 1000 DAYS", TYPE_TIMESTAMP);
  TestIsNull(
      "CAST('1400-01-01 00:12:00' AS TIMESTAMP) - INTERVAL 1 DAYS", TYPE_TIMESTAMP);
  TestIsNull(
      "CAST('9999-12-31 21:00:00' AS TIMESTAMP) + INTERVAL 10000 HOURS", TYPE_TIMESTAMP);
  TestIsNull(
      "CAST('1400-01-01 00:12:00' AS TIMESTAMP) - INTERVAL 24 HOURS", TYPE_TIMESTAMP);
  TestIsNull("CAST('9999-12-31 21:00:00' AS TIMESTAMP) + INTERVAL 1000000 MINUTES",
      TYPE_TIMESTAMP);
  TestIsNull(
      "CAST('1400-01-01 00:12:00' AS TIMESTAMP) - INTERVAL 13 MINUTES", TYPE_TIMESTAMP);
  TestIsNull("CAST('9999-12-31 23:59:59' AS TIMESTAMP) + INTERVAL 1 SECONDS",
      TYPE_TIMESTAMP);
  TestIsNull(
      "CAST('1400-01-01 00:00:00' AS TIMESTAMP) - INTERVAL 1 SECONDS", TYPE_TIMESTAMP);
  TestIsNull(
      "CAST('9999-12-31 21:00:00' AS TIMESTAMP) + INTERVAL 100000000000 MILLISECONDS",
      TYPE_TIMESTAMP);
  TestIsNull("CAST('1400-01-01 00:00:00' AS TIMESTAMP) - INTERVAL 1 MILLISECONDS",
      TYPE_TIMESTAMP);
  TestIsNull(
      "CAST('9999-12-31 21:00:00' AS TIMESTAMP) + INTERVAL 100000000000000 MICROSECONDS",
      TYPE_TIMESTAMP);
  TestIsNull("CAST('1400-01-01 00:00:00' AS TIMESTAMP) - INTERVAL 1 MICROSECONDS",
      TYPE_TIMESTAMP);
  TestIsNull("CAST('9999-12-31 21:00:00' AS TIMESTAMP) + INTERVAL 100000000000000000 "
      "NANOSECONDS", TYPE_TIMESTAMP);
  TestIsNull("CAST('1400-01-01 00:00:00' AS TIMESTAMP) - INTERVAL 1 NANOSECONDS",
      TYPE_TIMESTAMP);
  // Add/sub months.
  TestStringValue("cast(date_add(cast('2012-01-01 09:10:11.123456789' "
      "as timestamp), interval 13 months) as string)",
      "2013-02-01 09:10:11.123456789");
  TestStringValue("cast(date_sub(cast('2013-02-01 09:10:11.123456789' "
      "as timestamp), interval 13 months) as string)",
      "2012-01-01 09:10:11.123456789");
  TestStringValue("cast(date_add(cast('2012-01-31 09:10:11.123456789' "
      "as timestamp), interval cast(1 as bigint) month) as string)",
      "2012-02-29 09:10:11.123456789");
  TestStringValue("cast(date_sub(cast('2012-02-29 09:10:11.123456789' "
      "as timestamp), interval cast(1 as bigint) month) as string)",
      "2012-01-29 09:10:11.123456789");
  TestStringValue("cast(add_months(cast('1405-01-29 09:10:11.123456789' "
      "as timestamp), -60) as string)",
      "1400-01-29 09:10:11.123456789");
  TestStringValue("cast(add_months(cast('9995-01-29 09:10:11.123456789' "
      "as timestamp), 59) as string)",
      "9999-12-29 09:10:11.123456789");
  // Add/sub weeks.
  TestStringValue("cast(date_add(cast('2012-01-01 09:10:11.123456789' "
      "as timestamp), interval 2 weeks) as string)",
      "2012-01-15 09:10:11.123456789");
  TestStringValue("cast(date_sub(cast('2012-01-15 09:10:11.123456789' "
      "as timestamp), interval 2 weeks) as string)",
      "2012-01-01 09:10:11.123456789");
  TestStringValue("cast(date_add(cast('2012-01-01 09:10:11.123456789' "
      "as timestamp), interval cast(53 as bigint) weeks) as string)",
      "2013-01-06 09:10:11.123456789");
  TestStringValue("cast(date_sub(cast('2013-01-06 09:10:11.123456789' "
      "as timestamp), interval cast(53 as bigint) weeks) as string)",
      "2012-01-01 09:10:11.123456789");
  // Add/sub days.
  TestStringValue("cast(date_add(cast('2012-01-01 09:10:11.123456789' "
      "as timestamp), interval 10 days) as string)",
      "2012-01-11 09:10:11.123456789");
  TestStringValue("cast(date_sub(cast('2012-01-01 09:10:11.123456789' "
      "as timestamp), interval 10 days) as string)",
      "2011-12-22 09:10:11.123456789");
  TestStringValue("cast(date_add(cast('2011-12-22 09:10:11.12345678' "
      "as timestamp), interval cast(10 as bigint) days) as string)",
      "2012-01-01 09:10:11.123456780");
  TestStringValue("cast(date_sub(cast('2011-12-22 09:10:11.12345678' "
      "as timestamp), interval cast(365 as bigint) days) as string)",
      "2010-12-22 09:10:11.123456780");
  // Add/sub days (HIVE's date_add/sub variant).
  TestStringValue("cast(date_add(cast('2012-01-01 09:10:11.123456789' "
      "as timestamp), 10) as string)",
      "2012-01-11 09:10:11.123456789");
  TestStringValue(
      "cast(date_sub(cast('2012-01-01 09:10:11.123456789' as timestamp), 10) as string)",
      "2011-12-22 09:10:11.123456789");
  TestStringValue(
      "cast(date_add(cast('2011-12-22 09:10:11.12345678' as timestamp),"
      "cast(10 as bigint)) as string)", "2012-01-01 09:10:11.123456780");
  TestStringValue(
      "cast(date_sub(cast('2011-12-22 09:10:11.12345678' as timestamp),"
      "cast(365 as bigint)) as string)", "2010-12-22 09:10:11.123456780");
  // Add/sub hours.
  TestStringValue("cast(date_add(cast('2012-01-01 00:00:00.123456789' "
      "as timestamp), interval 25 hours) as string)",
      "2012-01-02 01:00:00.123456789");
  TestStringValue("cast(date_sub(cast('2012-01-02 01:00:00.123456789' "
      "as timestamp), interval 25 hours) as string)",
      "2012-01-01 00:00:00.123456789");
  TestStringValue("cast(date_add(cast('2012-01-01 00:00:00.123456789' "
      "as timestamp), interval cast(25 as bigint) hours) as string)",
      "2012-01-02 01:00:00.123456789");
  TestStringValue("cast(date_sub(cast('2012-01-02 01:00:00.123456789' "
      "as timestamp), interval cast(25 as bigint) hours) as string)",
      "2012-01-01 00:00:00.123456789");
  // Add/sub minutes.
  TestStringValue("cast(date_add(cast('2012-01-01 00:00:00.123456789' "
      "as timestamp), interval 1533 minutes) as string)",
      "2012-01-02 01:33:00.123456789");
  TestStringValue("cast(date_sub(cast('2012-01-02 01:33:00.123456789' "
      "as timestamp), interval 1533 minutes) as string)",
      "2012-01-01 00:00:00.123456789");
  TestStringValue("cast(date_add(cast('2012-01-01 00:00:00.123456789' "
      "as timestamp), interval cast(1533 as bigint) minutes) as string)",
      "2012-01-02 01:33:00.123456789");
  TestStringValue("cast(date_sub(cast('2012-01-02 01:33:00.123456789' "
      "as timestamp), interval cast(1533 as bigint) minutes) as string)",
      "2012-01-01 00:00:00.123456789");
  // Add/sub seconds.
  TestStringValue("cast(date_add(cast('2012-01-01 00:00:00.123456789' "
      "as timestamp), interval 90033 seconds) as string)",
      "2012-01-02 01:00:33.123456789");
  TestStringValue("cast(date_sub(cast('2012-01-02 01:00:33.123456789' "
      "as timestamp), interval 90033 seconds) as string)",
      "2012-01-01 00:00:00.123456789");
  TestStringValue("cast(date_add(cast('2012-01-01 00:00:00.123456789' "
      "as timestamp), interval cast(90033 as bigint) seconds) as string)",
      "2012-01-02 01:00:33.123456789");
  TestStringValue("cast(date_sub(cast('2012-01-02 01:00:33.123456789' "
      "as timestamp), interval cast(90033 as bigint) seconds) as string)",
      "2012-01-01 00:00:00.123456789");
  // Add/sub milliseconds.
  TestStringValue("cast(date_add(cast('2012-01-01 00:00:00.000000001' "
      "as timestamp), interval 90000033 milliseconds) as string)",
      "2012-01-02 01:00:00.033000001");
  TestStringValue("cast(date_sub(cast('2012-01-02 01:00:00.033000001' "
      "as timestamp), interval 90000033 milliseconds) as string)",
      "2012-01-01 00:00:00.000000001");
  TestStringValue("cast(date_add(cast('2012-01-01 00:00:00.000000001' "
      "as timestamp), interval cast(90000033 as bigint) milliseconds) as string)",
      "2012-01-02 01:00:00.033000001");
  TestStringValue("cast(date_sub(cast('2012-01-02 01:00:00.033000001' "
      "as timestamp), interval cast(90000033 as bigint) milliseconds) as string)",
      "2012-01-01 00:00:00.000000001");
  TestStringValue("cast(cast(0 as timestamp) + interval -10000000000000 milliseconds "
      "as string)", "1653-02-10 06:13:20");
  // Add/sub microseconds.
  TestStringValue("cast(date_add(cast('2012-01-01 00:00:00.000000001' "
      "as timestamp), interval 1033 microseconds) as string)",
      "2012-01-01 00:00:00.001033001");
  TestStringValue("cast(date_sub(cast('2012-01-01 00:00:00.001033001' "
      "as timestamp), interval 1033 microseconds) as string)",
      "2012-01-01 00:00:00.000000001");
  TestStringValue("cast(date_add(cast('2012-01-01 00:00:00.000000001' "
      "as timestamp), interval cast(1033 as bigint) microseconds) as string)",
      "2012-01-01 00:00:00.001033001");
  TestStringValue("cast(date_sub(cast('2012-01-01 00:00:00.001033001' "
      "as timestamp), interval cast(1033 as bigint) microseconds) as string)",
      "2012-01-01 00:00:00.000000001");
  // Add/sub nanoseconds.
  TestStringValue("cast(date_add(cast('2012-01-01 00:00:00.000000001' "
      "as timestamp), interval 1033 nanoseconds) as string)",
      "2012-01-01 00:00:00.000001034");
  TestStringValue("cast(date_sub(cast('2012-01-01 00:00:00.000001034' "
      "as timestamp), interval 1033 nanoseconds) as string)",
      "2012-01-01 00:00:00.000000001");
  TestStringValue("cast(date_add(cast('2012-01-01 00:00:00.000000001' "
      "as timestamp), interval cast(1033 as bigint) nanoseconds) as string)",
      "2012-01-01 00:00:00.000001034");
  TestStringValue("cast(date_sub(cast('2012-01-01 00:00:00.000001034' "
      "as timestamp), interval cast(1033 as bigint) nanoseconds) as string)",
      "2012-01-01 00:00:00.000000001");

  // NULL arguments.
  TestIsNull("date_add(NULL, interval 10 years)", TYPE_TIMESTAMP);
  TestIsNull("date_add(cast('2012-01-01 09:10:11.123456789' as timestamp),"
      "interval NULL years)", TYPE_TIMESTAMP);
  TestIsNull("date_add(NULL, interval NULL years)", TYPE_TIMESTAMP);
  TestIsNull("date_sub(NULL, interval 10 years)", TYPE_TIMESTAMP);
  TestIsNull("date_sub(cast('2012-01-01 09:10:11.123456789' as timestamp),"
      "interval NULL years)", TYPE_TIMESTAMP);
  TestIsNull("date_sub(NULL, interval NULL years)", TYPE_TIMESTAMP);

  // ADD_MONTHS() differs from '... + INTERVAL 1 MONTH'. The function version treats
  // an input value that falls on the last day of the month specially -- the result will
  // also always be the last day of the month.
  TestStringValue("cast(add_months(cast('2000-02-29' as timestamp), 1) as string)",
      "2000-03-31 00:00:00");
  TestStringValue("cast(cast('2000-02-29' as timestamp) + interval 1 month as string)",
      "2000-03-29 00:00:00");
  TestStringValue("cast(add_months(cast('1999-02-28' as timestamp), 12) as string)",
      "2000-02-29 00:00:00");
  TestStringValue("cast(cast('1999-02-28' as timestamp) + interval 12 month as string)",
      "2000-02-28 00:00:00");

  // Try a few cases in which ADD_MONTHS() and INTERVAL produce the same result.
  TestStringValue("cast(months_sub(cast('2000-03-31' as timestamp), 1) as string)",
      "2000-02-29 00:00:00");
  TestStringValue("cast(cast('2000-03-31' as timestamp) - interval 1 month as string)",
      "2000-02-29 00:00:00");
  TestStringValue("cast(months_add(cast('2000-03-31' as timestamp), -2) as string)",
      "2000-01-31 00:00:00");
  TestStringValue("cast(cast('2000-03-31' as timestamp) + interval -2 month as string)",
      "2000-01-31 00:00:00");

  // Test add/sub behavior with edge case time interval values.
  string max_int = lexical_cast<string>(numeric_limits<int32_t>::max());
  string max_long = lexical_cast<string>(numeric_limits<int64_t>::max());
  typedef map<string, int64_t> MaxIntervals;
  MaxIntervals max_intervals;
  max_intervals["years"] = TimestampFunctions::MAX_YEAR_INTERVAL;
  max_intervals["months"] = TimestampFunctions::MAX_MONTH_INTERVAL;
  max_intervals["weeks"] = TimestampFunctions::MAX_WEEK_INTERVAL;
  max_intervals["days"] = TimestampFunctions::MAX_DAY_INTERVAL;
  max_intervals["hours"] = TimestampFunctions::MAX_HOUR_INTERVAL;
  max_intervals["minutes"] = TimestampFunctions::MAX_MINUTE_INTERVAL;
  max_intervals["seconds"] = TimestampFunctions::MAX_SEC_INTERVAL;
  max_intervals["microseconds"] = TimestampFunctions::MAX_MILLI_INTERVAL;
  max_intervals["nanoseconds"] = numeric_limits<int64_t>::max();
  string year_5000 = "cast('5000-01-01' as timestamp)";
  string gt_year_5000 = "cast('5000-01-01 00:00:00.1' as timestamp)";
  string lt_year_5000 = "cast('4999-12-31 23:59:59.9' as timestamp)";
  for (MaxIntervals::iterator it = max_intervals.begin(); it != max_intervals.end();
      ++it) {
    const string& unit = it->first;
    const string& lt_max_interval =
        lexical_cast<string>(static_cast<int64_t>(0.9 * it->second));
    // Test that pushing a value beyond the max/min values results in a NULL.
    TestIsNull(StrCat(unit, "_add(cast('9999-12-31 23:59:59' as timestamp), "
       , lt_max_interval, ")"), TYPE_TIMESTAMP);
    TestIsNull(StrCat(unit, "_sub(cast('1400-01-01 00:00:00' as timestamp), "
       , lt_max_interval, ")"), TYPE_TIMESTAMP);

    // Same as above but with edge case values of max int/long.
    TestIsNull(StrCat(unit,"_add(years_add(cast('9999-12-31 23:59:59' as timestamp), 1), "
       , max_int, ")"), TYPE_TIMESTAMP);
    TestIsNull(StrCat(unit, "_sub(cast('1400-01-01 00:00:00' as timestamp), ",
        max_int, ")"), TYPE_TIMESTAMP);
    TestIsNull(StrCat(unit,"_add(years_add(cast('9999-12-31 23:59:59' as timestamp), 1), "
       , max_long, ")"), TYPE_TIMESTAMP);
    TestIsNull(StrCat(unit, "_sub(cast('1400-01-01 00:00:00' as timestamp), ", max_long
       , ")"), TYPE_TIMESTAMP);

    // Test that adding/subtracting a value slightly less than the MAX_*_INTERVAL
    // can result in a non-NULL.
    TestIsNotNull(StrCat(unit, "_add(cast('1400-01-01 00:00:00' as timestamp), "
       , lt_max_interval, ")"), TYPE_TIMESTAMP);
    TestIsNotNull(StrCat(unit, "_sub(cast('9999-12-31 23:59:59' as timestamp), "
       , lt_max_interval, ")"), TYPE_TIMESTAMP);

    // Test that adding/subtracting either results in NULL or a value more/less than
    // the original value.
    TestValue(StrCat("isnull(", unit, "_add(", year_5000, ", ", max_int
       , "), ", gt_year_5000, ") > ", year_5000), TYPE_BOOLEAN, true);
    TestValue(StrCat("isnull(", unit, "_sub(", year_5000, ", ", max_int
       , "), ", lt_year_5000, ") < ", year_5000), TYPE_BOOLEAN, true);
    TestValue(StrCat("isnull(", unit, "_add(", year_5000, ", ", max_long
       , "), ", gt_year_5000, ") > ", year_5000), TYPE_BOOLEAN, true);
    TestValue(StrCat("isnull(", unit, "_sub(", year_5000, ", ", max_long
       , "), ", lt_year_5000, ") < ", year_5000), TYPE_BOOLEAN, true);
  }

  // Regression test for IMPALA-2260, a seemingly non-edge case value results in an
  // overflow causing the 9999999 interval to become negative.
  for (MaxIntervals::iterator it = max_intervals.begin(); it != max_intervals.end();
      ++it) {
    const string& unit = it->first;

    // The static max interval definitions aren't exact so (max interval - 1) may still
    // produce a NULL. The static max interval definitions are within an order of
    // magnitude of the real max values so testing can start at max / 10.
    for (int64_t interval = it->second / 10; interval > 0; interval /= 10) {
      const string& sql_interval = lexical_cast<string>(interval);
      TestIsNotNull(StrCat(unit, "_add(cast('1400-01-01 00:00:00' as timestamp), "
         , sql_interval, ")"), TYPE_TIMESTAMP);
      TestIsNotNull(StrCat(unit, "_sub(cast('9999-12-31 23:59:59' as timestamp), "
         , sql_interval, ")"), TYPE_TIMESTAMP);
    }
  }

  // Test Unix epoch conversions.
  TestTimestampUnixEpochConversions(0, "1970-01-01 00:00:00");

  // Regression tests for IMPALA-1579, Unix times should be BIGINTs instead of INTs.
  TestValue("unix_timestamp('2038-01-19 03:14:07')", TYPE_BIGINT, 2147483647);
  TestValue("unix_timestamp('2038-01-19 03:14:08')", TYPE_BIGINT, 2147483648);
  TestValue("unix_timestamp('2038/01/19 03:14:08', 'yyyy/MM/dd HH:mm:ss')", TYPE_BIGINT,
      2147483648);
  TestValue("unix_timestamp(cast('2038-01-19 03:14:08' as timestamp))", TYPE_BIGINT,
      2147483648);


  // Test Unix epoch conversions again but now converting into local timestamp values.
  {
    ScopedTimeZoneOverride time_zone("PST8PDT");
    ScopedExecOption use_local(executor_,
        "USE_LOCAL_TZ_FOR_UNIX_TIMESTAMP_CONVERSIONS=1");
    // Determine what the local time would have been when it was 1970-01-01 GMT
    ptime local_time_at_epoch = c_local_adjustor<ptime>::utc_to_local(from_time_t(0));
    // ... and as an Impala compatible string.
    string local_time_at_epoch_as_str = to_iso_extended_string(local_time_at_epoch.date())
        + " " + to_simple_string(local_time_at_epoch.time_of_day());
    // Determine what the Unix timestamp would have been when it was 1970-01-01 in the
    // local time zone.
    int64_t unix_time_at_local_epoch =
        (from_time_t(0) - local_time_at_epoch).total_seconds();
    TestTimestampUnixEpochConversions(unix_time_at_local_epoch,
        local_time_at_epoch_as_str);

    // Check that daylight savings calculation is done.
    {
      ScopedTimeZoneOverride time_zone("PST8PDT");
      TestValue("unix_timestamp('2015-01-01')", TYPE_BIGINT, 1420099200);   // PST applies
      TestValue("unix_timestamp('2015-07-01')", TYPE_BIGINT, 1435734000);   // PDT applies
    }
    {
      ScopedTimeZoneOverride time_zone("EST5EDT");
      TestValue("unix_timestamp('2015-01-01')", TYPE_BIGINT, 1420088400);   // EST applies
      TestValue("unix_timestamp('2015-07-01')", TYPE_BIGINT, 1435723200);   // EDT applies
    }
  }

  TestIsNull("from_unixtime(NULL, 'yyyy-MM-dd')", TYPE_STRING);
  TestIsNull("from_unixtime(999999999999999)", TYPE_STRING);
  TestIsNull("from_unixtime(999999999999999, 'yyyy-MM-dd')", TYPE_STRING);
  TestStringValue("from_unixtime(unix_timestamp('1999-01-01 10:10:10'), \
      'yyyy-MM-dd')", "1999-01-01");
  TestStringValue("from_unixtime(unix_timestamp('1999-01-01 10:10:10'), \
      'yyyy-MM-dd HH:mm:ss')", "1999-01-01 10:10:10");
  TestStringValue("from_unixtime(unix_timestamp('1999-01-01 10:10:10') + (60*60*24), \
        'yyyy-MM-dd')", "1999-01-02");
  TestStringValue("from_unixtime(unix_timestamp('1999-01-01 10:10:10') + 10, \
        'yyyy-MM-dd HH:mm:ss')", "1999-01-01 10:10:20");
  TestStringValue("from_timestamp(cast('1999-01-01 10:10:10' as timestamp), \
      'yyyy-MM-dd')", "1999-01-01");
  TestStringValue("from_timestamp(cast('1999-01-01 10:10:10' as timestamp), \
      'yyyy-MM-dd HH:mm:ss')", "1999-01-01 10:10:10");
  TestStringValue("from_timestamp(to_timestamp(unix_timestamp('1999-01-01 10:10:10') \
      + 60*60*24), 'yyyy-MM-dd')", "1999-01-02");
  TestStringValue("from_timestamp(to_timestamp(unix_timestamp('1999-01-01 10:10:10') \
      + 10), 'yyyy-MM-dd HH:mm:ss')", "1999-01-01 10:10:20");
  TestStringValue("from_timestamp(cast('2999-05-05 11:11:11' as timestamp), 'HH:mm:ss')",
      "11:11:11");
  TestValue("cast('2011-12-22 09:10:11.123456789' as timestamp) > \
      cast('2011-12-22 09:10:11.12345678' as timestamp)", TYPE_BOOLEAN, true);
  TestValue("cast('2011-12-22 08:10:11.123456789' as timestamp) > \
      cast('2011-12-22 09:10:11.12345678' as timestamp)", TYPE_BOOLEAN, false);
  TestValue("cast('2011-12-22 09:10:11.000000' as timestamp) = \
      cast('2011-12-22 09:10:11' as timestamp)", TYPE_BOOLEAN, true);
  TestValue("year(cast('2011-12-22 09:10:11.000000' as timestamp))", TYPE_INT, 2011);
  TestValue("quarter(cast('2011-12-22 09:10:11.000000' as timestamp))", TYPE_INT, 4);
  TestValue("month(cast('2011-12-22 09:10:11.000000' as timestamp))", TYPE_INT, 12);
  TestValue("dayofmonth(cast('2011-12-22 09:10:11.000000' as timestamp))", TYPE_INT, 22);
  TestValue("day(cast('2011-12-22 09:10:11.000000' as timestamp))", TYPE_INT, 22);
  TestValue("dayofyear(cast('2011-12-22 09:10:11.000000' as timestamp))", TYPE_INT, 356);
  TestValue("weekofyear(cast('2011-12-22 09:10:11.000000' as timestamp))", TYPE_INT, 51);
  TestValue("week(cast('2011-12-22 09:10:11.000000' as timestamp))", TYPE_INT, 51);
  TestValue("dayofweek(cast('2011-12-18 09:10:11.000000' as timestamp))", TYPE_INT, 1);
  TestValue("dayofweek(cast('2011-12-22 09:10:11.000000' as timestamp))", TYPE_INT, 5);
  TestValue("dayofweek(cast('2011-12-24 09:10:11.000000' as timestamp))", TYPE_INT, 7);
  TestValue("hour(cast('2011-12-22 09:10:11.000000' as timestamp))", TYPE_INT, 9);
  TestValue("minute(cast('2011-12-22 09:10:11.000000' as timestamp))", TYPE_INT, 10);
  TestValue("second(cast('2011-12-22 09:10:11.000000' as timestamp))", TYPE_INT, 11);
  TestValue("millisecond(cast('2011-12-22 09:10:11.123456' as timestamp))",
      TYPE_INT, 123);
  TestValue("millisecond(cast('2011-12-22 09:10:11' as timestamp))", TYPE_INT, 0);
  TestValue("millisecond(cast('2011-12-22' as timestamp))", TYPE_INT, 0);
  TestValue("year(cast('2011-12-22' as timestamp))", TYPE_INT, 2011);
  TestValue("quarter(cast('2011-12-22' as timestamp))", TYPE_INT, 4);
  TestValue("month(cast('2011-12-22' as timestamp))", TYPE_INT, 12);
  TestValue("dayofmonth(cast('2011-12-22' as timestamp))", TYPE_INT, 22);
  TestValue("day(cast('2011-12-22' as timestamp))", TYPE_INT, 22);
  TestValue("dayofyear(cast('2011-12-22' as timestamp))", TYPE_INT, 356);
  TestValue("weekofyear(cast('2011-12-22' as timestamp))", TYPE_INT, 51);
  TestValue("week(cast('2011-12-22' as timestamp))", TYPE_INT, 51);
  TestValue("dayofweek(cast('2011-12-18' as timestamp))", TYPE_INT, 1);
  TestValue("dayofweek(cast('2011-12-22' as timestamp))", TYPE_INT, 5);
  TestValue("dayofweek(cast('2011-12-24' as timestamp))", TYPE_INT, 7);
  TestStringValue(
      "to_date(cast('2011-12-22 09:10:11.12345678' as timestamp))", "2011-12-22");

  // These expressions directly extract hour/minute/second/millis from STRING type
  // to support these functions for timestamp strings without a date part (IMPALA-11355).
  TestValue("hour('09:10:11.000000')", TYPE_INT, 9);
  TestValue("minute('09:10:11.000000')", TYPE_INT, 10);
  TestValue("second('09:10:11.000000')", TYPE_INT, 11);
  TestValue("millisecond('09:10:11.123456')", TYPE_INT, 123);
  TestValue("millisecond('09:10:11')", TYPE_INT, 0);
  // Test the functions above with invalid inputs.
  TestIsNull("hour('09:10:1')", TYPE_INT);
  TestIsNull("hour('838:59:59')", TYPE_INT);
  TestIsNull("minute('09-10-11')", TYPE_INT);
  TestIsNull("second('09:aa:11.000000')", TYPE_INT);
  TestIsNull("second('09:10:11pm')", TYPE_INT);
  TestIsNull("millisecond('24:11:11.123')", TYPE_INT);
  TestIsNull("millisecond('09:61:11.123')", TYPE_INT);
  TestIsNull("millisecond('09:10:61.123')", TYPE_INT);
  TestIsNull("millisecond('09:10:11.123aaa')", TYPE_INT);
  TestIsNull("millisecond('')", TYPE_INT);

  // Check that timeofday() does not crash or return incorrect results
  TestIsNotNull("timeofday()", TYPE_STRING);

  TestValue("timestamp_cmp('1964-05-04 15:33:45','1966-05-04 15:33:45')", TYPE_INT, -1);
  TestValue("timestamp_cmp('1966-09-04 15:33:45','1966-05-04 15:33:45')", TYPE_INT, 1);
  TestValue("timestamp_cmp('1966-05-04 15:33:45','1966-05-04 15:33:45')", TYPE_INT, 0);
  TestValue("timestamp_cmp('1967-06-05','1966-05-04')", TYPE_INT, 1);
  TestValue("timestamp_cmp('1966-05-04','1966-05-04 15:33:45')", TYPE_INT, -1);

  TestIsNull("timestamp_cmp('','1966-05-04 15:33:45')", TYPE_INT);
  TestIsNull("timestamp_cmp('','1966-05-04 15:33:45')", TYPE_INT);
  TestIsNull("timestamp_cmp(NULL,'1966-05-04 15:33:45')", TYPE_INT);
  // Invalid timestamp test case
  TestIsNull("timestamp_cmp('1966-5-4 50:33:45','1966-5-4 15:33:45')", TYPE_INT);

  TestValue("int_months_between('1967-07-19','1966-06-04')", TYPE_INT, 13);
  TestValue("int_months_between('1966-06-04 16:34:45','1967-07-19 15:33:46')",
      TYPE_INT, -13);
  TestValue("int_months_between('1967-07-19','1967-07-19')", TYPE_INT, 0);
  TestValue("int_months_between('2015-07-19','2015-08-18')", TYPE_INT, 0);

  TestIsNull("int_months_between('','1966-06-04')", TYPE_INT);

  TestValue("months_between('1967-07-19','1966-06-04')", TYPE_DOUBLE,
      13.48387096774194);
  TestValue("months_between('1966-06-04 16:34:45','1967-07-19 15:33:46')",
      TYPE_DOUBLE, -13.48387096774194);
  TestValue("months_between('1967-07-19','1967-07-19')", TYPE_DOUBLE, 0);
  TestValue("months_between('2015-02-28','2015-05-31')", TYPE_DOUBLE, -3);
  TestValue("months_between('2012-02-29','2012-01-31')", TYPE_DOUBLE, 1);

  TestIsNull("months_between('','1966-06-04')", TYPE_DOUBLE);

  TestValue("datediff(cast('2011-12-22 09:10:11.12345678' as timestamp), \
      cast('2012-12-22' as timestamp))", TYPE_INT, -366);
  TestValue("datediff(cast('2012-12-22' as timestamp), \
      cast('2011-12-22 09:10:11.12345678' as timestamp))", TYPE_INT, 366);

  TestIsNull("cast('2020-05-06 24:59:59' as timestamp)", TYPE_TIMESTAMP);
  TestIsNull("cast('10000-12-31' as timestamp)", TYPE_TIMESTAMP);
  TestIsNull("cast('10000-12-31 23:59:59' as timestamp)", TYPE_TIMESTAMP);
  TestIsNull("cast('2000-12-31 24:59:59' as timestamp)", TYPE_TIMESTAMP);

  TestIsNull("year(NULL)", TYPE_INT);
  TestIsNull("quarter(NULL)", TYPE_INT);
  TestIsNull("month(NULL)", TYPE_INT);
  TestIsNull("dayofmonth(NULL)", TYPE_INT);
  TestIsNull("day(NULL)", TYPE_INT);
  TestIsNull("dayofweek(NULL)", TYPE_INT);
  TestIsNull("dayofyear(NULL)", TYPE_INT);
  TestIsNull("weekofyear(NULL)", TYPE_INT);
  TestIsNull("week(NULL)", TYPE_INT);
  TestIsNull("datediff(NULL, cast('2011-12-22 09:10:11.12345678' as timestamp))",
      TYPE_INT);
  TestIsNull("datediff(cast('2012-12-22' as timestamp), NULL)", TYPE_INT);
  TestIsNull("datediff(NULL, NULL)", TYPE_INT);
  TestIsNull("millisecond(NULL)", TYPE_INT);

  TestStringValue("dayname(cast('2011-12-18 09:10:11.000000' as timestamp))", "Sunday");
  TestStringValue("dayname(cast('2011-12-19 09:10:11.000000' as timestamp))", "Monday");
  TestStringValue("dayname(cast('2011-12-20 09:10:11.000000' as timestamp))", "Tuesday");
  TestStringValue("dayname(cast('2011-12-21 09:10:11.000000' as timestamp))",
      "Wednesday");
  TestStringValue("dayname(cast('2011-12-22 09:10:11.000000' as timestamp))",
      "Thursday");
  TestStringValue("dayname(cast('2011-12-23 09:10:11.000000' as timestamp))", "Friday");
  TestStringValue("dayname(cast('2011-12-24 09:10:11.000000' as timestamp))",
      "Saturday");
  TestStringValue("dayname(cast('2011-12-25 09:10:11.000000' as timestamp))", "Sunday");
  TestIsNull("dayname(NULL)", TYPE_STRING);

  TestStringValue("monthname(cast('2011-01-18 09:10:11.000000' as timestamp))", "January");
  TestStringValue("monthname(cast('2011-02-18 09:10:11.000000' as timestamp))", "February");
  TestStringValue("monthname(cast('2011-03-18 09:10:11.000000' as timestamp))", "March");
  TestStringValue("monthname(cast('2011-04-18 09:10:11.000000' as timestamp))", "April");
  TestStringValue("monthname(cast('2011-05-18 09:10:11.000000' as timestamp))", "May");
  TestStringValue("monthname(cast('2011-06-18 09:10:11.000000' as timestamp))", "June");
  TestStringValue("monthname(cast('2011-07-18 09:10:11.000000' as timestamp))", "July");
  TestStringValue("monthname(cast('2011-08-18 09:10:11.000000' as timestamp))", "August");
  TestStringValue("monthname(cast('2011-09-18 09:10:11.000000' as timestamp))", "September");
  TestStringValue("monthname(cast('2011-10-18 09:10:11.000000' as timestamp))", "October");
  TestStringValue("monthname(cast('2011-11-18 09:10:11.000000' as timestamp))", "November");
  TestStringValue("monthname(cast('2011-12-18 09:10:11.000000' as timestamp))", "December");
  TestIsNull("monthname(NULL)", TYPE_STRING);

  TestValue("quarter(cast('2011-01-18 09:10:11.000000' as timestamp))", TYPE_INT, 1);
  TestValue("quarter(cast('2011-02-18 09:10:11.000000' as timestamp))", TYPE_INT, 1);
  TestValue("quarter(cast('2011-03-18 09:10:11.000000' as timestamp))", TYPE_INT, 1);
  TestValue("quarter(cast('2011-04-18 09:10:11.000000' as timestamp))", TYPE_INT, 2);
  TestValue("quarter(cast('2011-05-18 09:10:11.000000' as timestamp))", TYPE_INT, 2);
  TestValue("quarter(cast('2011-06-18 09:10:11.000000' as timestamp))", TYPE_INT, 2);
  TestValue("quarter(cast('2011-07-18 09:10:11.000000' as timestamp))", TYPE_INT, 3);
  TestValue("quarter(cast('2011-08-18 09:10:11.000000' as timestamp))", TYPE_INT, 3);
  TestValue("quarter(cast('2011-09-18 09:10:11.000000' as timestamp))", TYPE_INT, 3);
  TestValue("quarter(cast('2011-10-18 09:10:11.000000' as timestamp))", TYPE_INT, 4);
  TestValue("quarter(cast('2011-11-18 09:10:11.000000' as timestamp))", TYPE_INT, 4);
  TestValue("quarter(cast('2011-12-18 09:10:11.000000' as timestamp))", TYPE_INT, 4);
  TestIsNull("quarter(NULL)", TYPE_INT);

  // Tests from Hive
  // The hive documentation states that timestamps are timezoneless, but the tests
  // show that they treat them as being in the current timezone so these tests
  // use the utc conversion to correct for that and get the same answers as
  // are in the hive test output.
  TestValue("cast(to_utc_timestamp(cast('2011-01-01 01:01:01' as timestamp), 'PST') "
      "as boolean)", TYPE_BOOLEAN, true);
  TestValue("cast(to_utc_timestamp(cast('2011-01-01 01:01:01' as timestamp), 'PST') "
      "as tinyint)", TYPE_TINYINT, 77);
  TestValue("cast(to_utc_timestamp(cast('2011-01-01 01:01:01' as timestamp), 'PST') "
      "as smallint)", TYPE_SMALLINT, -4787);
  TestValue("cast(to_utc_timestamp(cast('2011-01-01 01:01:01' as timestamp), 'PST') "
      "as int)", TYPE_INT, 1293872461);
  TestValue("cast(to_utc_timestamp(cast('2011-01-01 01:01:01' as timestamp), 'PST') "
      "as bigint)", TYPE_BIGINT, 1293872461);
  // We have some rounding errors going backend to front, so do it as a string.
  TestStringValue("cast(cast (to_utc_timestamp(cast('2011-01-01 01:01:01' "
      "as timestamp), 'PST') as float) as string)", "1.29387251e+09");
  TestValue("cast(to_utc_timestamp(cast('2011-01-01 01:01:01' as timestamp), 'PST') "
      "as double)", TYPE_DOUBLE, 1.293872461E9);
  TestValue("cast(to_utc_timestamp(cast('2011-01-01 01:01:01.1' as timestamp), 'PST') "
      "as double)", TYPE_DOUBLE, 1.2938724611E9);
  TestValue("cast(to_utc_timestamp(cast('2011-01-01 01:01:01.0001' as timestamp), 'PST') "
      "as double)", TYPE_DOUBLE, 1.2938724610001E9);
  TestStringValue("cast(from_utc_timestamp(cast(1.3041352164485E9 as timestamp), 'PST') "
      "as string)", "2011-04-29 20:46:56.448500000");
  // NULL arguments.
  TestIsNull("from_utc_timestamp(NULL, 'PST')", TYPE_TIMESTAMP);
  TestIsNull("from_utc_timestamp(cast('2011-01-01 01:01:01.1' as timestamp), NULL)",
      TYPE_TIMESTAMP);
  TestIsNull("from_utc_timestamp(NULL, NULL)", TYPE_TIMESTAMP);

  // Tests from Hive. When casting from timestamp to numeric, timestamps are considered
  // to be local values.
  {
    ScopedExecOption use_local(executor_,
        "USE_LOCAL_TZ_FOR_UNIX_TIMESTAMP_CONVERSIONS=1");
    ScopedTimeZoneOverride time_zone("PST8PDT");
    TestValue("cast(cast('2011-01-01 01:01:01' as timestamp) as boolean)", TYPE_BOOLEAN,
        true);
    TestValue("cast(cast('2011-01-01 01:01:01' as timestamp) as tinyint)", TYPE_TINYINT,
        77);
    TestValue("cast(cast('2011-01-01 01:01:01' as timestamp) as smallint)", TYPE_SMALLINT,
        -4787);
    TestValue("cast(cast('2011-01-01 01:01:01' as timestamp) as int)", TYPE_INT,
        1293872461);
    TestValue("cast(cast('2011-01-01 01:01:01' as timestamp) as bigint)", TYPE_BIGINT,
        1293872461);
    // We have some rounding errors going backend to front, so do it as a string.
    TestStringValue("cast(cast(cast('2011-01-01 01:01:01' as timestamp) as float)"
        " as string)", "1.29387251e+09");
    TestValue("cast(cast('2011-01-01 01:01:01' as timestamp) as double)", TYPE_DOUBLE,
        1.293872461E9);
    TestValue("cast(cast('2011-01-01 01:01:01.1' as timestamp) as double)", TYPE_DOUBLE,
        1.2938724611E9);
    TestValue("cast(cast('2011-01-01 01:01:01.0001' as timestamp) as double)",
        TYPE_DOUBLE, 1.2938724610001E9);
    TestStringValue("cast(cast(1.3041352164485E9 as timestamp) as string)",
        "2011-04-29 20:46:56.448500000");

    // NULL arguments.
    TestIsNull("from_utc_timestamp(NULL, 'PST')", TYPE_TIMESTAMP);
    TestIsNull("from_utc_timestamp(cast('2011-01-01 01:01:01.1' as timestamp), NULL)",
        TYPE_TIMESTAMP);
    TestIsNull("from_utc_timestamp(NULL, NULL)", TYPE_TIMESTAMP);
  }

  // Hive silently ignores bad timezones.  We log a problem.
  TestStringValue("cast(from_utc_timestamp("
                  "cast('1970-01-01 00:00:00' as timestamp), 'FOOBAR') as string)",
      "1970-01-01 00:00:00");

  // These return NULL because timezone conversion makes the value out
  // of range.
  TestIsNull("to_utc_timestamp(CAST(\"1400-01-01 05:00:00\" as TIMESTAMP), \"AET\")",
      TYPE_TIMESTAMP);
  TestIsNull("from_utc_timestamp(CAST(\"1400-01-01 05:00:00\" as TIMESTAMP), \"PST\")",
      TYPE_TIMESTAMP);
  TestIsNull("from_utc_timestamp(CAST(\"9999-12-31 21:00:00\" as TIMESTAMP), \"JST\")",
      TYPE_TIMESTAMP);
  TestIsNull("to_utc_timestamp(CAST(\"9999-12-31 21:00:00\" as TIMESTAMP), \"PST\")",
      TYPE_TIMESTAMP);

  // With support of date strings this generates a date and 0 time.
  TestStringValue("cast(cast('1999-01-10' as timestamp) as string)",
      "1999-01-10 00:00:00");

  // Test functions with unknown expected value.
  TestValidTimestampValue("now()");
  TestValidTimestampValue("utc_timestamp()");
  TestValidTimestampValue("current_timestamp()");
  TestValidTimestampValue("cast(unix_timestamp() as timestamp)");

  {
    // Test that the epoch is reasonable. The default behavior of UNIX_TIMESTAMP()
    // is incorrect but wasn't changed for compatibility reasons. The function returns
    // a value as though the current timezone is UTC. Or in other words, 1970-01-01
    // in the current timezone is the effective epoch. A flag was introduced to enable
    // the correct behavior. The first test below checks the default/incorrect behavior.
    ScopedTimeZoneOverride time_zone(TEST_TZ_WITHOUT_DST);
    time_t unix_start_time =
        (posix_time::microsec_clock::local_time() - from_time_t(0)).total_seconds();
    int64_t unix_timestamp_result = ConvertValue<int64_t>(GetValue("unix_timestamp()",
        ColumnType(TYPE_BIGINT)));
    EXPECT_BETWEEN(unix_start_time, unix_timestamp_result, static_cast<int64_t>(
        (posix_time::microsec_clock::local_time() - from_time_t(0)).total_seconds()));

    // Check again with the flag enabled.
    {
      ScopedExecOption use_local(executor_,
          "USE_LOCAL_TZ_FOR_UNIX_TIMESTAMP_CONVERSIONS=1");
      tm before = to_tm(posix_time::microsec_clock::local_time());
      unix_start_time = mktime(&before);
      unix_timestamp_result = ConvertValue<int64_t>(GetValue("unix_timestamp()",
          ColumnType(TYPE_BIGINT)));
      tm after = to_tm(posix_time::microsec_clock::local_time());
      EXPECT_BETWEEN(unix_start_time, unix_timestamp_result,
          static_cast<int64_t>(mktime(&after)));
    }
  }

  // Test that now() and current_timestamp() are reasonable.
  {
    ScopedTimeZoneOverride time_zone(TEST_TZ_WITHOUT_DST);
    ScopedExecOption use_local(executor_,
        "USE_LOCAL_TZ_FOR_UNIX_TIMESTAMP_CONVERSIONS=1");
    const Timezone& local_tz = time_zone.GetTimezone();

    const TimestampValue start_time =
        TimestampValue::FromUnixTimeMicros(UnixMicros(), &local_tz);
    TimestampValue timestamp_result =
        ConvertValue<TimestampValue>(GetValue("now()", ColumnType(TYPE_TIMESTAMP)));
    EXPECT_BETWEEN(start_time, timestamp_result,
        TimestampValue::FromUnixTimeMicros(UnixMicros(), &local_tz));
    timestamp_result = ConvertValue<TimestampValue>(GetValue("current_timestamp()",
        ColumnType(TYPE_TIMESTAMP)));
    EXPECT_BETWEEN(start_time, timestamp_result,
        TimestampValue::FromUnixTimeMicros(UnixMicros(), &local_tz));
  }

  // Test that utc_timestamp() is reasonable.
  const TimestampValue utc_start_time =
      TimestampValue::UtcFromUnixTimeMicros(UnixMicros());
  TimestampValue timestamp_result = ConvertValue<TimestampValue>(
      GetValue("utc_timestamp()", ColumnType(TYPE_TIMESTAMP)));
  EXPECT_BETWEEN(utc_start_time, timestamp_result,
      TimestampValue::UtcFromUnixTimeMicros(UnixMicros()));

  // Test cast(unix_timestamp() as timestamp).
  {
    ScopedTimeZoneOverride time_zone(TEST_TZ_WITHOUT_DST);
    ScopedExecOption use_local(executor_,
        "USE_LOCAL_TZ_FOR_UNIX_TIMESTAMP_CONVERSIONS=1");
    const Timezone& local_tz = time_zone.GetTimezone();

    // UNIX_TIMESTAMP() has second precision so the comparison start time is shifted back
    // a second to ensure an earlier value.
    time_t unix_start_time =
        (posix_time::microsec_clock::local_time() - from_time_t(0)).total_seconds();
    TimestampValue timestamp_result = ConvertValue<TimestampValue>(GetValue(
        "cast(unix_timestamp() as timestamp)", ColumnType(TYPE_TIMESTAMP)));
    EXPECT_BETWEEN(TimestampValue::FromUnixTime(unix_start_time - 1, &local_tz),
        timestamp_result,
        TimestampValue::FromUnixTimeMicros(UnixMicros(), &local_tz));
  }

  // Test that UTC and local time represent the same point in time
  {
    ScopedTimeZoneOverride time_zone_override("PST8PDT");

    const string stmt = "select now(), utc_timestamp()";
    vector<FieldSchema> result_types;
    Status status = executor_->Exec(stmt, &result_types);
    EXPECT_TRUE(status.ok()) << "stmt: " << stmt << "\nerror: " << status.GetDetail();
    DCHECK(result_types.size() == 2);
    EXPECT_EQ(result_types[0].type, "timestamp")
        << "invalid type returned by now()";
    EXPECT_EQ(result_types[1].type, "timestamp")
        << "invalid type returned by utc_timestamp()";
    string result_row;
    status = executor_->FetchResult(&result_row);
    EXPECT_TRUE(status.ok()) << "stmt: " << stmt << "\nerror: " << status.GetDetail();
    vector<string> result_cols;
    boost::split(result_cols, result_row, boost::is_any_of("\t"));
    // To ensure this fails if columns are not tab separated
    DCHECK(result_cols.size() == 2);
    const TimestampValue local_time = ConvertValue<TimestampValue>(result_cols[0]);
    const TimestampValue utc_timestamp = ConvertValue<TimestampValue>(result_cols[1]);

    TimestampValue utc_converted_to_local(utc_timestamp);
    utc_converted_to_local.UtcToLocal(time_zone_override.GetTimezone());
    EXPECT_EQ(utc_converted_to_local, local_time);
  }

  // Test alias
  TestValue("now() = current_timestamp()", TYPE_BOOLEAN, true);

  // Test custom formats
  TestValue("unix_timestamp('1970|01|01 00|00|00', 'yyyy|MM|dd HH|mm|ss')", TYPE_BIGINT,
      0);
  TestValue("unix_timestamp('01,Jan,1970,00,00,00', 'dd,MMM,yyyy,HH,mm,ss')", TYPE_BIGINT,
      0);
  // This time format is misleading because a trailing Z means UTC but a timestamp can
  // have no time zone association. unix_timestamp() expects inputs to be in local time.
  TestValue<int64_t>("unix_timestamp('1983-08-05T05:00:00.000Z', "
      "'yyyy-MM-ddTHH:mm:ss.SSSZ')", TYPE_BIGINT, 428907600);

  TestStringValue("from_unixtime(0, 'yyyy|MM|dd HH|mm|ss')", "1970|01|01 00|00|00");
  TestStringValue("from_unixtime(0, 'dd,MMM,yyyy,HH,mm,ss')", "01,Jan,1970,00,00,00");

  // Test invalid formats returns error
  TestError("unix_timestamp('1970-01-01 00:00:00', 'yyyy-MM-dd hh:mm:ss')");
  TestError("unix_timestamp('1970-01-01 10:10:10', NULL)");
  TestError("unix_timestamp(NULL, NULL)");
  TestError("from_unixtime(0, NULL)");
  TestError("from_unixtime(cast(0 as bigint), NULL)");
  TestError("from_unixtime(NULL, NULL)");
  TestError("to_timestamp('1970-01-01 00:00:00', NULL)");
  TestError("to_timestamp(NULL, NULL)");
  TestError("from_timestamp(cast('1970-01-01 00:00:00' as timestamp), NULL)");
  TestError("from_timestamp(NULL, NULL)");
  TestError("unix_timestamp('1970-01-01 00:00:00', ' ')");
  TestError("unix_timestamp('1970-01-01 00:00:00', ' -===-')");
  TestError("unix_timestamp('1970-01-01', '\"foo\"')");
  TestError("from_unixtime(0, 'YY-MM-dd HH:mm:dd')");
  TestError("from_unixtime(0, 'yyyy-MM-dd hh::dd')");
  TestError("from_unixtime(cast(0 as bigint), 'YY-MM-dd HH:mm:dd')");
  TestError("from_unixtime(cast(0 as bigint), 'yyyy-MM-dd hh::dd')");
  TestError("from_unixtime(0, '')");
  TestError("from_unixtime(0, NULL)");
  TestError("from_unixtime(0, ' ')");
  TestError("from_unixtime(0, ' -=++=- ')");
  TestError("to_timestamp('1970-01-01 00:00:00', ' ')");
  TestError("to_timestamp('1970-01-01 00:00:00', ' -===-')");
  TestError("to_timestamp('1970-01-01', '\"foo\"')");
  TestError("from_timestamp(cast('1970-01-01 00:00:00' as timestamp), ' ')");
  TestError("from_timestamp(cast('1970-01-01 00:00:00' as timestamp), ' -===-')");
  TestError("from_timestamp(cast('1970-01-01' as timestamp), '\"foo\"')");
  TestError("to_timestamp('1970-01-01', 'YY-MM-dd HH:mm:dd')");
  TestError("to_timestamp('1970-01-01', 'yyyy-MM-dd hh::dd')");
  TestError("to_timestamp('1970-01-01', 'YY-MM-dd HH:mm:dd')");
  TestError("to_timestamp('1970-01-01', 'yyyy-MM-dd hh::dd')");
  TestError("to_timestamp('1970-01-01', '')");
  TestError("to_timestamp('1970-01-01', NULL)");
  TestError("to_timestamp('1970-01-01', ' ')");
  TestError("to_timestamp('1970-01-01', ' -=++=- ')");
  TestError("from_timestamp(cast('1970-01-01' as timestamp), 'YY-MM-dd HH:mm:dd')");
  TestError("from_timestamp(cast('1970-01-01' as timestamp), 'yyyy-MM-dd hh::dd')");
  TestError("from_timestamp(cast('1970-01-01' as timestamp), 'YY-MM-dd HH:mm:dd')");
  TestError("from_timestamp(cast('1970-01-01' as timestamp), 'yyyy-MM-dd hh::dd')");
  TestError("from_timestamp(cast('1970-01-01' as timestamp), '')");
  TestError("from_timestamp(cast('1970-01-01' as timestamp), NULL)");
  TestError("from_timestamp(cast('1970-01-01' as timestamp), ' ')");
  TestError("from_timestamp(cast('1970-01-01' as timestamp), ' -=++=- ')");

  // Valid format string, but invalid Timestamp, should return null;
  TestIsNull("unix_timestamp('1970-01-01', 'yyyy-MM-dd HH:mm:ss')", TYPE_BIGINT);
  TestIsNull("unix_timestamp('1970', 'yyyy-MM-dd')", TYPE_BIGINT);
  TestIsNull("unix_timestamp('', 'yyyy-MM-dd')", TYPE_BIGINT);
  TestIsNull("unix_timestamp('|1|1 00|00|00', 'yyyy|M|d HH|MM|ss')", TYPE_BIGINT);
  TestIsNull("to_timestamp('1970-01-01', 'yyyy-MM-dd HH:mm:ss')", TYPE_TIMESTAMP);
  TestIsNull("to_timestamp('1970', 'yyyy-MM-dd')", TYPE_TIMESTAMP);
  TestIsNull("to_timestamp('', 'yyyy-MM-dd')", TYPE_TIMESTAMP);
  TestIsNull("to_timestamp('|1|1 00|00|00', 'yyyy|M|d HH|MM|ss')", TYPE_TIMESTAMP);
  TestIsNull("from_timestamp(cast('1970' as timestamp), 'yyyy-MM-dd')", TYPE_STRING);
  TestIsNull("from_timestamp(cast('' as timestamp), 'yyyy-MM-dd')", TYPE_STRING);
  TestIsNull("from_timestamp(cast('|1|1 00|00|00' as timestamp), 'yyyy|M|d HH|MM|ss')", TYPE_STRING);

  TestIsNull("unix_timestamp('1970-01', 'yyyy-MM-dd')", TYPE_BIGINT);
  TestIsNull("unix_timestamp('1970-20-01', 'yyyy-MM-dd')", TYPE_BIGINT);

  // regression test for IMPALA-1105
  TestIsNull("cast(trunc('2014-07-22 01:34:55 +0100', 'year') as STRING)", TYPE_STRING);
  TestStringValue("cast(trunc(cast('2014-04-01' as timestamp), 'SYYYY') as string)",
          "2014-01-01 00:00:00");
  // note: no time value in string defaults to 00:00:00
  TestStringValue("cast(trunc(cast('2014-01-01' as timestamp), 'MI') as string)",
          "2014-01-01 00:00:00");

  TestStringValue(
        "cast(trunc(cast('2014-04-01 01:01:01' as timestamp), 'SYYYY') as string)",
          "2014-01-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-04-01 01:01:01' as timestamp), 'YYYY') as string)",
          "2014-01-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-04-01 01:01:01' as timestamp), 'YEAR') as string)",
          "2014-01-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-04-01 01:01:01' as timestamp), 'SYEAR') as string)",
          "2014-01-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-04-01 01:01:01' as timestamp), 'YYY') as string)",
          "2014-01-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-04-01 01:01:01' as timestamp), 'YY') as string)",
          "2014-01-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-04-01 01:01:01' as timestamp), 'Y') as string)",
          "2014-01-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2000-01-01 00:00:00' as timestamp), 'Y') as string)",
          "2000-01-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2000-01-01 00:00:00' as timestamp), 'Q') as string)",
          "2000-01-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2000-01-01 01:00:00' as timestamp), 'Q') as string)",
          "2000-01-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2000-02-01 01:00:00' as timestamp), 'Q') as string)",
          "2000-01-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2000-03-01 01:00:00' as timestamp), 'Q') as string)",
          "2000-01-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2000-04-01 01:00:00' as timestamp), 'Q') as string)",
          "2000-04-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2000-05-01 01:00:00' as timestamp), 'Q') as string)",
          "2000-04-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2000-06-01 01:00:00' as timestamp), 'Q') as string)",
          "2000-04-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2000-07-01 01:00:00' as timestamp), 'Q') as string)",
          "2000-07-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2000-08-01 01:00:00' as timestamp), 'Q') as string)",
          "2000-07-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2000-09-01 01:00:00' as timestamp), 'Q') as string)",
          "2000-07-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2000-10-01 01:00:00' as timestamp), 'Q') as string)",
          "2000-10-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2000-11-01 01:00:00' as timestamp), 'Q') as string)",
          "2000-10-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2000-12-01 01:00:00' as timestamp), 'Q') as string)",
          "2000-10-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2001-02-05 01:01:01' as timestamp), 'MONTH') as string)",
          "2001-02-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2001-02-05 01:01:01' as timestamp), 'MON') as string)",
          "2001-02-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2001-02-05 01:01:01' as timestamp), 'MM') as string)",
          "2001-02-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2001-02-05 01:01:01' as timestamp), 'RM') as string)",
          "2001-02-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2001-01-01 00:00:00' as timestamp), 'MM') as string)",
          "2001-01-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2001-12-29 00:00:00' as timestamp), 'MM') as string)",
          "2001-12-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-01-08 01:02:03' as timestamp), 'WW') as string)",
          "2014-01-08 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-01-07 00:00:00' as timestamp), 'WW') as string)",
          "2014-01-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-01-08 00:00:00' as timestamp), 'WW') as string)",
          "2014-01-08 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-01-09 00:00:00' as timestamp), 'WW') as string)",
          "2014-01-08 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-01-14 00:00:00' as timestamp), 'WW') as string)",
          "2014-01-08 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-01-08 01:02:03' as timestamp), 'W') as string)",
          "2014-01-08 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-01-07 00:00:00' as timestamp), 'W') as string)",
          "2014-01-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-01-08 00:00:00' as timestamp), 'W') as string)",
          "2014-01-08 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-01-09 00:00:00' as timestamp), 'W') as string)",
          "2014-01-08 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-01-14 00:00:00' as timestamp), 'W') as string)",
          "2014-01-08 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-02-01 01:02:03' as timestamp), 'W') as string)",
          "2014-02-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-02-02 00:00:00' as timestamp), 'W') as string)",
          "2014-02-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-02-03 00:00:00' as timestamp), 'W') as string)",
          "2014-02-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-02-07 00:00:00' as timestamp), 'W') as string)",
          "2014-02-01 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-02-08 00:00:00' as timestamp), 'W') as string)",
          "2014-02-08 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-02-24 00:00:00' as timestamp), 'W') as string)",
          "2014-02-22 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-01-08 01:02:03' as timestamp), 'DDD') as string)",
          "2014-01-08 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-01-08 01:02:03' as timestamp), 'DD') as string)",
          "2014-01-08 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-02-08 01:02:03' as timestamp), 'J') as string)",
          "2014-02-08 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-02-08 00:00:00' as timestamp), 'J') as string)",
          "2014-02-08 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2014-02-19 00:00:00' as timestamp), 'J') as string)",
          "2014-02-19 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2012-09-10 01:02:03' as timestamp), 'DAY') as string)",
          "2012-09-10 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2012-09-10 01:02:03' as timestamp), 'DY') as string)",
          "2012-09-10 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2012-09-10 01:02:03' as timestamp), 'D') as string)",
          "2012-09-10 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2012-09-11 01:02:03' as timestamp), 'D') as string)",
          "2012-09-10 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2012-09-12 01:02:03' as timestamp), 'D') as string)",
          "2012-09-10 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2012-09-16 01:02:03' as timestamp), 'D') as string)",
          "2012-09-10 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2012-09-10 07:02:03' as timestamp), 'HH') as string)",
          "2012-09-10 07:00:00");
  TestStringValue(
        "cast(trunc(cast('2012-09-10 07:02:03' as timestamp), 'HH12') as string)",
          "2012-09-10 07:00:00");
  TestStringValue(
        "cast(trunc(cast('2012-09-10 07:02:03' as timestamp), 'HH24') as string)",
          "2012-09-10 07:00:00");
  TestStringValue(
        "cast(trunc(cast('2012-09-10 00:02:03' as timestamp), 'HH') as string)",
          "2012-09-10 00:00:00");
  TestStringValue(
        "cast(trunc(cast('2012-09-10 23:02:03' as timestamp), 'HH') as string)",
          "2012-09-10 23:00:00");
  TestStringValue(
        "cast(trunc(cast('2012-09-10 23:59:59' as timestamp), 'HH') as string)",
          "2012-09-10 23:00:00");
  TestStringValue(
        "cast(trunc(cast('2012-09-10 07:02:03' as timestamp), 'MI') as string)",
          "2012-09-10 07:02:00");
  TestStringValue(
        "cast(trunc(cast('2012-09-10 07:00:03' as timestamp), 'MI') as string)",
          "2012-09-10 07:00:00");
  TestStringValue(
        "cast(trunc(cast('2012-09-10 07:00:00' as timestamp), 'MI') as string)",
          "2012-09-10 07:00:00");
  TestStringValue(
        "cast(trunc(cast('2012-09-10 07:59:03' as timestamp), 'MI') as string)",
          "2012-09-10 07:59:00");
  TestStringValue(
        "cast(trunc(cast('2012-09-10 07:59:59.123' as timestamp), 'MI') as string)",
          "2012-09-10 07:59:00");
  TestNonOkStatus(
      "cast(trunc(cast('2012-09-10 07:59:59' as timestamp), 'MIN') as string)");
  TestNonOkStatus(
      "cast(trunc(cast('2012-09-10 07:59:59' as timestamp), 'XXYYZZ') as string)");

  // Extract as a regular function
  TestValue("extract(cast('2006-05-12 18:27:28.123456789' as timestamp), 'YEAR')",
            TYPE_BIGINT, 2006);
  TestValue("extract('2006-05-12 18:27:28.123456789', 'YEAR')", TYPE_BIGINT, 2006);
  TestValue("extract(cast('2006-05-12 18:27:28.123456789' as timestamp), 'MoNTH')",
            TYPE_BIGINT, 5);
  TestValue("extract(cast('2006-05-12 18:27:28.123456789' as timestamp), 'DaY')",
            TYPE_BIGINT, 12);
  TestValue("extract(cast('2006-05-12 06:27:28.123456789' as timestamp), 'hour')",
            TYPE_BIGINT, 6);
  TestValue("extract(cast('2006-05-12 18:27:28.123456789' as timestamp), 'MINUTE')",
            TYPE_BIGINT, 27);
  TestValue("extract(cast('2006-05-12 18:27:28.123456789' as timestamp), 'SECOND')",
            TYPE_BIGINT, 28);
  TestValue("extract(cast('2006-05-12 18:27:28.123456789' as timestamp), 'MILLISECOND')",
            TYPE_BIGINT, 28123);
  TestValue("extract(cast('2006-05-13 01:27:28.123456789' as timestamp), 'EPOCH')",
            TYPE_BIGINT, 1147483648);
  TestNonOkStatus("extract(cast('2006-05-13 01:27:28.123456789' as timestamp), 'foo')");
  TestNonOkStatus("extract(cast('2006-05-13 01:27:28.123456789' as timestamp), NULL)");
  TestIsNull("extract(NULL, 'EPOCH')", TYPE_BIGINT);
  TestNonOkStatus("extract(NULL, NULL)");

  // Extract using FROM keyword
  TestValue("extract(YEAR from cast('2006-05-12 18:27:28.123456789' as timestamp))",
            TYPE_BIGINT, 2006);
  TestValue("extract(QUARTER from cast('2006-05-12 18:27:28.123456789' as timestamp))",
            TYPE_BIGINT, 2);
  TestValue("extract(MoNTH from cast('2006-05-12 18:27:28.123456789' as timestamp))",
            TYPE_BIGINT, 5);
  TestValue("extract(DaY from cast('2006-05-12 18:27:28.123456789' as timestamp))",
            TYPE_BIGINT, 12);
  TestValue("extract(hour from cast('2006-05-12 06:27:28.123456789' as timestamp))",
            TYPE_BIGINT, 6);
  TestValue("extract(MINUTE from cast('2006-05-12 18:27:28.123456789' as timestamp))",
            TYPE_BIGINT, 27);
  TestValue("extract(SECOND from cast('2006-05-12 18:27:28.123456789' as timestamp))",
            TYPE_BIGINT, 28);
  TestValue("extract(MILLISECOND from cast('2006-05-12 18:27:28.123456789' as timestamp))",
            TYPE_BIGINT, 28123);
  TestValue("extract(EPOCH from cast('2006-05-13 01:27:28.123456789' as timestamp))",
            TYPE_BIGINT, 1147483648);
  TestNonOkStatus("extract(foo from cast('2006-05-13 01:27:28.123456789' as timestamp))");
  TestNonOkStatus("extract(NULL from cast('2006-05-13 01:27:28.123456789' as timestamp))");
  TestIsNull("extract(EPOCH from NULL)", TYPE_BIGINT);

  // Date_part, same as extract function but with arguments swapped
  TestValue("date_part('YEAR', cast('2006-05-12 18:27:28.123456789' as timestamp))",
            TYPE_BIGINT, 2006);
  TestValue("date_part('QUARTER', cast('2006-05-12 18:27:28.123456789' as timestamp))",
            TYPE_BIGINT, 2);
  TestValue("date_part('MoNTH', cast('2006-05-12 18:27:28.123456789' as timestamp))",
            TYPE_BIGINT, 5);
  TestValue("date_part('DaY', cast('2006-05-12 18:27:28.123456789' as timestamp))",
            TYPE_BIGINT, 12);
  TestValue("date_part('hour', cast('2006-05-12 06:27:28.123456789' as timestamp))",
            TYPE_BIGINT, 6);
  TestValue("date_part('MINUTE', cast('2006-05-12 18:27:28.123456789' as timestamp))",
            TYPE_BIGINT, 27);
  TestValue("date_part('SECOND', cast('2006-05-12 18:27:28.123456789' as timestamp))",
            TYPE_BIGINT, 28);
  TestValue("date_part('MILLISECOND', cast('2006-05-12 18:27:28.123456789' as timestamp))",
            TYPE_BIGINT, 28123);
  TestValue("date_part('EPOCH', cast('2006-05-13 01:27:28.123456789' as timestamp))",
            TYPE_BIGINT, 1147483648);
  TestNonOkStatus("date_part('foo', cast('2006-05-13 01:27:28.123456789' as timestamp))");
  TestNonOkStatus("date_part(NULL, cast('2006-05-13 01:27:28.123456789' as timestamp))");
  TestIsNull("date_part('EPOCH', NULL)", TYPE_BIGINT);
  TestNonOkStatus("date_part(NULL, NULL)");

  // Test with timezone offset
  TestStringValue("cast(cast('2012-01-01T09:10:11Z' as timestamp) as string)",
                  "2012-01-01 09:10:11");
  TestStringValue("cast(cast('2012-01-01T09:10:11+01:30' as timestamp) as string)",
                  "2012-01-01 09:10:11");
  TestStringValue("cast(cast('2012-01-01T09:10:11-01:30' as timestamp) as string)",
                  "2012-01-01 09:10:11");
  TestStringValue("cast(cast('2012-01-01T09:10:11+0130' as timestamp) as string)",
                  "2012-01-01 09:10:11");
  TestStringValue("cast(cast('2012-01-01T09:10:11-0130' as timestamp) as string)",
                  "2012-01-01 09:10:11");
  TestStringValue("cast(cast('2012-01-01T09:10:11+01' as timestamp) as string)",
                  "2012-01-01 09:10:11");
  TestStringValue("cast(cast('2012-01-01T09:10:11-01' as timestamp) as string)",
                  "2012-01-01 09:10:11");
  TestStringValue(
      "cast(cast('2012-01-01T09:10:11.12345+01:30' as timestamp) as string)",
      "2012-01-01 09:10:11.123450000");
  TestStringValue(
      "cast(cast('2012-01-01T09:10:11.12345-01:30' as timestamp) as string)",
      "2012-01-01 09:10:11.123450000");

  TestValue("unix_timestamp('2038-01-19T03:14:08-0100')", TYPE_BIGINT, 2147483648);
  TestValue("unix_timestamp('2038/01/19T03:14:08+01:00', 'yyyy/MM/ddTHH:mm:ss')",
            TYPE_BIGINT, 2147483648);

  TestValue("unix_timestamp('2038/01/19T03:14:08+01:00', 'yyyy/MM/ddTHH:mm:ss+hh:mm')",
            TYPE_BIGINT, 2147480048);
  TestError("unix_timestamp('1990-01-01+01:00', 'yyyy-MM-dd+hh:mm')");
  TestError("unix_timestamp('1970-01-01 00:00:00+01:10', 'yyyy-MM-dd HH:mm:ss+hh:dd')");

  TestStringValue("cast(trunc('2014-07-22T01:34:55+0100', 'year') as STRING)",
                  "2014-01-01 00:00:00");

  // Test timezone offset format
  TestStringValue("from_unixtime(unix_timestamp('2012-01-01 19:10:11+02:30', \
      'yyyy-MM-dd HH:mm:ss+hh:mm'))", "2012-01-01 16:40:11");
  TestStringValue("from_unixtime(unix_timestamp('2012-01-01 19:10:11-0630', \
      'yyyy-MM-dd HH:mm:ss-hhmm'))", "2012-01-02 01:40:11");
  TestStringValue("from_unixtime(unix_timestamp('2012-01-01 01:10:11+02', \
      'yyyy-MM-dd HH:mm:ss+hh'))", "2011-12-31 23:10:11");
  TestStringValue("from_unixtime(unix_timestamp('2012-12-31 11:10:11-1430', \
      'yyyy-MM-dd HH:mm:ss+hhmm'))", "2013-01-01 01:40:11");
  TestStringValue("from_unixtime(unix_timestamp('2012-01-01 11:10:11+1430', \
      'yyyy-MM-dd HH:mm:ss+hhmm'))", "2011-12-31 20:40:11");
  TestStringValue("from_unixtime(unix_timestamp('2012-02-28 11:10:11-1430', \
      'yyyy-MM-dd HH:mm:ss+hhmm'))", "2012-02-29 01:40:11");
  TestStringValue("from_unixtime(unix_timestamp('1970-01-01 00:00:00+05:00', \
      'yyyy-MM-dd HH:mm:ss+hh:mm'))", "1969-12-31 19:00:00");
  TestStringValue("from_unixtime(unix_timestamp('1400-01-01 19:00:00+1500', \
      'yyyy-MM-dd HH:mm:ss+hhmm'))", "1400-01-01 04:00:00");
  TestStringValue("from_unixtime(unix_timestamp('1400-01-01 02:00:00+0200', \
      'yyyy-MM-dd HH:mm:ss+hhmm'))", "1400-01-01 00:00:00");
  TestIsNull("from_unixtime(unix_timestamp('1400-01-01 00:00:00+0100', \
      'yyyy-MM-dd HH:mm:ss+hhmm'))", TYPE_STRING);
  TestIsNull("from_unixtime(unix_timestamp('1399-12-31 23:00:00+0500', \
      'yyyy-MM-dd HH:mm:ss+hhmm'))", TYPE_STRING);
  TestStringValue("from_unixtime(unix_timestamp('9999-12-31 01:00:00-05:00', \
      'yyyy-MM-dd HH:mm:ss+hh:mm'))", "9999-12-31 06:00:00");
  TestStringValue("from_unixtime(unix_timestamp('9999-12-31 22:59:59-01:00', \
      'yyyy-MM-dd HH:mm:ss+hh:mm'))", "9999-12-31 23:59:59");
  TestIsNull("from_unixtime(unix_timestamp('9999-12-31 22:59:00-01:01', \
      'yyyy-MM-dd HH:mm:ss+hh:mm'))", TYPE_STRING);
  TestIsNull("from_unixtime(unix_timestamp('9999-12-31 23:00:00-05:00', \
      'yyyy-MM-dd HH:mm:ss+hh:mm'))", TYPE_STRING);
  TestIsNull("from_unixtime(unix_timestamp('10000-01-01 02:00:00+02:00', \
      'yyyy-MM-dd HH:mm:ss+hh:mm'))", TYPE_STRING);
  TestIsNull("from_unixtime(unix_timestamp('2012-02-28 11:10:11', \
      'yyyy-MM-dd HH:mm:ss+hhmm'))", TYPE_STRING);
  TestIsNull("from_unixtime(unix_timestamp('2012-02-28 11:10:11-14', \
      'yyyy-MM-dd HH:mm:ss+hhmm'))", TYPE_STRING);
  TestIsNull("from_unixtime(unix_timestamp('2012-02-28 11:10:11*1430', \
      'yyyy-MM-dd HH:mm:ss+hhmm'))", TYPE_STRING);
  TestIsNull("from_unixtime(unix_timestamp('2012-02-28 11:10:11+1587', \
      'yyyy-MM-dd HH:mm:ss+hhmm'))", TYPE_STRING);
  TestIsNull("from_unixtime(unix_timestamp('2012-02-28 11:10:11+2530', \
      'yyyy-MM-dd HH:mm:ss+hhmm'))", TYPE_STRING);
  TestIsNull("from_unixtime(unix_timestamp('2012-02-28 11:10:11+1530', \
      'yyyy-MM-dd HH:mm:ss+hh:mm'))", TYPE_STRING);
  TestIsNull("from_unixtime(unix_timestamp('2012-02-28 11:10:11+2430', \
      'yyyy-MM-dd HH:mm:ss+hhmm'))", TYPE_STRING);
  TestIsNull("from_unixtime(unix_timestamp('2012-02-28 11:10:11+1560', \
      'yyyy-MM-dd HH:mm:ss+hhmm'))", TYPE_STRING);
  TestIsNull("from_unixtime(unix_timestamp('1400-01-01 00:00:00+1500', \
      'yyyy-MM-dd HH:mm:ss+hhmm'))", TYPE_STRING);
  TestStringValue("cast(to_timestamp('2012-01-01 19:10:11+02:30', \
      'yyyy-MM-dd HH:mm:ss+hh:mm') as string)", "2012-01-01 16:40:11");
  TestStringValue("cast(to_timestamp('2012-01-01 19:10:11-0630', \
      'yyyy-MM-dd HH:mm:ss-hhmm') as string)", "2012-01-02 01:40:11");
  TestStringValue("cast(to_timestamp('2012-01-01 01:10:11+02', \
      'yyyy-MM-dd HH:mm:ss+hh') as string)", "2011-12-31 23:10:11");
  TestStringValue("cast(to_timestamp('2012-12-31 11:10:11-1430', \
      'yyyy-MM-dd HH:mm:ss+hhmm') as string)", "2013-01-01 01:40:11");
  TestStringValue("cast(to_timestamp('2012-01-01 11:10:11+1430', \
      'yyyy-MM-dd HH:mm:ss+hhmm') as string)", "2011-12-31 20:40:11");
  TestStringValue("cast(to_timestamp('2012-02-28 11:10:11-1430', \
      'yyyy-MM-dd HH:mm:ss+hhmm') as string)", "2012-02-29 01:40:11");
  TestStringValue("cast(to_timestamp('1970-01-01 00:00:00+05:00', \
      'yyyy-MM-dd HH:mm:ss+hh:mm') as string)", "1969-12-31 19:00:00");
  TestStringValue("cast(to_timestamp('1400-01-01 19:00:00+1500', \
      'yyyy-MM-dd HH:mm:ss+hhmm') as string)", "1400-01-01 04:00:00");
  TestStringValue("cast(to_timestamp('1400-01-01 02:00:00+0200', \
      'yyyy-MM-dd HH:mm:ss+hhmm') as string)", "1400-01-01 00:00:00");

  TestError("from_unixtime(unix_timestamp('2012-02-28 11:10:11+0530', \
      'yyyy-MM-dd HH:mm:ss+hhdd'))");
  TestError("from_unixtime(unix_timestamp('2012-02-28+0530', 'yyyy-MM-dd+hhmm'))");
  TestError("from_unixtime(unix_timestamp('10:00:00+0530 2010-01-01', \
      'HH:mm:ss+hhmm yyyy-MM-dd'))");

  TestError("to_timestamp('01:01:01', 'HH:mm:ss')");
  TestError("to_timestamp('2012-02-28 11:10:11+0530', 'yyyy-MM-dd HH:mm:ss+hhdd')");
  TestError("to_timestamp('2012-02-28+0530', 'yyyy-MM-dd+hhmm')");
  TestError("to_timestamp('10:00:00+0530 2010-01-01', 'HH:mm:ss+hhmm yyyy-MM-dd')");
  TestError("from_timestamp(cast('2012-02-28 11:10:11+0530' as timestamp), 'yyyy-MM-dd HH:mm:ss+hhdd')");
  TestError("from_timestamp(cast('2012-02-28+0530' as timestamp), 'yyyy-MM-dd+hhmm')");
  TestError("from_timestamp(cast('10:00:00+0530 2010-01-01' as timestamp), 'HH:mm:ss+hhmm yyyy-MM-dd')");

  // Regression test for IMPALA-2732, can't parse custom date formats with non-zero-padded
  // values
  TestValue("unix_timestamp('12/2/2015', 'MM/d/yyyy')", TYPE_BIGINT, 1449014400);
  TestIsNull("unix_timestamp('12/2/2015', 'MM/dd/yyyy')", TYPE_BIGINT);
  TestValue("unix_timestamp('12/31/2015', 'MM/d/yyyy')", TYPE_BIGINT, 1451520000);
  TestValue("unix_timestamp('12/31/2015', 'MM/dd/yyyy')", TYPE_BIGINT, 1451520000);

  // next_day udf test for IMPALA-2459
  TestNextDayFunction();

  // last_day udf test for IMPALA-5316
  TestLastDayFunction();

  // Test microsecond unix time conversion functions.
  TestValue("utc_to_unix_micros(\"1400-01-01 00:00:00\")", TYPE_BIGINT,
      -17987443200000000);
  TestValue("utc_to_unix_micros(\"1970-01-01 00:00:00\")", TYPE_BIGINT,
      0);
  TestValue("utc_to_unix_micros(\"9999-01-01 23:59:59.9999999\")", TYPE_BIGINT,
      253370851200000000);

  TestStringValue("cast(unix_micros_to_utc_timestamp(-17987443200000000) as string)",
      "1400-01-01 00:00:00");
  TestIsNull("unix_micros_to_utc_timestamp(-17987443200000001)", TYPE_TIMESTAMP);
  TestStringValue("cast(unix_micros_to_utc_timestamp(253402300799999999) as string)",
      "9999-12-31 23:59:59.999999000");
  TestIsNull("unix_micros_to_utc_timestamp(253402300800000000)", TYPE_TIMESTAMP);
}

TEST_P(TimestampExprTest, TruncForDateTest) {
  // trunc(date, string unit)
  // Truncate date to year
  for (const string& unit: { "SYYYY", "YYYY", "YEAR", "SYEAR", "YYY", "YY", "Y" }) {
    const string expr = "trunc(date'2014-04-01', '" + unit + "')";
    TestDateValue(expr, DateValue(2014, 1, 1));
  }
  TestDateValue("trunc(date'2000-01-01', 'Y')", DateValue(2000, 1, 1));

  // Truncate date to quarter
  TestDateValue("trunc(date'2000-01-01', 'Q')", DateValue(2000, 1, 1));
  TestDateValue("trunc(date'2000-02-01', 'Q')", DateValue(2000, 1, 1));
  TestDateValue("trunc(date'2000-03-01', 'Q')", DateValue(2000, 1, 1));
  TestDateValue("trunc(date'2000-04-01', 'Q')", DateValue(2000, 4, 1));
  TestDateValue("trunc(date'2000-05-01', 'Q')", DateValue(2000, 4, 1));
  TestDateValue("trunc(date'2000-06-01', 'Q')", DateValue(2000, 4, 1));
  TestDateValue("trunc(date'2000-07-01', 'Q')", DateValue(2000, 7, 1));
  TestDateValue("trunc(date'2000-08-01', 'Q')", DateValue(2000, 7, 1));
  TestDateValue("trunc(date'2000-09-01', 'Q')", DateValue(2000, 7, 1));
  TestDateValue("trunc(date'2000-10-01', 'Q')", DateValue(2000, 10, 1));
  TestDateValue("trunc(date'2000-11-01', 'Q')", DateValue(2000, 10, 1));
  TestDateValue("trunc(date'2000-12-01', 'Q')", DateValue(2000, 10, 1));

  // Truncate date to month
  for (const string& unit: { "MONTH", "MON", "MM", "RM" }) {
    const string expr = "trunc(date'2001-02-05', '" + unit + "')";
    TestDateValue(expr, DateValue(2001, 2, 1));
  }
  TestDateValue("trunc(date'2001-01-01', 'MM')", DateValue(2001, 1, 1));
  TestDateValue("trunc(date'2001-12-29', 'MM')", DateValue(2001, 12, 1));

  // Same day of the week as the first day of the year
  TestDateValue("trunc(date'2014-01-07', 'WW')", DateValue(2014, 1, 1));
  TestDateValue("trunc(date'2014-01-08', 'WW')", DateValue(2014, 1, 8));
  TestDateValue("trunc(date'2014-01-09', 'WW')", DateValue(2014, 1, 8));
  TestDateValue("trunc(date'2014-01-14', 'WW')", DateValue(2014, 1, 8));

  // Same day of the week as the first day of the month
  TestDateValue("trunc(date'2014-01-07', 'W')", DateValue(2014, 1, 1));
  TestDateValue("trunc(date'2014-01-08', 'W')", DateValue(2014, 1, 8));
  TestDateValue("trunc(date'2014-01-09', 'W')", DateValue(2014, 1, 8));
  TestDateValue("trunc(date'2014-01-14', 'W')", DateValue(2014, 1, 8));
  TestDateValue("trunc(date'2014-02-01', 'W')", DateValue(2014, 2, 1));
  TestDateValue("trunc(date'2014-02-02', 'W')", DateValue(2014, 2, 1));
  TestDateValue("trunc(date'2014-02-03', 'W')", DateValue(2014, 2, 1));
  TestDateValue("trunc(date'2014-02-07', 'W')", DateValue(2014, 2, 1));
  TestDateValue("trunc(date'2014-02-08', 'W')", DateValue(2014, 2, 8));
  TestDateValue("trunc(date'2014-02-24', 'W')", DateValue(2014, 2, 22));

  // Truncate to day, i.e. leave the date intact
  for (const string& unit: { "DDD", "DD", "J" }) {
    const string expr = "trunc(date'2014-01-08', '" + unit + "')";
    TestDateValue(expr, DateValue(2014, 1, 8));
  }

  // Truncate date to starting day of the week
  for (const string& unit: { "DAY", "DY", "D" }) {
    const string expr = "trunc(date'2012-09-10', '" + unit + "')";
    TestDateValue(expr, DateValue(2012, 9, 10));
  }
  TestDateValue("trunc(date'2012-09-11', 'D')", DateValue(2012, 9, 10));
  TestDateValue("trunc(date'2012-09-12', 'D')", DateValue(2012, 9, 10));
  TestDateValue("trunc(date'2012-09-16', 'D')", DateValue(2012, 9, 10));

  // Test upper limit
  TestDateValue("trunc(date'9999-12-31', 'YYYY')", DateValue(9999, 1, 1));
  TestDateValue("trunc(date'9999-12-31', 'Q')", DateValue(9999, 10, 1));
  TestDateValue("trunc(date'9999-12-31', 'MONTH')", DateValue(9999, 12, 1));
  TestDateValue("trunc(date'9999-12-31', 'W')", DateValue(9999, 12, 29));
  TestDateValue("trunc(date'9999-12-31', 'WW')", DateValue(9999, 12, 31));
  TestDateValue("trunc(date'9999-12-31', 'DDD')", DateValue(9999, 12, 31));
  TestDateValue("trunc(date'9999-12-31', 'DAY')", DateValue(9999, 12, 27));

  // Test lower limit
  TestDateValue("trunc(date'0001-01-01', 'YYYY')", DateValue(1, 1, 1));
  TestDateValue("trunc(date'0001-01-01', 'Q')", DateValue(1, 1, 1));
  TestDateValue("trunc(date'0001-03-31', 'Q')", DateValue(1, 1, 1));
  TestDateValue("trunc(date'0001-01-01', 'MONTH')", DateValue(1, 1, 1));
  // 0001-01-01 is Monday
  TestDateValue("trunc(date'0001-01-01', 'W')", DateValue(1, 1, 1));
  TestDateValue("trunc(date'0001-01-07', 'W')", DateValue(1, 1, 1));
  TestDateValue("trunc(date'0001-01-08', 'W')", DateValue(1, 1, 8));
  TestDateValue("trunc(date'0001-01-01', 'WW')", DateValue(1, 1, 1));
  TestDateValue("trunc(date'0001-01-07', 'WW')", DateValue(1, 1, 1));
  TestDateValue("trunc(date'0001-01-08', 'WW')", DateValue(1, 1, 8));
  TestDateValue("trunc(date'0001-01-01', 'DAY')", DateValue(1, 1, 1));
  TestDateValue("trunc(date'0001-01-07', 'DAY')", DateValue(1, 1, 1));
  TestDateValue("trunc(date'0001-01-08', 'DAY')", DateValue(1, 1, 8));

  // Truncating date to hour or minute returns an error
  for (const string& unit: { "HH", "HH12", "HH24", "MI" }) {
    const string expr = "trunc(date'2012-09-10', '" + unit + "')";
    TestNonOkStatus(expr);  // Unsupported Truncate Unit
  }

  // Invalid trunc unit
  for (const string& unit: { "MIN", "XXYYZZ", "" }) {
    const string expr = "trunc(date'2012-09-10', '" + unit + "')";
    TestNonOkStatus(expr);  // Invalid Truncate Unit
  }
  TestNonOkStatus("trunc(date'2012-09-10', NULL)");  // Invalid Truncate Unit
  TestNonOkStatus("trunc(cast(NULL as date), NULL)");  // Invalid Truncate Unit

  // Truncating NULL date returns NULL.
  TestIsNull("trunc(cast(NULL as date), 'DDD')", TYPE_DATE);
}

TEST_P(TimestampExprTest, DateTruncForDateTest) {
  TestDateValue("date_trunc('MILLENNIUM', date '2016-05-08')", DateValue(2001, 1, 1));
  TestDateValue("date_trunc('MILLENNIUM', date '3000-12-31')", DateValue(2001, 1, 1));
  TestDateValue("date_trunc('MILLENNIUM', date '3001-01-01')", DateValue(3001, 1, 1));
  TestDateValue("date_trunc('CENTURY', date '2016-05-08')", DateValue(2001, 1, 1));
  TestDateValue("date_trunc('CENTURY', date '2116-05-08')", DateValue(2101, 1, 1));
  TestDateValue("date_trunc('DECADE', date '2116-05-08')", DateValue(2110, 1, 1));
  TestDateValue("date_trunc('YEAR', date '2016-05-08')", DateValue(2016, 1, 1));
  TestDateValue("date_trunc('MONTH', date '2016-05-08')", DateValue(2016, 5, 1));
  TestDateValue("date_trunc('WEEK', date '2116-05-08')", DateValue(2116, 5, 4));
  TestDateValue("date_trunc('WEEK', date '2017-01-01')", DateValue(2016,12,26));
  TestDateValue("date_trunc('WEEK', date '2017-01-02')", DateValue(2017, 1, 2));
  TestDateValue("date_trunc('WEEK', date '2017-01-07')", DateValue(2017, 1, 2));
  TestDateValue("date_trunc('WEEK', date '2017-01-08')", DateValue(2017, 1, 2));
  TestDateValue("date_trunc('WEEK', date '2017-01-09')", DateValue(2017, 1, 9));
  TestDateValue("date_trunc('DAY', date '1416-05-08')", DateValue(1416, 5, 8));

  // Test upper limit
  TestDateValue("date_trunc('MILLENNIUM', date '9999-12-31')", DateValue(9001, 1, 1));
  TestDateValue("date_trunc('CENTURY', date '9999-12-31')", DateValue(9901, 1, 1));
  TestDateValue("date_trunc('DECADE', date '9999-12-31')", DateValue(9990, 1, 1));
  TestDateValue("date_trunc('YEAR', date '9999-12-31')", DateValue(9999, 1, 1));
  TestDateValue("date_trunc('MONTH', date '9999-12-31')", DateValue(9999, 12, 1));
  TestDateValue("date_trunc('WEEK', date '9999-12-31')", DateValue(9999, 12, 27));
  TestDateValue("date_trunc('DAY', date '9999-12-31')", DateValue(9999, 12, 31));

  // Test lower limit for millennium
  TestDateValue("date_trunc('MILLENNIUM', date '1001-01-01')", DateValue(1001, 1, 1));
  TestDateValue("date_trunc('MILLENNIUM', date '1000-01-01')", DateValue(1, 1, 1));
  TestDateValue("date_trunc('MILLENNIUM', date '0001-01-01')", DateValue(1, 1, 1));

  // Test lower limit for century
  TestDateValue("date_trunc('CENTURY', date '0101-01-01')", DateValue(101, 1, 1));
  TestDateValue("date_trunc('CENTURY', date '0100-01-01')", DateValue(1, 1, 1));
  TestDateValue("date_trunc('CENTURY', date '0001-01-01')", DateValue(1, 1, 1));

  // Test lower limit for decade
  TestDateValue("date_trunc('DECADE', date '0011-01-01')", DateValue(10, 1, 1));
  TestDateValue("date_trunc('DECADE', date '0010-01-01')", DateValue(10, 1, 1));
  TestIsNull("date_trunc('DECADE', date '0001-01-01')", TYPE_DATE);

  // Test lower limit for year, month, day
  TestDateValue("date_trunc('YEAR', date '0001-01-01')", DateValue(1, 1, 1));
  TestDateValue("date_trunc('MONTH', date '0001-01-01')", DateValue(1, 1, 1));
  TestDateValue("date_trunc('DAY', date '0001-01-01')", DateValue(1, 1, 1));

  // Test lower limit for week
  TestDateValue("date_trunc('WEEK', date '0001-01-08')", DateValue(1, 1, 8));
  TestDateValue("date_trunc('WEEK', date '0001-01-07')", DateValue(1, 1, 1));
  TestDateValue("date_trunc('WEEK', date '0001-01-01')", DateValue(1, 1, 1));

  // Test invalid input.
  // Truncating date to hour or minute returns an error
  for (const string& unit: { "HOUR", "MINUTE", "SECOND", "MILLISECONDS",
      "MICROSECONDS" }) {
    const string expr = "date_trunc('" + unit + "', date '2012-09-10')";
    TestNonOkStatus(expr);  // Unsupported Date Truncate Unit
  }

  // Invalid trunc unit
  for (const string& unit: { "YEARR", "XXYYZZ", "" }) {
    const string expr = "date_trunc('" + unit + "', date '2012-09-10')";
    TestNonOkStatus(expr);  // Invalid Date Truncate Unit
  }
  TestNonOkStatus("date_trunc(NULL, date '2012-09-10'");  // Invalid Date Truncate Unit
  TestNonOkStatus("date_trunc(NULL, cast(NULL as date))");  // Invalid Date Truncate Unit

  // Truncating NULL date returns NULL.
  TestIsNull("date_trunc('DAY', cast(NULL as date))", TYPE_DATE);
}

TEST_P(TimestampExprTest, ExtractAndDatePartForDateTest) {
  // extract as a regular function
  TestValue("extract(date '2006-05-12', 'YEAR')", TYPE_BIGINT, 2006);
  TestValue("extract(date '2006-05-12', 'quarter')", TYPE_BIGINT, 2);
  TestValue("extract(date '2006-05-12', 'MoNTH')", TYPE_BIGINT, 5);
  TestValue("extract(date '2006-05-12', 'DaY')", TYPE_BIGINT, 12);

  // extract using FROM keyword
  TestValue("extract(year from date '2006-05-12')", TYPE_BIGINT, 2006);
  TestValue("extract(QUARTER from date '2006-05-12')", TYPE_BIGINT, 2);
  TestValue("extract(mOnTh from date '2006-05-12')", TYPE_BIGINT, 5);
  TestValue("extract(dAy from date '2006-05-12')", TYPE_BIGINT, 12);

  // Test upper limit
  TestValue("extract(date '9999-12-31', 'YEAR')", TYPE_BIGINT, 9999);
  TestValue("extract(quarter from date '9999-12-31')", TYPE_BIGINT, 4);
  TestValue("extract(date '9999-12-31', 'month')", TYPE_BIGINT, 12);
  TestValue("extract(DAY from date '9999-12-31')", TYPE_BIGINT, 31);

  // Test lower limit
  TestValue("extract(date '0001-01-01', 'YEAR')", TYPE_BIGINT, 1);
  TestValue("extract(quarter from date '0001-01-01')", TYPE_BIGINT, 1);
  TestValue("extract(date '0001-01-01', 'month')", TYPE_BIGINT, 1);
  TestValue("extract(DAY from date '0001-01-01')", TYPE_BIGINT, 1);

  // Time of day extract fields are not supported
  for (const string& field: { "MINUTE", "SECOND", "MILLISECOND", "EPOCH" }) {
    const string expr = "extract(date '2012-09-10', '" + field + "')";
    TestNonOkStatus(expr);  // Unsupported Extract Field
  }

  // Invalid extract fields
  for (const string& field: { "foo", "SSECOND", "" }) {
    const string expr = "extract(date '2012-09-10', '" + field + "')";
    TestNonOkStatus(expr);  // Invalid Extract Field
  }
  TestNonOkStatus("extract(date '2012-09-10', NULL)");  // Invalid Extract Field
  TestNonOkStatus("extract(cast(NULL as date), NULL)");  // Invalid Extract Field

  TestIsNull("extract(cast(NULL as date), 'YEAR')", TYPE_BIGINT);
  TestIsNull("extract(YEAR from cast(NULL as date))", TYPE_BIGINT);

  // date_part, same as extract function but with arguments swapped
  TestValue("date_part('YEAR', date '2006-05-12')", TYPE_BIGINT, 2006);
  TestValue("date_part('QuarTer', date '2006-05-12')", TYPE_BIGINT, 2);
  TestValue("date_part('Month', date '2006-05-12')", TYPE_BIGINT, 5);
  TestValue("date_part('Day', date '2006-05-12')", TYPE_BIGINT, 12);

  // Test upper limit
  TestValue("date_part('YEAR', date '9999-12-31')", TYPE_BIGINT, 9999);
  TestValue("date_part('QUARTER', '9999-12-31')", TYPE_BIGINT, 4);
  TestValue("date_part('month', date '9999-12-31')", TYPE_BIGINT, 12);
  TestValue("date_part('DAY', date '9999-12-31')", TYPE_BIGINT, 31);

  // Test lower limit
  TestValue("date_part('year', date '0001-01-01')", TYPE_BIGINT, 1);
  TestValue("date_part('quarter', date '0001-01-01')", TYPE_BIGINT, 1);
  TestValue("date_part('MONTH', date '0001-01-01')", TYPE_BIGINT, 1);
  TestValue("date_part('DAY', date '0001-01-01')", TYPE_BIGINT, 1);

  // Time of day extract fields are not supported
  for (const string& field: { "MINUTE", "SECOND", "MILLISECOND", "EPOCH" }) {
    const string expr = "date_part('" + field + "', date '2012-09-10')";
    // Unsupported Date Part Field
    TestNonOkStatus(expr);
  }

  // Invalid extract fields
  for (const string& field: { "foo", "SSECOND", "" }) {
    const string expr = "date_part('" + field + "', date '2012-09-10')";
    TestNonOkStatus(expr);  // Invalid Date Part Field
  }
  TestNonOkStatus("date_part(MULL, date '2012-09-10')");  // Invalid Date Part Field
  TestNonOkStatus("date_part(MULL, cast(NULL as date))");  // Invalid Date Part Field

  TestIsNull("date_part('YEAR', cast(NULL as date))", TYPE_BIGINT);
}

TEST_P(TimestampExprTest, DateFunctions) {
  // year:
  TestValue("year(date '2019-06-05')", TYPE_INT, 2019);
  TestValue("year(date '9999-12-31')", TYPE_INT, 9999);
  TestValue("year(date '0001-01-01')", TYPE_INT, 1);
  TestIsNull("year(cast(NULL as date))", TYPE_INT);

  // Test that the name-resolution algorithm picks up the TIMESTAMP-version of year() if
  // year() is called with a STRING.
  TestValue("year('2019-06-05')", TYPE_INT, 2019);
  // 1399-12-31 is out of the valid TIMESTAMP range, year(TIMESTAMP) returns NULL.
  TestIsNull("year('1399-12-31')", TYPE_INT);
  // year(DATE) returns the correct result.
  TestValue("year(DATE '1399-12-31')", TYPE_INT, 1399);
  // Test that calling year(TIMESTAMP) with an invalid argument returns NULL.
  TestIsNull("year('2019-02-29')", TYPE_INT);
  // Test that calling year(DATE) with an invalid argument returns an error.
  TestError("year(DATE '2019-02-29')");

  // month:
  TestValue("month(date '2019-06-05')", TYPE_INT, 6);
  TestValue("month(date '9999-12-31')", TYPE_INT, 12);
  TestValue("month(date '0001-01-01')", TYPE_INT, 1);
  TestIsNull("month(cast(NULL as date))", TYPE_INT);

  // monthname:
  TestStringValue("monthname(date '2019-06-05')", "June");
  TestStringValue("monthname(date '9999-12-31')", "December");
  TestStringValue("monthname(date '0001-01-01')", "January");
  TestIsNull("monthname(cast(NULL as date))", TYPE_STRING);

  // day, dayofmonth:
  TestValue("day(date '2019-06-05')", TYPE_INT, 5);
  TestValue("day(date '9999-12-31')", TYPE_INT, 31);
  TestValue("day(date '0001-01-01')", TYPE_INT, 1);
  TestIsNull("day(cast(NULL as date))", TYPE_INT);
  TestValue("dayofmonth(date '2019-06-07')", TYPE_INT, 7);
  TestValue("dayofmonth(date '9999-12-31')", TYPE_INT, 31);
  TestValue("dayofmonth(date '0001-01-01')", TYPE_INT, 1);
  TestIsNull("dayofmonth(cast(NULL as date))", TYPE_INT);

  // quarter:
  TestValue("quarter(date '2019-01-01')", TYPE_INT, 1);
  TestValue("quarter(date '2019-03-31')", TYPE_INT, 1);
  TestValue("quarter(date '2019-04-01')", TYPE_INT, 2);
  TestValue("quarter(date '2019-06-30')", TYPE_INT, 2);
  TestValue("quarter(date '2019-07-01')", TYPE_INT, 3);
  TestValue("quarter(date '2019-09-30')", TYPE_INT, 3);
  TestValue("quarter(date '2019-10-01')", TYPE_INT, 4);
  TestValue("quarter(date '2019-12-31')", TYPE_INT, 4);
  TestValue("quarter(date '9999-12-31')", TYPE_INT, 4);
  TestValue("quarter(date '0001-01-01')", TYPE_INT, 1);
  TestIsNull("quarter(cast(NULL as date))", TYPE_INT);

  // dayofweek:
  TestValue("dayofweek(date '2019-06-05')", TYPE_INT, 4);
  // 9999-12-31 is Friday.
  TestValue("dayofweek(date '9999-12-31')", TYPE_INT, 6);
  // 0001-01-01 is Monday.
  TestValue("dayofweek(date '0001-01-01')", TYPE_INT, 2);
  TestIsNull("dayofweek(cast(NULL as date))", TYPE_INT);

  // dayname:
  TestStringValue("dayname(date '2019-06-03')", "Monday");
  TestStringValue("dayname(date '2019-06-04')", "Tuesday");
  TestStringValue("dayname(date '2019-06-05')", "Wednesday");
  TestStringValue("dayname(date '2019-06-06')", "Thursday");
  TestStringValue("dayname(date '2019-06-07')", "Friday");
  TestStringValue("dayname(date '2019-06-08')", "Saturday");
  TestStringValue("dayname(date '2019-06-09')", "Sunday");
  TestStringValue("dayname(date '9999-12-31')", "Friday");
  TestStringValue("dayname(date '0001-01-01')", "Monday");
  TestIsNull("dayname(cast(NULL as date))", TYPE_STRING);

  // dayofyear:
  TestValue("dayofyear(date '2019-01-01')", TYPE_INT, 1);
  TestValue("dayofyear(date '2019-12-31')", TYPE_INT, 365);
  TestValue("dayofyear(date '2019-06-05')", TYPE_INT, 31 + 28 + 31 + 30 + 31 + 5);
  TestValue("dayofyear(date '2016-12-31')", TYPE_INT, 366);
  TestValue("dayofyear(date '2016-06-05')", TYPE_INT, 31 + 29 + 31 + 30 + 31 + 5);
  TestValue("dayofyear(date '9999-12-31')", TYPE_INT, 365);
  TestValue("dayofyear(date '0001-01-01')", TYPE_INT, 1);
  TestIsNull("dayofyear(cast(NULL as date))", TYPE_INT);

  // week, weekofyear
  // 2019-01-01 is Tuesday, it belongs to the first week of the year.
  TestValue("weekofyear(date '2018-12-31')", TYPE_INT, 1);
  TestValue("weekofyear(date '2019-01-01')", TYPE_INT, 1);
  TestValue("weekofyear(date '2019-01-06')", TYPE_INT, 1);
  TestValue("weekofyear(date '2019-01-07')", TYPE_INT, 2);
  TestValue("weekofyear(date '2019-06-05')", TYPE_INT, 23);
  TestValue("weekofyear(date '2019-12-23')", TYPE_INT, 52);
  TestValue("weekofyear(date '2019-12-29')", TYPE_INT, 52);
  TestValue("weekofyear(date '2019-12-30')", TYPE_INT, 1);
  // Year 2015 has 53 weeks. 2015-12-31 is Thursday.
  TestValue("weekofyear(date '2015-12-31')", TYPE_INT, 53);
  TestValue("week(date '2018-12-31')", TYPE_INT, 1);
  TestValue("week(date '2019-01-01')", TYPE_INT, 1);
  TestValue("week(date '2019-01-06')", TYPE_INT, 1);
  TestValue("week(date '2019-01-07')", TYPE_INT, 2);
  TestValue("week(date '2019-06-05')", TYPE_INT, 23);
  TestValue("week(date '2019-12-23')", TYPE_INT, 52);
  TestValue("week(date '2019-12-29')", TYPE_INT, 52);
  TestValue("week(date '2019-12-30')", TYPE_INT, 1);
  TestValue("week(date '2015-12-31')", TYPE_INT, 53);
  // 0001-01-01 is Monday. It belongs to the first week of the year.
  TestValue("weekofyear(date '0001-01-01')", TYPE_INT, 1);
  TestValue("week(date '0001-01-01')", TYPE_INT, 1);
  // 9999-12-31 is Friday. It belongs to the last week of the year.
  TestValue("weekofyear(date '9999-12-31')", TYPE_INT, 52);
  TestValue("week(date '9999-12-31')", TYPE_INT, 52);
  TestIsNull("weekofyear(cast(NULL as date))", TYPE_INT);
  TestIsNull("week(cast(NULL as date))", TYPE_INT);

  // next_day:
  // 2019-06-05 is Wednesday.
  TestDateValue("next_day(date '2019-06-05', 'monday')", DateValue(2019, 6, 10));
  TestDateValue("next_day(date '2019-06-05', 'TUE')", DateValue(2019, 6, 11));
  TestDateValue("next_day(date '2019-06-05', 'Wed')", DateValue(2019, 6, 12));
  TestDateValue("next_day(date '2019-06-05', 'THursdaY')", DateValue(2019, 6, 6));
  TestDateValue("next_day(date '2019-06-05', 'fRI')", DateValue(2019, 6, 7));
  TestDateValue("next_day(date '2019-06-05', 'saturDAY')", DateValue(2019, 6, 8));
  TestDateValue("next_day(date '2019-06-05', 'suN')", DateValue(2019, 6, 9));
  // 0001-01-01 is Monday
  TestDateValue("next_day(date '0001-01-01', 'MON')", DateValue(1, 1, 8));
  TestDateValue("next_day(date '0001-01-01', 'sunday')", DateValue(1, 1, 7));
  // 9999-12-31 is Friday
  TestDateValue("next_day(date'9999-12-30', 'FRI')", DateValue(9999, 12, 31));
  TestIsNull("next_day(date'9999-12-30', 'THU')", TYPE_DATE);
  // Date is null
  TestIsNull("next_day(cast(NULL as date), 'THU')", TYPE_DATE);
  // Invalid day
  for (const string day: { "", "S", "sa", "satu", "saturdayy" }) {
    const string expr = "next_day(date '2019-06-05', '" + day + "')";
    TestError(expr);
  }
  TestError("next_day(date '2019-06-05', NULL)");
  TestError("next_day(cast(NULL as date), NULL)");

  // last_day:
  TestDateValue("last_day(date'2019-01-11')", DateValue(2019, 1, 31));
  TestDateValue("last_day(date'2019-02-05')", DateValue(2019, 2, 28));
  TestDateValue("last_day(date'2019-04-25')", DateValue(2019, 4, 30));
  TestDateValue("last_day(date'2019-05-31')", DateValue(2019, 5, 31));
  // 2016 is leap year
  TestDateValue("last_day(date'2016-02-05')", DateValue(2016, 2, 29));
  TestDateValue("last_day(date'0001-01-01')", DateValue(1, 1, 31));
  TestDateValue("last_day(date'9999-12-31')", DateValue(9999, 12, 31));
  TestIsNull("last_day(cast(NULL as date))", TYPE_DATE);

  // years_add, years_sub:
  TestDateValue("years_add(date '0125-05-24', 0)", DateValue(125, 5, 24));
  TestDateValue("years_sub(date '0125-05-24', 0)", DateValue(125, 5, 24));
  TestDateValue("years_add(date '0125-05-24', 125)", DateValue(250, 5, 24));
  TestDateValue("years_add(date '0125-05-24', -124)", DateValue(1, 5, 24));
  TestDateValue("years_sub(date '0125-05-24', 124)", DateValue(1, 5, 24));
  // Test leap years.
  TestDateValue("years_add(date '2000-02-29', 1)", DateValue(2001, 2, 28));
  TestDateValue("years_add(date '2000-02-29', 4)", DateValue(2004, 2, 29));
  TestDateValue("years_sub(date '2000-02-29', 1)", DateValue(1999, 2, 28));
  TestDateValue("years_sub(date '2000-02-29', 4)", DateValue(1996, 2, 29));
  // Test upper and lower limit
  TestDateValue("years_add(date'0001-12-31', 9998)", DateValue(9999, 12, 31));
  TestIsNull("years_add(date'0001-12-31', 9999)", TYPE_DATE);
  TestDateValue("years_sub(date'9999-01-01', 9998)", DateValue(1, 1, 1));
  TestIsNull("years_sub(date'9999-01-01', 9999)", TYPE_DATE);
  // Test max int64
  TestIsNull("years_add(date'0001-01-01', 2147483647)", TYPE_DATE);
  TestIsNull("years_sub(date'9999-12-31', 2147483647)", TYPE_DATE);
  // Test NULL values
  TestIsNull("years_add(cast(NULL as date), 1)", TYPE_DATE);
  TestIsNull("years_add(date '2019-01-01', NULL)", TYPE_DATE);
  TestIsNull("years_add(cast(NULL as date), NULL)", TYPE_DATE);

  // months_add, add_months, months_sub:
  TestDateValue("months_add(date '0005-01-29', 0)", DateValue(5, 1, 29));
  TestDateValue("months_sub(date '0005-01-29', 0)", DateValue(5, 1, 29));
  TestDateValue("add_months(date '0005-01-29', -48)", DateValue(1, 1, 29));
  TestDateValue("months_add(date '0005-01-29', -48)", DateValue(1, 1, 29));
  TestDateValue("months_sub(date '0005-01-29', 48)", DateValue(1, 1, 29));
  TestDateValue("add_months(date '9995-01-29', 59)", DateValue(9999, 12, 29));
  TestDateValue("months_add(date '9995-01-29', 59)", DateValue(9999, 12, 29));
  TestDateValue("months_sub(date '9995-01-29', -59)", DateValue(9999, 12, 29));
  // If the input date falls on the last day of the month, the result will also always be
  // the last day of the month.
  TestDateValue("add_months(date '2000-02-29', 1)", DateValue(2000, 3, 31));
  TestDateValue("add_months(date '1999-02-28', 12)", DateValue(2000, 2, 29));
  TestDateValue("months_sub(date '2000-03-31', 1)", DateValue(2000, 2, 29));
  TestDateValue("months_add(date '2000-03-31', -2)", DateValue(2000, 1, 31));
  // Test upper and lower limit.
  // 12 * 9998 == 119976
  TestDateValue("months_add(date '0001-12-31', 119976)", DateValue(9999, 12, 31));
  TestIsNull("months_add(date'0001-12-31', 119977)", TYPE_DATE);
  TestDateValue("months_sub(date '9999-01-01', 119976)", DateValue(1, 1, 1));
  TestIsNull("months_sub(date'9999-01-01', 119977)", TYPE_DATE);
  // Test max int64
  TestIsNull("months_add(date'0001-01-01', 2147483647)", TYPE_DATE);
  TestIsNull("months_sub(date'9999-12-31', 2147483647)", TYPE_DATE);
  // Test NULL values
  TestIsNull("months_add(cast(NULL as date), 1)", TYPE_DATE);
  TestIsNull("months_add(date '2019-01-01', NULL)", TYPE_DATE);
  TestIsNull("months_add(cast(NULL as date), NULL)", TYPE_DATE);

  // weeks_add, weeks_sub:
  TestDateValue("weeks_add(date'2019-06-12', 0)", DateValue(2019, 6, 12));
  TestDateValue("weeks_sub(date'2019-06-12', 0)", DateValue(2019, 6, 12));
  TestDateValue("weeks_add(date'2019-06-12', 29)", DateValue(2020, 1, 1));
  TestDateValue("weeks_add(date'2019-06-12', -24)", DateValue(2018, 12, 26));
  TestDateValue("weeks_sub(date'2019-06-12', 24)", DateValue(2018, 12, 26));
  // Test leap year
  TestDateValue("weeks_add(date '2016-01-04', 8)", DateValue(2016, 2, 29));
  // Test upper and ower limit. There are 3652058 days between 0001-01-01 and 9999-12-31.
  // 3652058 days is 521722 weeks + 4 days.
  TestDateValue("weeks_add(date'0001-01-01', 521722)", DateValue(9999, 12, 27));
  TestIsNull("weeks_add(date'0001-01-01', 521723)", TYPE_DATE);
  TestDateValue("weeks_sub(date'9999-12-31', 521722)", DateValue(1, 1, 5));
  TestIsNull("weeks_sub(date'9999-12-31', 521723)", TYPE_DATE);
  // Test max int64
  TestIsNull("weeks_add(date'0001-01-01', 2147483647)", TYPE_DATE);
  TestIsNull("weeks_sub(date'9999-12-31', 2147483647)", TYPE_DATE);
  // Test NULL values
  TestIsNull("weeks_sub(cast(NULL as date), 1)", TYPE_DATE);
  TestIsNull("weeks_sub(date '2019-01-01', NULL)", TYPE_DATE);
  TestIsNull("weeks_sub(cast(NULL as date), NULL)", TYPE_DATE);

  // days_add, date_add, days_sub, date_sub, subdate:
  TestDateValue("days_add(date'2019-06-12', 0)", DateValue(2019, 6, 12));
  TestDateValue("days_sub(date'2019-06-12', 0)", DateValue(2019, 6, 12));
  TestDateValue("date_add(date'2019-01-01', 365)", DateValue(2020, 1, 1));
  TestDateValue("date_sub(date'2019-12-31', 365)", DateValue(2018, 12, 31));
  // Test leap year
  TestDateValue("date_add(date'2016-01-01', 366)", DateValue(2017, 1, 1));
  TestDateValue("subdate(date'2016-12-31', 366)", DateValue(2015, 12, 31));
  // Test upper and lower limit. There are 3652058 days between 0001-01-01 and 9999-12-31.
  TestDateValue("days_add(date '0001-01-01', 3652058)", DateValue(9999, 12, 31));
  TestIsNull("date_add(date '0001-01-01', 3652059)", TYPE_DATE);
  TestDateValue("days_sub(date '9999-12-31', 3652058)", DateValue(1, 1, 1));
  TestIsNull("date_sub(date '9999-12-31', 3652059)", TYPE_DATE);
  // Test max int64
  TestIsNull("days_add(date'0001-01-01', 2147483647)", TYPE_DATE);
  TestIsNull("days_sub(date'9999-12-31', 2147483647)", TYPE_DATE);
  // Test NULL values
  TestIsNull("days_add(cast(NULL as date), 1)", TYPE_DATE);
  TestIsNull("days_add(date '2019-01-01', NULL)", TYPE_DATE);
  TestIsNull("days_add(cast(NULL as date), NULL)", TYPE_DATE);

  // Interval expressions:
  // Test year interval expressions.
  TestDateValue("date_add(date '2000-02-29', interval 1 year)", DateValue(2001, 2, 28));
  TestDateValue("date_add(date '2000-02-29', interval 4 year)", DateValue(2004, 2, 29));
  TestDateValue("date_sub(date '2000-02-29', interval 1 year)", DateValue(1999, 2, 28));
  TestDateValue("date_sub(date '2000-02-29', interval 4 year)", DateValue(1996, 2, 29));
  TestDateValue("date '2000-02-29' + interval 1 year", DateValue(2001, 2, 28));
  TestDateValue("date '2000-02-29' + interval 4 years", DateValue(2004, 2, 29));
  TestDateValue("date '0001-12-31' + interval 9998 years", DateValue(9999, 12, 31));
  TestIsNull("date '0001-12-31' + interval 9999 years", TYPE_DATE);
  TestIsNull("date '0001-01-01' + interval 2147483647 years", TYPE_DATE);
  // Test month interval expressions. Keep-last-day-of-month behavior is not enforced.
  TestDateValue("date_add(date '2000-02-29', interval 1 month)", DateValue(2000, 3, 29));
  TestDateValue("date_add(date '1999-02-28', interval 12 months)",
      DateValue(2000, 2, 28));
  TestDateValue("date_sub(date '2000-03-31', interval 1 month)", DateValue(2000, 2, 29));
  TestDateValue("date_add(date '2000-03-31', interval -2 months)",
      DateValue(2000, 1, 31));
  TestDateValue("date '2000-02-29' + interval 1 month", DateValue(2000, 3, 29));
  TestDateValue("date '2000-03-31' - interval 2 months", DateValue(2000, 1, 31));
  TestDateValue("date '9999-01-01' - interval 119976 months", DateValue(1, 1, 1));
  TestIsNull("date'9999-01-01' - interval 119977 months", TYPE_DATE);
  TestIsNull("date'9999-12-31' - interval 2147483647 months", TYPE_DATE);
  // Test week interval expressions.
  TestDateValue("date_add(date'2019-06-12', interval -24 weeks)",
      DateValue(2018, 12, 26));
  TestDateValue("date_sub(date'2019-06-12', interval 24 weeks)", DateValue(2018, 12, 26));
  TestDateValue("date_add(date '2016-01-04', interval 8 weeks)", DateValue(2016, 2, 29));
  TestDateValue("date '2019-06-12' - interval 24 weeks", DateValue(2018, 12, 26));
  TestDateValue("date '2018-12-26' + interval 24 weeks", DateValue(2019, 6, 12));
  TestDateValue("date '9999-12-31' - interval 521722 weeks", DateValue(1, 1, 5));
  TestIsNull("date '9999-12-31' - interval 521723 weeks", TYPE_DATE);
  TestIsNull("date'9999-12-31' - interval 2147483647 weeks", TYPE_DATE);
  // Test day interval expressions.
  TestDateValue("date_add(date '2019-01-01', interval 365 days)", DateValue(2020, 1, 1));
  TestDateValue("date_sub(date '2016-12-31', interval 366 days)",
      DateValue(2015, 12, 31));
  TestDateValue("date '0001-01-01' + interval 3652058 days", DateValue(9999, 12, 31));
  TestIsNull("date '0001-01-01' + interval 3652059 days", TYPE_DATE);
  TestIsNull("date '9999-12-31' - interval 2147483647 days", TYPE_DATE);
  // Test NULL values.
  TestIsNull("date_add(date '2019-01-01', interval cast(NULL as BIGINT) days)",
      TYPE_DATE);
  TestIsNull("date_add(cast(NULL as date), interval 1 days)", TYPE_DATE);
  TestIsNull("date_add(cast(NULL as date), interval cast(NULL as BIGINT) days)",
      TYPE_DATE);
  TestIsNull("date '2019-01-01' - interval cast(NULL as BIGINT) days", TYPE_DATE);
  TestIsNull("cast(NULL as date) - interval 1 days", TYPE_DATE);
  TestIsNull("cast(NULL as date) - interval cast(NULL as BIGINT) days", TYPE_DATE);

  // datediff:
  TestValue("datediff(date'2019-05-12', date '2019-05-12')", TYPE_INT, 0);
  TestValue("datediff(date'2020-01-01', '2019-01-01')", TYPE_INT, 365);
  TestValue("datediff('2019-01-01', date '2020-01-01')", TYPE_INT, -365);
  // Test leap year
  TestValue("datediff(date'2021-01-01', date '2020-01-01')", TYPE_INT, 366);
  TestValue("datediff('2020-01-01', date '2021-01-01')", TYPE_INT, -366);
  // Test difference between min and max date
  TestValue("datediff(date'9999-12-31', date '0001-01-01')", TYPE_INT, 3652058);
  TestValue("datediff(date'0001-01-01', '9999-12-31')", TYPE_INT, -3652058);
  // Test NULL values
  TestIsNull("datediff(cast(NULL as DATE), date '0001-01-01')", TYPE_INT);
  TestIsNull("datediff(date'9999-12-31', cast(NULL as date))", TYPE_INT);
  TestIsNull("datediff(cast(NULL as DATE), cast(NULL as date))", TYPE_INT);

  // date_cmp:
  TestValue("date_cmp(date '2019-06-11', date '2019-06-11')", TYPE_INT, 0);
  TestValue("date_cmp(date '2019-06-11', '2019-06-12')", TYPE_INT, -1);
  TestValue("date_cmp('2019-06-12', date '2019-06-11')", TYPE_INT, 1);
  // Test NULL values
  TestIsNull("date_cmp(date '2019-06-12', cast(NULL as date))", TYPE_INT);
  TestIsNull("date_cmp(cast(NULL as date), date '2019-06-11')", TYPE_INT);
  TestIsNull("date_cmp(cast(NULL as DATE), cast(NULL as date))", TYPE_INT);
  // Test upper and lower limit
  TestValue("date_cmp(date '9999-12-31', '0001-01-01')", TYPE_INT, 1);

  // int_months_between:
  TestValue("int_months_between(date '1967-07-19','1966-06-04')", TYPE_INT, 13);
  TestValue("int_months_between('1966-06-04', date'1967-07-19')", TYPE_INT, -13);
  TestValue("int_months_between(date '1967-07-19','1967-07-19')", TYPE_INT, 0);
  TestValue("int_months_between('2015-07-19', date '2015-08-18')", TYPE_INT, 0);
  // Test lower and upper limit
  TestValue("int_months_between(date '9999-12-31','0001-01-01')", TYPE_INT,
      9998 * 12 + 11);
  // Test NULL values
  TestIsNull("int_months_between(date '1999-11-25', cast(NULL as date))", TYPE_INT);
  TestIsNull("int_months_between(cast(NULL as DATE), date '1999-11-25')", TYPE_INT);
  TestIsNull("int_months_between(cast(NULL as DATE), cast(NULL as date))", TYPE_INT);

  // months_between:
  TestValue("months_between(DATE '1967-07-19','1966-06-04')", TYPE_DOUBLE,
      13.48387096774194);
  TestValue("months_between('1966-06-04', date'1967-07-19')",
      TYPE_DOUBLE, -13.48387096774194);
  TestValue("months_between(date'1967-07-19','1967-07-19')", TYPE_DOUBLE, 0);
  TestValue("months_between(date'2015-02-28','2015-05-31')", TYPE_DOUBLE, -3);
  TestValue("months_between(date'2012-02-29','2012-01-31')", TYPE_DOUBLE, 1);
  // Test NULL values
  TestIsNull("months_between(date '1999-11-25', cast(NULL as date))", TYPE_DOUBLE);
  TestIsNull("months_between(cast(NULL as DATE), date '1999-11-25')", TYPE_DOUBLE);
  TestIsNull("months_between(cast(NULL as DATE), cast(NULL as date))", TYPE_DOUBLE);

  // current_date:
  // Test that current_date() is reasonable.
  {
    ScopedTimeZoneOverride time_zone(TEST_TZ_WITHOUT_DST);
    ScopedExecOption use_local(executor_,
        "USE_LOCAL_TZ_FOR_UNIX_TIMESTAMP_CONVERSIONS=1");
    const Timezone& local_tz = time_zone.GetTimezone();

    const boost::gregorian::date start_date =
        TimestampValue::FromUnixTimeMicros(UnixMicros(), &local_tz).date();
    DateValue current_dv =
        ConvertValue<DateValue>(GetValue("current_date()", ColumnType(TYPE_DATE)));
    const boost::gregorian::date end_date =
        TimestampValue::FromUnixTimeMicros(UnixMicros(), &local_tz).date();

    int year, month, day;
    EXPECT_TRUE(current_dv.ToYearMonthDay(&year, &month, &day));
    const boost::gregorian::date current_date(year, month, day);
    EXPECT_BETWEEN(start_date, current_date, end_date);
  }
}

TEST_P(TimestampExprTest, DateTruncTest) {
  TestTimestampValue("date_trunc('MILLENNIUM', '2016-05-08 10:30:00')",
      TimestampValue::ParseSimpleDateFormat("2001-01-01 00:00:00"));
  TestTimestampValue("date_trunc('MILLENNIUM', '3000-12-31 23:59:59.999999999')",
      TimestampValue::ParseSimpleDateFormat("2001-01-01 00:00:00"));
  TestTimestampValue("date_trunc('MILLENNIUM', '3001-01-01 00:00:00')",
      TimestampValue::ParseSimpleDateFormat("3001-01-01 00:00:00"));
  TestTimestampValue("date_trunc('CENTURY', '2016-05-08 10:30:00')",
      TimestampValue::ParseSimpleDateFormat("2001-01-01 00:00:00  "));
  TestTimestampValue("date_trunc('CENTURY', '2116-05-08 10:30:00')",
      TimestampValue::ParseSimpleDateFormat("2101-01-01 00:00:00"));
  TestTimestampValue("date_trunc('DECADE', '2116-05-08 10:30:00')",
      TimestampValue::ParseSimpleDateFormat("2110-01-01 00:00:00"));
  TestTimestampValue("date_trunc('YEAR', '2016-05-08 10:30:00')",
      TimestampValue::ParseSimpleDateFormat("2016-01-01 00:00:00"));
  TestTimestampValue("date_trunc('MONTH', '2016-05-08 00:00:00')",
      TimestampValue::ParseSimpleDateFormat("2016-05-01 00:00:00"));
  TestTimestampValue("date_trunc('WEEK', '2116-05-08 10:30:00')",
      TimestampValue::ParseSimpleDateFormat("2116-05-04 00:00:00"));
  TestTimestampValue("date_trunc('WEEK', '2017-01-01 10:37:03.455722111')",
      TimestampValue::ParseSimpleDateFormat("2016-12-26 00:00:00"));
  TestTimestampValue("date_trunc('WEEK', '2017-01-02 10:37:03.455722111')",
      TimestampValue::ParseSimpleDateFormat("2017-01-02 00:00:00"));
  TestTimestampValue("date_trunc('WEEK', '2017-01-07 10:37:03.455722111')",
      TimestampValue::ParseSimpleDateFormat("2017-01-02 00:00:00"));
  TestTimestampValue("date_trunc('WEEK', '2017-01-08 10:37:03.455722111')",
      TimestampValue::ParseSimpleDateFormat("2017-01-02 00:00:00"));
  TestTimestampValue("date_trunc('WEEK', '2017-01-09 10:37:03.455722111')",
      TimestampValue::ParseSimpleDateFormat("2017-01-09 00:00:00"));
  TestTimestampValue("date_trunc('DAY', '1416-05-08 10:37:03.455722111')",
      TimestampValue::ParseSimpleDateFormat("1416-05-08 00:00:00"));

  TestTimestampValue("date_trunc('HOUR', '1416-05-08 10:30:03.455722111')",
      TimestampValue::ParseSimpleDateFormat("1416-05-08 10:00:00"));
  TestTimestampValue("date_trunc('HOUR', '1416-05-08 23:30:03.455722111')",
      TimestampValue::ParseSimpleDateFormat("1416-05-08 23:00:00"));
  TestTimestampValue("date_trunc('MINUTE', '1416-05-08 10:37:03.455722111')",
      TimestampValue::ParseSimpleDateFormat("1416-05-08 10:37:00"));
  TestTimestampValue("date_trunc('SECOND', '1416-05-08 10:37:03.455722111')",
      TimestampValue::ParseSimpleDateFormat("1416-05-08 10:37:03"));
  TestTimestampValue("date_trunc('MILLISECONDS', '1416-05-08 10:37:03.455722111')",
      TimestampValue::ParseSimpleDateFormat("1416-05-08 10:37:03.455000000"));
  TestTimestampValue("date_trunc('MICROSECONDS', '1416-05-08 10:37:03.455722111')",
      TimestampValue::ParseSimpleDateFormat("1416-05-08 10:37:03.455722000"));

  // Test corner cases.
  TestTimestampValue("date_trunc('MILLENNIUM', '9999-12-31 23:59:59.999999999')",
      TimestampValue::ParseSimpleDateFormat("9001-01-01 00:00:00"));
  TestTimestampValue("date_trunc('CENTURY', '9999-12-31 23:59:59.999999999')",
      TimestampValue::ParseSimpleDateFormat("9901-01-01 00:00:00"));
  TestTimestampValue("date_trunc('DECADE', '9999-12-31 23:59:59.999999999')",
      TimestampValue::ParseSimpleDateFormat("9990-01-01 00:00:00"));
  TestTimestampValue("date_trunc('YEAR', '9999-12-31 23:59:59.999999999')",
      TimestampValue::ParseSimpleDateFormat("9999-01-01 00:00:00"));
  TestTimestampValue("date_trunc('MONTH', '9999-12-31 23:59:59.999999999')",
      TimestampValue::ParseSimpleDateFormat("9999-12-01 00:00:00"));
  TestTimestampValue("date_trunc('WEEK', '9999-12-31 23:59:59.999999999')",
      TimestampValue::ParseSimpleDateFormat("9999-12-27 00:00:00"));
  TestTimestampValue("date_trunc('DAY', '9999-12-31 23:59:59.999999999')",
      TimestampValue::ParseSimpleDateFormat("9999-12-31 00:00:00"));
  TestTimestampValue("date_trunc('HOUR', '9999-12-31 23:59:59.999999999')",
      TimestampValue::ParseSimpleDateFormat("9999-12-31 23:00:00"));
  TestTimestampValue("date_trunc('MINUTE', '9999-12-31 23:59:59.999999999')",
      TimestampValue::ParseSimpleDateFormat("9999-12-31 23:59:00"));
  TestTimestampValue("date_trunc('SECOND', '9999-12-31 23:59:59.999999999')",
      TimestampValue::ParseSimpleDateFormat("9999-12-31 23:59:59"));
  TestTimestampValue("date_trunc('MILLISECONDS', '9999-12-31 23:59:59.999999999')",
      TimestampValue::ParseSimpleDateFormat("9999-12-31 23:59:59.999"));
  TestTimestampValue("date_trunc('MICROSECONDS', '9999-12-31 23:59:59.999999999')",
      TimestampValue::ParseSimpleDateFormat("9999-12-31 23:59:59.999999"));

  TestTimestampValue("date_trunc('DECADE', '1400-01-01 00:00:00')",
      TimestampValue::ParseSimpleDateFormat("1400-01-01 00:00:00"));
  TestTimestampValue("date_trunc('YEAR', '1400-01-01 00:00:00')",
      TimestampValue::ParseSimpleDateFormat("1400-01-01 00:00:00"));
  TestTimestampValue("date_trunc('MONTH', '1400-01-01 00:00:00')",
      TimestampValue::ParseSimpleDateFormat("1400-01-01 00:00:00"));
  TestTimestampValue("date_trunc('DAY', '1400-01-01 00:00:00')",
      TimestampValue::ParseSimpleDateFormat("1400-01-01 00:00:00"));
  TestTimestampValue("date_trunc('HOUR', '1400-01-01 00:00:00')",
      TimestampValue::ParseSimpleDateFormat("1400-01-01 00:00:00"));
  TestTimestampValue("date_trunc('MINUTE', '1400-01-01 00:00:00')",
      TimestampValue::ParseSimpleDateFormat("1400-01-01 00:00:00"));
  TestTimestampValue("date_trunc('SECOND', '1400-01-01 00:00:00')",
      TimestampValue::ParseSimpleDateFormat("1400-01-01 00:00:00"));
  TestTimestampValue("date_trunc('MILLISECONDS', '1400-01-01 00:00:00')",
      TimestampValue::ParseSimpleDateFormat("1400-01-01 00:00:00"));
  TestTimestampValue("date_trunc('MICROSECONDS', '1400-01-01 00:00:00')",
      TimestampValue::ParseSimpleDateFormat("1400-01-01 00:00:00"));

  // Test lower limit for century
  TestIsNull("date_trunc('CENTURY', '1400-01-01 00:00:00')", TYPE_TIMESTAMP);
  TestIsNull("date_trunc('CENTURY', '1400-12-31 23:59:59.999999999')", TYPE_TIMESTAMP);
  TestTimestampValue("date_trunc('CENTURY', '1401-01-01 00:00:00')",
      TimestampValue::ParseSimpleDateFormat("1401-01-01 00:00:00"));

  // Test lower limit for millennium
  TestIsNull("date_trunc('MILLENNIUM', '1400-01-01 00:00:00')", TYPE_TIMESTAMP);
  TestIsNull("date_trunc('MILLENNIUM', '2000-12-31 23:59:59.999999999')", TYPE_TIMESTAMP);
  TestTimestampValue("date_trunc('MILLENNIUM', '2001-01-01 00:00:00')",
      TimestampValue::ParseSimpleDateFormat("2001-01-01 00:00:00"));

  // Test lower limit for week
  TestIsNull("date_trunc('WEEK', '1400-01-01 00:00:00')", TYPE_TIMESTAMP);
  TestIsNull("date_trunc('WEEK', '1400-01-05 23:59:59.999999999')", TYPE_TIMESTAMP);
  TestTimestampValue("date_trunc('WEEK', '1400-01-06 00:00:00')",
      TimestampValue::ParseSimpleDateFormat("1400-01-06 00:00:00"));
  TestTimestampValue("date_trunc('WEEK', '1400-01-07 23:59:59.999999999')",
      TimestampValue::ParseSimpleDateFormat("1400-01-06 00:00:00"));

  // Test invalid input.
  TestIsNull("date_trunc('HOUR', '12202010')", TYPE_TIMESTAMP);
  TestIsNull("date_trunc('HOUR', '')", TYPE_TIMESTAMP);
  TestIsNull("date_trunc('HOUR', NULL)", TYPE_TIMESTAMP);
  TestIsNull("date_trunc('HOUR', '02-13-2014')", TYPE_TIMESTAMP);
  TestIsNull("date_trunc('CENTURY', '16-05-08 10:30:00')", TYPE_TIMESTAMP);
  TestIsNull("date_trunc('CENTURY', '1116-05-08 10:30:00')", TYPE_TIMESTAMP);
  TestIsNull("date_trunc('DAY', '00:00:00')", TYPE_TIMESTAMP);
  TestError("date_trunc('YsEAR', '2016-05-08 10:30:00')");
  TestError("date_trunc('D', '2116-05-08 10:30:00')");
  TestError("date_trunc('2017-01-09', '2017-01-09 10:37:03.455722111' )");
  TestError("date_trunc('2017-01-09 10:00:00', 'HOUR')");
}

} // namespace impala

INSTANTIATE_TEST_CASE_P(Instantiations, TimestampExprTest, ::testing::Values(
  //              disable_codegen  enable_expr_rewrites
  std::make_tuple(true,            false),
  std::make_tuple(false,           false),
  std::make_tuple(true,            true)));
  // Note: the false/true case is not tested because it provides very little
  // incremental coverage but adds a lot to test runtime (this test is quite
  // slow when codegen is enabled).
  //
  // Mostly we get the best backend codegened/interpreted coverage from running
  // the unmodified (i.e. more complex) expr trees enabled. Running the trees
  // in interpreted mode should be enough to validate correctness of the
  // rewrites. So enabling the remaining combination might provide some
  // additional incidental coverage from codegening some additional expr tree
  // shapes but mostly it isn't that interesting since the majority of
  // expressions get folded to a constant anyway.
