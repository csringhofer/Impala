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

package org.apache.impala.analysis;

import com.google.common.collect.Lists;
import com.google.common.base.Enums;

import org.apache.impala.catalog.BuiltinsDb;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;

import java.util.stream.Collectors;
import java.util.Arrays;

/**
 * Representation of the following TRIM expressions:
 * TRIM(<Where> FROM <String_to_be_trimmed>)
 * TRIM(<Characters> FROM <String_to_be_trimmed>)
 * TRIM(<Where> <Characters> FROM <String_to_be_trimmed>)
 * Such forms of TRIM are not handled by FunctionCallExpr.
 *
 * Given class serves as a syntax wrapper around FunctionCallExpr, which instantiates
 * a corresponding *trim function with regular syntax based on the arguments.
 */
public class TrimFromExpr extends FunctionCallExpr {
  private final FunctionName trimFromFnName_;
  private final TrimOption where_;
  private final Expr charset_;
  private final Expr srcExpr_;
  private String slot_ = null;

  // Trim option set.
  public enum TrimOption {
    LEADING,
    TRAILING,
    BOTH
  }

  private static FunctionName baseFnName(FunctionName fnName, TrimOption where) {
    String fn = fnName.toString();
    if (where == null) {
      // Default to BOTH if where is null
      where = TrimOption.BOTH;
    }
    switch (where) {
      case LEADING:
        fn = fn.replaceAll("trim", "ltrim");
        break;
      case TRAILING:
        fn = fn.replaceAll("trim", "rtrim");
        break;
      case BOTH:
      default:
        fn = fn.replaceAll("trim", "btrim");
        break;
    }
    return new FunctionName(fn);
  }

  // For "TRIM(<where> FROM <string>)"
  public TrimFromExpr(FunctionName fnName, TrimOption where, Expr srcexpr) {
    this(fnName, where, null, srcexpr);
  }

  // For "TRIM(<charset> FROM <string>)"
  public TrimFromExpr(FunctionName fnName, Expr charset, Expr srcexpr) {
    this(fnName, null, charset, srcexpr);
  }

  // For special case: "select trim(table_column from 'string') from table"
  public TrimFromExpr(FunctionName fnName, String slot, Expr srcexpr) {
    this(fnName, null, null, srcexpr);
    slot_ = slot;
  }

  // For generic "TRIM(where <charset> FROM <string>)"
  public TrimFromExpr(FunctionName fnName, TrimOption where, Expr charset, Expr srcexpr) {
    super(baseFnName(fnName, where),
        charset == null ? Lists.newArrayList(srcexpr) :
                          Lists.newArrayList(srcexpr, charset));
    trimFromFnName_ = fnName;
    where_ = where;
    charset_ = charset;
    srcExpr_ = srcexpr;
    type_ = Type.STRING;
  }

  /**
   * Copy c'tor used in clone().
   */
  protected TrimFromExpr(TrimFromExpr other) {
    super(other);
    trimFromFnName_ = other.trimFromFnName_;
    where_ = other.where_;
    charset_ = other.charset_;
    srcExpr_ = other.srcExpr_;
    slot_ = other.slot_;
  }

  @Override
  protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
    // Do these sanity checks before calling the super class to get the expected error
    // messages if trim() is used in an invalid way.
    trimFromFnName_.analyze(analyzer);
    if (!trimFromFnName_.getFunction().equals("trim")
        && !trimFromFnName_.getFunction().equals("utf8_trim")) {
      throw new AnalysisException("Function "
          + trimFromFnName_.getFunction().toUpperCase()
          + " does not accept the keyword FROM.");
    }
    if ((trimFromFnName_.getDb() != null)
        && !trimFromFnName_.getDb().equals(BuiltinsDb.NAME)) {
      throw new AnalysisException("Function "
          + trimFromFnName_.toString() + " conflicts "
          + "with the TRIM builtin.");
    }

    super.analyzeImpl(analyzer);

    if (slot_ != null) {
      SlotRef slotRef = new SlotRef(Arrays.asList(slot_));
      try {
        slotRef.analyze(analyzer);
      } catch (AnalysisException e) {
        // In case 'where' parameter cannot be interpreted as a slot, trim option error
        // is more informative.
        throw new AnalysisException("Trim option '" + slot_ + "' in expression '"
            + toSql() + "' is invalid. Expected one of: "
            + Arrays.stream(TrimOption.values()).map(option -> option.name())
            .collect(Collectors.joining(", "))
            + ".");
      }
      if (super.getParams().size() == 1) {
        super.getParams().exprs().add(slotRef);
        super.children_.add(slotRef);
      }
    }
  }

  @Override
  protected String getFunctionNotFoundError(Type[] argTypes) {
    StringBuilder errMsg = new StringBuilder();
    if (charset_ != null && charset_.getType() != Type.STRING) {
      errMsg.append("Expression '" + charset_.toSql() + "' has a return type of ");
      errMsg.append(charset_.getType().toSql() + " but a STRING is required.");
    }
    if (srcExpr_ != null && srcExpr_.getType() != Type.STRING) {
      if (errMsg.length() > 0) errMsg.append(" ");
      errMsg.append("Expression '" + srcExpr_.toSql() + "' has a return type of ");
      errMsg.append(srcExpr_.getType().toSql() + " but a STRING is required.");
    }
    return errMsg.toString();
  }

  @Override
  public String toSqlImpl(ToSqlOptions options) {
    StringBuilder strBuilder = new StringBuilder();
    strBuilder.append(trimFromFnName_).append("(");
    if (where_ != null) {
      strBuilder.append(new StringLiteral(where_.name()).getValueWithOriginalEscapes());
      strBuilder.append(" ");
    }
    if (slot_ != null) {
      strBuilder.append(new StringLiteral(slot_).getValueWithOriginalEscapes());
      strBuilder.append(" ");
    }
    if (charset_ != null) {
      strBuilder.append(charset_.toSql(options));
      strBuilder.append(" ");
    }
    strBuilder.append("FROM ");
    strBuilder.append(srcExpr_.toSql(options));
    strBuilder.append(")");
    return strBuilder.toString();
  }

  @Override
  public Expr clone() { return new TrimFromExpr(this); }
}
