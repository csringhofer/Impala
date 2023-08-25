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

package org.apache.impala.planner;

import java.util.ArrayList;
import java.util.List;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.ExprSubstitutionMap;
import org.apache.impala.thrift.TDataPartition;
import org.apache.impala.thrift.TLocalPartitioningRole;
import org.apache.impala.thrift.TPartitionType;
import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Specification of the partition of a single stream of data.
 * Examples of those streams of data are: the scan of a table; the output
 * of a plan fragment; etc. (ie, this is not restricted to direct exchanges
 * between two fragments, which in the backend is facilitated by the classes
 * DataStreamSender/DataStreamMgr/DataStreamRecvr).
 */
public class DataPartition {
  private final TPartitionType type_;

  // Used for any partitioning that requires computing the partition.
  // Always non-null.
  private List<Expr> partitionExprs_;

  private TLocalPartitioningRole localPartitioningRole_ = TLocalPartitioningRole.NONE;

  private DataPartition(TPartitionType type, List<Expr> exprs,
      TLocalPartitioningRole localPartitioningRole) {
    Preconditions.checkNotNull(exprs);
    Preconditions.checkState(!exprs.isEmpty());
    Preconditions.checkState(type == TPartitionType.HASH_PARTITIONED
        || type == TPartitionType.RANGE_PARTITIONED
        || type == TPartitionType.KUDU);
    type_ = type;
    partitionExprs_ = exprs;
    localPartitioningRole_ = localPartitioningRole;
  }

  private DataPartition(TPartitionType type) {
    Preconditions.checkState(type == TPartitionType.UNPARTITIONED
        || type == TPartitionType.RANDOM);
    type_ = type;
    partitionExprs_ = new ArrayList<>();
  }

  public final static DataPartition UNPARTITIONED =
      new DataPartition(TPartitionType.UNPARTITIONED);

  public final static DataPartition RANDOM =
      new DataPartition(TPartitionType.RANDOM);

  public static DataPartition hashPartitioned(List<Expr> expr) {
    return new DataPartition(TPartitionType.HASH_PARTITIONED, expr,
        TLocalPartitioningRole.NONE);
  }

  public static DataPartition kuduPartitioned(Expr expr) {
    return new DataPartition(TPartitionType.KUDU, Lists.newArrayList(expr), 
        TLocalPartitioningRole.NONE);
  }

  public boolean isPartitioned() { return type_ != TPartitionType.UNPARTITIONED; }
  public boolean isHashPartitioned() { return type_ == TPartitionType.HASH_PARTITIONED; }
  public TPartitionType getType() { return type_; }
  public List<Expr> getPartitionExprs() { return partitionExprs_; }

  public void setLocalPartitioningRole(TLocalPartitioningRole role) {
    localPartitioningRole_ = role;
  }

  public void substitute(ExprSubstitutionMap smap, Analyzer analyzer) {
    partitionExprs_ = Expr.substituteList(partitionExprs_, smap, analyzer, false);
  }

  public TDataPartition toThrift() {
    TDataPartition result = new TDataPartition(type_);
    if (partitionExprs_ != null) {
      result.setPartition_exprs(Expr.treesToThrift(partitionExprs_));
    }
    result.setLocal_partitioning_role(localPartitioningRole_);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) return false;
    if (obj.getClass() != this.getClass()) return false;
    DataPartition other = (DataPartition) obj;
    if (type_ != other.type_) return false;
    if (localPartitioningRole_ != other.localPartitioningRole_) return false;
    return Expr.equalLists(partitionExprs_, other.partitionExprs_);
  }

  public String debugString() {
    return MoreObjects.toStringHelper(this)
        .add("type_", type_)
        .addValue(Expr.debugString(partitionExprs_))
        .add("localPartitioningRole_", localPartitioningRole_)
        .toString();
  }

  public String getExplainString() {
    StringBuilder str = new StringBuilder();
    if (localPartitioningRole_ != TLocalPartitioningRole.NONE) str.append("LOCAL ");
    str.append(getPartitionShortName(type_));
    if (!partitionExprs_.isEmpty()) {
      List<String> strings = new ArrayList<>();
      for (Expr expr: partitionExprs_) {
        strings.add(expr.toSql());
      }
      str.append("(" + Joiner.on(",").join(strings) +")");
    }
    return str.toString();
  }

  private String getPartitionShortName(TPartitionType partition) {
    switch (partition) {
      case RANDOM: return "RANDOM";
      case HASH_PARTITIONED: return "HASH";
      case RANGE_PARTITIONED: return "RANGE";
      case UNPARTITIONED: return "UNPARTITIONED";
      case KUDU: return "KUDU";
      default: return "";
    }
  }
}
