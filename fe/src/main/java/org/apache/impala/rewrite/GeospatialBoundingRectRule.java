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

package org.apache.impala.rewrite;

import java.util.Arrays;
import java.util.List;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.CompoundPredicate;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.FunctionCallExpr;
import org.apache.impala.common.AnalysisException;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Examples:
 *  st_intersect(CONST_GEOM, st_point(x, y))
 *    -> x >= st_minx(CONST_GEOM) AND y >= st_minx(CONST_GEOM) AND
 *       x <= st_maxx(CONST_GEOM) AND y <= st_maxy(CONST_GEOM) AND
 *       st_intersect(CONST_GEOM, st_point(x, y))
 */
public class GeospatialBoundingRectRule implements ExprRewriteRule {
  public static ExprRewriteRule INSTANCE = new GeospatialBoundingRectRule();

  @Override
  public Expr apply(Expr expr, Analyzer analyzer) throws AnalysisException {
    if (!(expr instanceof FunctionCallExpr)) return expr;
    FunctionCallExpr fn = (FunctionCallExpr) expr;
    if (!fn.isGeoSpatial()) return expr;
    String name = fn.getFnName().getFunction();
    boolean isContains = name.equalsIgnoreCase("st_contains");
    boolean isSymetricRel = false;
    List<String> symmetric_rels = Arrays.asList(
      "st_crosses", "st_equals", "st_intersects", "st_overlaps", "st_touches");

    //DECLARE_RELATION(Disjoint)
    //DECLARE_RELATION(Within)
    if (!isContains) {
      for (String rel: symmetric_rels) {
        if (name.equalsIgnoreCase(rel)) {
          isSymetricRel = true;
          break;
        }
      }
      if (!isSymetricRel) return expr;
    }
    Expr constChild = null;
    Expr geomChild = null;
    if (expr.getChild(0).isConstant()) {
      // no need to apply if both children are const
      if (expr.getChild(1).isConstant()) return expr;
      constChild = expr.getChild(0);
      geomChild = expr.getChild(1);
    } else {
      if (!expr.getChild(1).isConstant()) return expr;
      if (isContains) return expr; // First child must be const for st_contains 
      constChild = expr.getChild(1);
      geomChild = expr.getChild(0);
    }

    if (!constChild.getType().isBinary()) return expr;
    if (!(geomChild instanceof FunctionCallExpr)) return expr;
    FunctionCallExpr pointFn = (FunctionCallExpr) geomChild;
    if (!pointFn.getFnName().getFunction().equalsIgnoreCase("st_point")) return expr;
    if (pointFn.getChildCount() != 2 
        || !pointFn.getChild(0).getType().isFloatingPointType() 
        || !pointFn.getChild(1).getType().isFloatingPointType()) {
      return expr;
    }
    Expr x = pointFn.getChild(0).clone();
    Expr y = pointFn.getChild(1).clone();
    Expr minx = new FunctionCallExpr("st_minx",  Arrays.asList(constChild.clone()));
    Expr miny = new FunctionCallExpr("st_miny",  Arrays.asList(constChild.clone()));
    Expr maxx = new FunctionCallExpr("st_maxx",  Arrays.asList(constChild.clone()));
    Expr maxy = new FunctionCallExpr("st_maxy",  Arrays.asList(constChild.clone()));
    List<Expr> predicates = Arrays.asList(
      new BinaryPredicate(BinaryPredicate.Operator.LE, minx, x),
      new BinaryPredicate(BinaryPredicate.Operator.LE, miny, y),
      new BinaryPredicate(BinaryPredicate.Operator.GE, maxx, x),
      new BinaryPredicate(BinaryPredicate.Operator.GE, maxy, y),
      expr.clone()
    );
    return CompoundPredicate.createConjunctivePredicate(predicates);
  }

  private GeospatialBoundingRectRule() {}
}
