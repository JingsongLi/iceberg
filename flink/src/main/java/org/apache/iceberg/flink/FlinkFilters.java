/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.flink;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expression.Operation;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.Pair;

import static org.apache.iceberg.expressions.Expressions.and;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.in;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.not;
import static org.apache.iceberg.expressions.Expressions.notEqual;
import static org.apache.iceberg.expressions.Expressions.or;
import static org.apache.iceberg.expressions.Expressions.startsWith;

public class FlinkFilters {
  private FlinkFilters() {
  }

  private static final ImmutableMap<FunctionDefinition, Operation> FILTERS = ImmutableMap
      .<FunctionDefinition, Operation>builder()
      .put(BuiltInFunctionDefinitions.EQUALS, Operation.EQ)
      .put(BuiltInFunctionDefinitions.NOT_EQUALS, Operation.NOT_EQ)
      .put(BuiltInFunctionDefinitions.GREATER_THAN, Operation.GT)
      .put(BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL, Operation.GT_EQ)
      .put(BuiltInFunctionDefinitions.LESS_THAN, Operation.LT)
      .put(BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL, Operation.LT_EQ)
      .put(BuiltInFunctionDefinitions.IN, Operation.IN)
      .put(BuiltInFunctionDefinitions.IS_NULL, Operation.IS_NULL)
      .put(BuiltInFunctionDefinitions.IS_NOT_NULL, Operation.NOT_NULL)
      .put(BuiltInFunctionDefinitions.AND, Operation.AND)
      .put(BuiltInFunctionDefinitions.OR, Operation.OR)
      .put(BuiltInFunctionDefinitions.NOT, Operation.NOT)
      .build();

  private static final Pattern STARTS_WITH_PATTERN = Pattern.compile("([^%]+)%");

  public static Expression convert(org.apache.flink.table.expressions.Expression expression) {
    if (!(expression instanceof CallExpression)) {
      return null;
    }

    CallExpression call = (CallExpression) expression;
    List<org.apache.flink.table.expressions.Expression> children = call.getChildren();
    FunctionDefinition definition = call.getFunctionDefinition();
    Operation op = FILTERS.get(definition);
    if (op != null) {
      switch (op) {
        case TRUE:
          return Expressions.alwaysTrue();

        case FALSE:
          return Expressions.alwaysFalse();

        case IS_NULL:
          return toReference(children.get(0)).map(Expressions::isNull).orElse(null);

        case NOT_NULL:
          return toReference(children.get(0)).map(Expressions::notNull).orElse(null);

        case LT:
          return refAndLiteral(children).map(pair -> lessThan(pair.first(), pair.second())).orElse(null);

        case LT_EQ:
          return refAndLiteral(children).map(pair -> lessThanOrEqual(pair.first(), pair.second())).orElse(null);

        case GT:
          return refAndLiteral(children).map(pair -> greaterThan(pair.first(), pair.second())).orElse(null);

        case GT_EQ:
          return refAndLiteral(children).map(pair -> greaterThanOrEqual(pair.first(), pair.second())).orElse(null);

        case EQ:
          return refAndLiteral(children).map(pair -> equal(pair.first(), pair.second())).orElse(null);

        case NOT_EQ:
          return refAndLiteral(children).map(pair -> notEqual(pair.first(), pair.second())).orElse(null);

        case IN:
          return refAndLiterals(children).map(pair -> in(pair.first(), pair.second())).orElse(null);

        case NOT:
          Expression child = convert(children.get(0));
          if (child != null) {
            return not(child);
          }
          return null;

        case AND: {
          Expression left = convert(children.get(0));
          Expression right = convert(children.get(1));
          if (left != null && right != null) {
            return and(left, right);
          }
          return null;
        }

        case OR: {
          Expression left = convert(children.get(0));
          Expression right = convert(children.get(1));
          if (left != null && right != null) {
            return or(left, right);
          }
          return null;
        }
      }
    }

    // optimizer for STARTS_WITH
    if (definition.equals(BuiltInFunctionDefinitions.LIKE) && children.size() == 2) {
      Optional<String> ref = toReference(children.get(0));
      Optional<Object> literal = toLiteral(children.get(1));

      if (ref.isPresent() && literal.isPresent()) {
        String pattern = literal.get().toString();
        Matcher matcher = STARTS_WITH_PATTERN.matcher(pattern);

        // exclude special char of like
        if (!pattern.contains("_") && matcher.matches()) {
          return startsWith(ref.get(), matcher.group(1));
        }
      }
    }

    if (definition.equals(BuiltInFunctionDefinitions.BETWEEN)) {
      Optional<String> ref = toReference(children.get(0));
      Optional<Object> lit1 = toLiteral(children.get(1));
      Optional<Object> lit2 = toLiteral(children.get(2));
      if (ref.isPresent() && lit1.isPresent() && lit2.isPresent()) {
        return and(greaterThanOrEqual(ref.get(), lit1.get()), lessThanOrEqual(ref.get(), lit2.get()));
      }
    }

    if (definition.equals(BuiltInFunctionDefinitions.NOT_BETWEEN)) {
      Optional<String> ref = toReference(children.get(0));
      Optional<Object> lit1 = toLiteral(children.get(1));
      Optional<Object> lit2 = toLiteral(children.get(2));
      if (ref.isPresent() && lit1.isPresent() && lit2.isPresent()) {
        return or(lessThan(ref.get(), lit1.get()), greaterThan(ref.get(), lit2.get()));
      }
    }

    // IS_TRUE IS_FALSE IS_NOT_TRUE IS_NOT_FALSE

    return null;
  }

  private static Optional<String> toReference(org.apache.flink.table.expressions.Expression expression) {
    return expression instanceof FieldReferenceExpression ?
        Optional.of(((FieldReferenceExpression) expression).getName()) :
        Optional.empty();
  }

  private static Optional<Object> toLiteral(org.apache.flink.table.expressions.Expression expression) {
    // Not support null literal
    return expression instanceof ValueLiteralExpression ?
        convertLiteral((ValueLiteralExpression) expression) :
        Optional.empty();
  }

  private static Optional<Pair<String, Object>> refAndLiteral(
      List<org.apache.flink.table.expressions.Expression> expressions) {
    Optional<String> ref0 = toReference(expressions.get(0));
    Optional<String> ref1 = toReference(expressions.get(1));
    Optional<Object> lit0 = toLiteral(expressions.get(0));
    Optional<Object> lit1 = toLiteral(expressions.get(1));
    if (ref0.isPresent() && lit1.isPresent()) {
      return Optional.of(Pair.of(ref0.get(), lit1.get()));
    } else if (ref1.isPresent() && lit0.isPresent()) {
      return Optional.of(Pair.of(ref1.get(), lit0.get()));
    } else {
      return Optional.empty();
    }
  }

  private static Optional<Pair<String, List<Object>>> refAndLiterals(
      List<org.apache.flink.table.expressions.Expression> expressions) {
    Optional<String> ref = toReference(expressions.get(0));
    Optional<List<Object>> literals = toLiterals(expressions.subList(1, expressions.size()));

    if (ref.isPresent() && literals.isPresent()) {
      return Optional.of(Pair.of(ref.get(), literals.get()));
    }
    return Optional.empty();
  }

  private static Optional<List<Object>> toLiterals(
      List<org.apache.flink.table.expressions.Expression> expressions) {
    List<Object> literals = Lists.newArrayListWithCapacity(expressions.size());
    for (org.apache.flink.table.expressions.Expression expression : expressions) {
      Optional<Object> literal = toLiteral(expression);
      if (literal.isPresent()) {
        literals.add(literal.get());
      } else {
        return Optional.empty();
      }
    }
    return Optional.of(literals);
  }

  private static Optional<Object> convertLiteral(ValueLiteralExpression expression) {
    Optional<?> value = expression.getValueAs(expression.getOutputDataType().getLogicalType().getDefaultConversion());
    return value.map(o -> {
      if (o instanceof LocalDateTime) {
        return DateTimeUtil.microsFromTimestamp((LocalDateTime) o);
      } else if (o instanceof LocalTime) {
        return DateTimeUtil.microsFromTime((LocalTime) o);
      } else if (o instanceof LocalDate) {
        return DateTimeUtil.daysFromDate((LocalDate) o);
      }
      return o;
    });
  }
}
