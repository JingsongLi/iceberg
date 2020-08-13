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

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.expressions.ApiExpressionUtils;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.UnresolvedCallExpression;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.expressions.utils.ApiExpressionDefaultVisitor;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.iceberg.expressions.And;
import org.apache.iceberg.expressions.Not;
import org.apache.iceberg.expressions.Or;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.util.DateTimeUtil;
import org.junit.Test;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;
import static org.junit.Assert.assertEquals;

public class TestFlinkFilters {

  private static final TableSchema SCHEMA = TableSchema.builder()
      .field("f0", DataTypes.BOOLEAN())
      .field("f1", DataTypes.INT())
      .field("f2", DataTypes.BIGINT())
      .field("f3", DataTypes.FLOAT())
      .field("f4", DataTypes.DOUBLE())
      .field("f5", DataTypes.STRING())
      .field("f6", DataTypes.BYTES())
      .field("f7", DataTypes.BINARY(3))
      .field("f8", DataTypes.DECIMAL(38, 18))
      .field("f9", DataTypes.DATE())
      .field("f10", DataTypes.TIME())
      .field("f11", DataTypes.TIMESTAMP())
      .build();

  @Test
  public void testEquals() {
    innerTestLiteral("f0", true, true);
    innerTestLiteral("f1", 5, 5);
    innerTestLiteral("f2", 5L, 5L);
    innerTestLiteral("f3", 5.5f, 5.5f);
    innerTestLiteral("f4", 5.5, 5.5);
    innerTestLiteral("f5", "str", "str");
    innerTestLiteral("f6", new byte[] {1, 2}, new byte[] {1, 2});
    innerTestLiteral("f7", new byte[] {1, 2}, new byte[] {1, 2});
    innerTestLiteral("f8", BigDecimal.valueOf(55), BigDecimal.valueOf(55));

    LocalDate date = LocalDate.parse("2020-05-05");
    innerTestLiteral("f9", date, DateTimeUtil.daysFromDate(date));

    LocalTime time = LocalTime.parse("05:05:05");
    innerTestLiteral("f10", time, DateTimeUtil.microsFromTime(time));

    LocalDateTime dateTime = LocalDateTime.parse("2020-05-05T05:05:05");
    innerTestLiteral("f11", dateTime, DateTimeUtil.microsFromTimestamp(dateTime));
  }

  private void innerTestLiteral(String field, Object flinkObj, Object icebergObj) {
    Expression expr = resolve($(field).isEqual(lit(flinkObj)));
    UnboundPredicate actual = (UnboundPredicate) FlinkFilters.convert(expr);
    assertPredicatesMatch(org.apache.iceberg.expressions.Expressions.equal(field, icebergObj), actual);
  }

  @Test
  public void testNotEquals() {
    Expression expr = resolve($("f0").isNotEqual(lit(true)));
    UnboundPredicate actual = (UnboundPredicate) FlinkFilters.convert(expr);
    assertPredicatesMatch(org.apache.iceberg.expressions.Expressions.notEqual("f0", true), actual);
  }

  @Test
  public void testLessThan() {
    Expression expr = resolve($("f1").isLess(lit(5)));
    UnboundPredicate actual = (UnboundPredicate) FlinkFilters.convert(expr);
    assertPredicatesMatch(org.apache.iceberg.expressions.Expressions.lessThan("f1", 5), actual);
  }

  @Test
  public void testLessThanEquals() {
    Expression expr = resolve($("f1").isLessOrEqual(lit(5)));
    UnboundPredicate actual = (UnboundPredicate) FlinkFilters.convert(expr);
    assertPredicatesMatch(org.apache.iceberg.expressions.Expressions.lessThanOrEqual("f1", 5), actual);
  }

  @Test
  public void testGreaterThan() {
    Expression expr = resolve($("f1").isGreater(lit(5)));
    UnboundPredicate actual = (UnboundPredicate) FlinkFilters.convert(expr);
    assertPredicatesMatch(org.apache.iceberg.expressions.Expressions.greaterThan("f1", 5), actual);
  }

  @Test
  public void testGreaterThanEquals() {
    Expression expr = resolve($("f1").isGreaterOrEqual(lit(5)));
    UnboundPredicate actual = (UnboundPredicate) FlinkFilters.convert(expr);
    assertPredicatesMatch(org.apache.iceberg.expressions.Expressions.greaterThanOrEqual("f1", 5), actual);
  }

  @Test
  public void testIn() {
    Expression expr = resolve($("f1").in(lit(5), lit(6), lit(7)));
    UnboundPredicate actual = (UnboundPredicate) FlinkFilters.convert(expr);
    UnboundPredicate<Integer> expected = org.apache.iceberg.expressions.Expressions.in("f1", 5, 6, 7);

    assertEquals(expected.op(), actual.op());
    assertEquals(expected.literals(), actual.literals());
    assertEquals(expected.ref().name(), actual.ref().name());
  }

  @Test
  public void testBetween() {
    Expression expr = resolve($("f1").between(lit(5), lit(6)));
    And actual = (And) FlinkFilters.convert(expr);
    And expected = (And) org.apache.iceberg.expressions.Expressions.and(
        org.apache.iceberg.expressions.Expressions.greaterThanOrEqual("f1", 5),
        org.apache.iceberg.expressions.Expressions.lessThanOrEqual("f1", 6));

    assertEquals(expected.op(), actual.op());
    assertEquals(expected.left().op(), actual.left().op());
    assertEquals(expected.right().op(), actual.right().op());
  }

  @Test
  public void testNotBetween() {
    Expression expr = resolve($("f1").notBetween(lit(5), lit(6)));
    Or actual = (Or) FlinkFilters.convert(expr);
    Or expected = (Or) org.apache.iceberg.expressions.Expressions.or(
        org.apache.iceberg.expressions.Expressions.lessThan("f1", 5),
        org.apache.iceberg.expressions.Expressions.greaterThan("f1", 6));

    assertEquals(expected.op(), actual.op());
    assertEquals(expected.left().op(), actual.left().op());
    assertEquals(expected.right().op(), actual.right().op());
  }

  @Test
  public void testIsNull() {
    Expression expr = resolve($("f1").isNull());
    UnboundPredicate actual = (UnboundPredicate) FlinkFilters.convert(expr);
    UnboundPredicate<Object> expected = org.apache.iceberg.expressions.Expressions.isNull("f1");

    assertEquals(expected.op(), actual.op());
    assertEquals(expected.ref().name(), actual.ref().name());
  }

  @Test
  public void testIsNotNull() {
    Expression expr = resolve($("f1").isNotNull());
    UnboundPredicate actual = (UnboundPredicate) FlinkFilters.convert(expr);
    UnboundPredicate<Object> expected = org.apache.iceberg.expressions.Expressions.notNull("f1");

    assertEquals(expected.op(), actual.op());
    assertEquals(expected.ref().name(), actual.ref().name());
  }

  @Test
  public void testAnd() {
    Expression expr = resolve($("f1").isEqual(lit(5)).and($("f2").isEqual(lit(5L))));
    And actual = (And) FlinkFilters.convert(expr);
    And expected = (And) org.apache.iceberg.expressions.Expressions.and(
        org.apache.iceberg.expressions.Expressions.equal("f1", 5),
        org.apache.iceberg.expressions.Expressions.equal("f2", 5L));

    assertEquals(expected.op(), actual.op());
    assertEquals(expected.left().op(), actual.left().op());
    assertEquals(expected.right().op(), actual.right().op());
  }

  @Test
  public void testOr() {
    Expression expr = resolve($("f1").isEqual(lit(5)).or($("f2").isEqual(lit(5L))));
    Or actual = (Or) FlinkFilters.convert(expr);
    Or expected = (Or) org.apache.iceberg.expressions.Expressions.or(
        org.apache.iceberg.expressions.Expressions.equal("f1", 5),
        org.apache.iceberg.expressions.Expressions.equal("f2", 5L));

    assertEquals(expected.op(), actual.op());
    assertEquals(expected.left().op(), actual.left().op());
    assertEquals(expected.right().op(), actual.right().op());
  }

  @Test
  public void testNot() {
    Expression expr = resolve(ApiExpressionUtils.unresolvedCall(
        BuiltInFunctionDefinitions.NOT, $("f1").isEqual(lit(5))));
    Not actual = (Not) FlinkFilters.convert(expr);
    Not expected = (Not) org.apache.iceberg.expressions.Expressions.not(
        org.apache.iceberg.expressions.Expressions.equal("f1", 5));

    assertEquals(expected.op(), actual.op());
    assertPredicatesMatch((UnboundPredicate) expected.child(), (UnboundPredicate) actual.child());
  }

  private void assertPredicatesMatch(UnboundPredicate expected, UnboundPredicate actual) {
    assertEquals(expected.op(), actual.op());
    assertEquals(expected.literal(), actual.literal());
    assertEquals(expected.ref().name(), actual.ref().name());
  }

  private static Expression resolve(Expression unresolved) {
    return unresolved.accept(new ApiExpressionDefaultVisitor<Expression>() {

      @Override
      public Expression visit(UnresolvedReferenceExpression unresolvedReference) {
        String name = unresolvedReference.getName();
        TableColumn field = SCHEMA.getTableColumn(name).get();
        int index = SCHEMA.getTableColumns().indexOf(field);
        return new FieldReferenceExpression(name, field.getType(), 0, index);
      }

      @Override
      public Expression visit(UnresolvedCallExpression unresolvedCall) {
        List children = unresolvedCall.getChildren().stream().map(e -> e.accept(this)).collect(Collectors.toList());
        // We don't need output dataType.
        return new CallExpression(unresolvedCall.getFunctionDefinition(), children, DataTypes.STRING());
      }

      @Override
      public Expression visit(ValueLiteralExpression valueLiteral) {
        return valueLiteral;
      }

      @Override
      protected Expression defaultMethod(Expression expression) {
        throw new UnsupportedOperationException("Expression unsupported: " + expression);
      }
    });
  }
}
