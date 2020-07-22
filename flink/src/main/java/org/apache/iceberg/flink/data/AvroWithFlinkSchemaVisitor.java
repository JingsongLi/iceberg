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

package org.apache.iceberg.flink.data;

import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.avro.AvroWithPartnerSchemaVisitor;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public abstract class AvroWithFlinkSchemaVisitor<T> extends AvroWithPartnerSchemaVisitor<LogicalType, T> {

  @Override
  public boolean isValidMapKey(LogicalType logicalType) {
    return logicalType.getTypeRoot().getFamilies().contains(LogicalTypeFamily.CHARACTER_STRING);
  }

  @Override
  public boolean isMapType(LogicalType logicalType) {
    return logicalType instanceof MapType;
  }

  @Override
  public LogicalType arrayElementType(LogicalType arrayType) {
    Preconditions.checkArgument(arrayType instanceof ArrayType, "Invalid array: %s is not an array", arrayType);
    return ((ArrayType) arrayType).getElementType();
  }

  @Override
  public LogicalType mapKeyType(LogicalType mapType) {
    Preconditions.checkArgument(mapType instanceof MapType, "Invalid map: %s is not a map", mapType);
    return ((MapType) mapType).getKeyType();
  }

  @Override
  public LogicalType mapValueType(LogicalType mapType) {
    Preconditions.checkArgument(mapType instanceof MapType, "Invalid map: %s is not a map", mapType);
    return ((MapType) mapType).getValueType();
  }

  @Override
  public String[] structFieldNames(LogicalType rowType) {
    Preconditions.checkArgument(rowType instanceof RowType, "Invalid struct: %s is not a struct", rowType);
    return ((RowType) rowType).getFieldNames().toArray(new String[0]);
  }

  @Override
  public LogicalType[] structFieldTypes(LogicalType rowType) {
    Preconditions.checkArgument(rowType instanceof RowType, "Invalid struct: %s is not a struct", rowType);
    return ((RowType) rowType).getChildren().toArray(new LogicalType[0]);
  }

  @Override
  public LogicalType nullType() {
    return new NullType();
  }
}
