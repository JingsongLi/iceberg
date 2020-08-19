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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.WatermarkSpec;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_ROWTIME;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_STRATEGY_DATA_TYPE;
import static org.apache.flink.table.descriptors.DescriptorProperties.WATERMARK_STRATEGY_EXPR;

/**
 * The serialization protocol is compatible with DescriptorProperties.putTableSchema.
 */
class CatalogSchemaUtil {
  private CatalogSchemaUtil() {}

  private static final Pattern SCHEMA_COLUMN_NAME_SUFFIX = Pattern.compile("\\d+\\.name");

  static Map<String, String> serializeComputedColumns(TableSchema schema) {
    DescriptorProperties properties = new DescriptorProperties();
    String key = "schema";
    for (int i = 0; i < schema.getFieldCount(); i++) {
      TableColumn column = schema.getTableColumns().get(i);
      if (column.isGenerated()) {
        properties.putString(key + '.' + i + '.' + DescriptorProperties.NAME, column.getName());
        properties.putString(key + '.' + i + '.' + DescriptorProperties.DATA_TYPE,
            column.getType().getLogicalType().asSerializableString());
        properties.putString(key + '.' + i + '.' + DescriptorProperties.EXPR, column.getExpr().get());
      }
    }

    if (!schema.getWatermarkSpecs().isEmpty()) {
      final List<List<String>> watermarkValues = new ArrayList<>();
      for (WatermarkSpec spec : schema.getWatermarkSpecs()) {
        watermarkValues.add(Arrays.asList(
            spec.getRowtimeAttribute(),
            spec.getWatermarkExpr(),
            spec.getWatermarkExprOutputType().getLogicalType().asSerializableString()));
      }
      properties.putIndexedFixedProperties(
          key + '.' + WATERMARK,
          Arrays.asList(WATERMARK_ROWTIME, WATERMARK_STRATEGY_EXPR, WATERMARK_STRATEGY_DATA_TYPE),
          watermarkValues);
    }

    return properties.asMap();
  }

  static TableSchema restoreSchemaFromMap(Map<String, String> options, TableSchema physicalSchema) {
    String key = "schema";

    DescriptorProperties properties = new DescriptorProperties();
    properties.putProperties(options);

    int computedColCount = options.keySet().stream()
        .filter((k) -> k.startsWith(key)
            // "key." is the prefix.
            && SCHEMA_COLUMN_NAME_SUFFIX.matcher(k.substring(key.length() + 1)).matches())
        .mapToInt((k) -> 1)
        .sum();

    if (computedColCount == 0) {
      return physicalSchema;
    }

    Iterator<TableColumn> physicalColumns = physicalSchema.getTableColumns().iterator();

    TableSchema.Builder builder = TableSchema.builder();
    for (int i = 0; i < computedColCount + physicalSchema.getFieldCount(); i++) {
      final String nameKey = key + '.' + i + '.' + DescriptorProperties.NAME;
      final String typeKey = key + '.' + i + '.' + DescriptorProperties.DATA_TYPE;
      final String exprKey = key + '.' + i + '.' + DescriptorProperties.EXPR;

      String name = options.get(nameKey);
      // If this field is a computed column.
      if (name != null) {
        builder.field(name, properties.getDataType(typeKey), properties.getString(exprKey));
      } else {
        TableColumn column = physicalColumns.next();
        builder.field(column.getName(), column.getType());
      }
    }

    // extract watermark information
    String watermarkPrefixKey = key + '.' + WATERMARK;
    final int watermarkCount = options.keySet().stream()
        .filter((k) -> k.startsWith(watermarkPrefixKey) && k.endsWith('.' + WATERMARK_ROWTIME))
        .mapToInt((k) -> 1)
        .sum();
    if (watermarkCount > 0) {
      for (int i = 0; i < watermarkCount; i++) {
        final String rowtimeKey = watermarkPrefixKey + '.' + i + '.' + WATERMARK_ROWTIME;
        final String exprKey = watermarkPrefixKey + '.' + i + '.' + WATERMARK_STRATEGY_EXPR;
        final String typeKey = watermarkPrefixKey + '.' + i + '.' + WATERMARK_STRATEGY_DATA_TYPE;
        final String rowtime = options.get(rowtimeKey);
        final String exprString = options.get(exprKey);
        final String typeString = options.get(typeKey);
        final DataType exprType = TypeConversions.fromLogicalToDataType(LogicalTypeParser.parse(typeString));
        builder.watermark(rowtime, exprString, exprType);
      }
    }

    return builder.build();
  }

  static PartitionSpec toPartitionSpec(List<String> partitionKeys, Schema icebergSchema, TableSchema schema) {
    PartitionSpec.Builder builder = PartitionSpec.builderFor(icebergSchema);
    for (String partitionKey : partitionKeys) {
      if (!schema.getTableColumn(partitionKey).get().getExpr().isPresent()) {
        builder.identity(partitionKey);
      } else {
        throw new UnsupportedOperationException("Creating partition from computed column is not supported yet.");
      }
    }
    return builder.build();
  }

  static List<String> toPartitionKeys(PartitionSpec spec, Schema icebergSchema, TableSchema schema) {
    List<String> partitionKeys = Lists.newArrayList();
    for (PartitionField field : spec.fields()) {
      if (field.transform().isIdentity()) {
        partitionKeys.add(icebergSchema.findColumnName(field.sourceId()));
      } else {
        // Not created by Flink SQL.
        // For compatibility with iceberg tables, return empty.
        return Collections.emptyList();
      }
    }
    return partitionKeys;
  }
}
