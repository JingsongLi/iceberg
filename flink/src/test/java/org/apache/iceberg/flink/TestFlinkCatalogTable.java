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

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

public class TestFlinkCatalogTable extends FlinkCatalogTestBase {

  public TestFlinkCatalogTable(String catalogName, String[] baseNamepace) {
    super(catalogName, baseNamepace);
  }

  @Before
  public void before() {
    sql("CREATE DATABASE %s", flinkDatabase);
    sql("USE CATALOG %s", catalogName);
    sql("USE %s", DATABASE);
  }

  @After
  public void cleanNamespaces() {
    sql("DROP TABLE IF EXISTS %s.tl", flinkDatabase);
    sql("DROP TABLE IF EXISTS %s.tl2", flinkDatabase);
    sql("DROP DATABASE IF EXISTS %s", flinkDatabase);
  }

  @Test
  public void testGetTable() {
    validationCatalog.createTable(
        TableIdentifier.of(icebergNamespace, "tl"),
        new Schema(
            Types.NestedField.optional(0, "id", Types.LongType.get()),
            Types.NestedField.optional(1, "strV", Types.StringType.get())));
    Assert.assertEquals(
        Arrays.asList(
            TableColumn.of("id", DataTypes.BIGINT()),
            TableColumn.of("strV", DataTypes.STRING())),
        tEnv.from("tl").getSchema().getTableColumns());
    Assert.assertTrue(tEnv.getCatalog(catalogName).get().tableExists(ObjectPath.fromString("db.tl")));
  }

  @Test
  public void testRenameTable() {
    Assume.assumeFalse("HadoopCatalog does not support rename table", isHadoopCatalog);

    validationCatalog.createTable(
        TableIdentifier.of(icebergNamespace, "tl"),
        new Schema(Types.NestedField.optional(0, "id", Types.LongType.get())));
    sql("ALTER TABLE tl RENAME TO tl2");
    AssertHelpers.assertThrows(
        "Should fail if trying to get a nonexistent table",
        ValidationException.class,
        "Table `tl` was not found.",
        () ->  tEnv.from("tl")
    );
    Assert.assertEquals(
        Collections.singletonList(TableColumn.of("id", DataTypes.BIGINT())),
        tEnv.from("tl2").getSchema().getTableColumns());
  }

  @Test
  public void testCreateTable() throws TableNotExistException {
    Assume.assumeFalse("HadoopCatalog does not support rename table", isHadoopCatalog);

    tEnv.executeSql("CREATE TABLE tl(id BIGINT)");

    Table table = table("tl");
    Assert.assertEquals(
        new Schema(Types.NestedField.optional(1, "id", Types.LongType.get())).asStruct(),
        table.schema().asStruct());
    Assert.assertEquals(Maps.newHashMap(), table.properties());

    CatalogTable catalogTable = catalogTable("tl");
    Assert.assertEquals(TableSchema.builder().field("id", DataTypes.BIGINT()).build(), catalogTable.getSchema());
    Assert.assertEquals(Maps.newHashMap(), catalogTable.getOptions());
  }

  @Test
  public void testCreatePartitionTable() throws TableNotExistException {
    Assume.assumeFalse("HadoopCatalog does not support rename table", isHadoopCatalog);

    tEnv.executeSql("CREATE TABLE tl(id BIGINT, dt STRING) PARTITIONED BY(dt)");

    Table table = table("tl");
    Assert.assertEquals(
        new Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "dt", Types.StringType.get())).asStruct(),
        table.schema().asStruct());
    Assert.assertEquals(PartitionSpec.builderFor(table.schema()).identity("dt").build(), table.spec());
    Assert.assertEquals(Maps.newHashMap(), table.properties());

    CatalogTable catalogTable = catalogTable("tl");
    Assert.assertEquals(
        TableSchema.builder().field("id", DataTypes.BIGINT()).field("dt", DataTypes.STRING()).build(),
        catalogTable.getSchema());
    Assert.assertEquals(Maps.newHashMap(), catalogTable.getOptions());
    Assert.assertEquals(Collections.singletonList("dt"), catalogTable.getPartitionKeys());
  }

  @Test
  public void testLoadTransformPartitionTable() throws TableNotExistException {
    Assume.assumeFalse("HadoopCatalog does not support rename table", isHadoopCatalog);

    Schema schema = new Schema(Types.NestedField.optional(0, "id", Types.LongType.get()));
    validationCatalog.createTable(
        TableIdentifier.of(icebergNamespace, "tl"), schema,
        PartitionSpec.builderFor(schema).bucket("id", 100).build());

    CatalogTable catalogTable = catalogTable("tl");
    Assert.assertEquals(
        TableSchema.builder().field("id", DataTypes.BIGINT()).build(),
        catalogTable.getSchema());
    Assert.assertEquals(Maps.newHashMap(), catalogTable.getOptions());
    Assert.assertEquals(Collections.emptyList(), catalogTable.getPartitionKeys());
  }

  @Test
  public void testAlterTable() {
    Assume.assumeFalse("HadoopCatalog does not support rename table", isHadoopCatalog);
    tEnv.executeSql("CREATE TABLE tl(id BIGINT) WITH ('oldK'='oldV')");
    Map<String, String> properties = Maps.newHashMap();
    properties.put("oldK", "oldV");

    // new
    tEnv.executeSql("ALTER TABLE tl SET('newK'='newV')");
    properties.put("newK", "newV");
    Assert.assertEquals(properties, table("tl").properties());

    // update old
    tEnv.executeSql("ALTER TABLE tl SET('oldK'='oldV2')");
    properties.put("oldK", "oldV2");
    Assert.assertEquals(properties, table("tl").properties());

    // TODO current-snapshot-id cherry-pick-snapshot-id

    // location
    tEnv.executeSql("ALTER TABLE tl SET('location'='/tmp/location')");
    properties.put("oldK", "oldV2");
    Assert.assertEquals("/tmp/location", table("tl").location());
  }

  // TODO add computed column and watermark cases

  private Table table(String name) {
    return validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, name));
  }

  private CatalogTable catalogTable(String name) throws TableNotExistException {
    return (CatalogTable) tEnv.getCatalog(tEnv.getCurrentCatalog()).get().getTable(new ObjectPath(DATABASE, name));
  }
}
