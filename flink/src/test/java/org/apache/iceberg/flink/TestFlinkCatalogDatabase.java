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

import java.io.File;
import java.util.Map;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class TestFlinkCatalogDatabase extends FlinkCatalogTestBase {

  public TestFlinkCatalogDatabase(String catalogName, String[] baseNamepace) {
    super(catalogName, baseNamepace);
  }

  @After
  public void clean() {
    sql("DROP TABLE IF EXISTS %s.tl", flinkIdentifier);
    sql("DROP DATABASE IF EXISTS %s", flinkIdentifier);
  }

  @Test
  public void testCreateNamespace() {
    Assert.assertFalse(
        "Database should not already exist",
        validationNamespaceCatalog.namespaceExists(icebergNamespace));

    sql("CREATE DATABASE %s", flinkIdentifier);

    Assert.assertTrue("Database should exist", validationNamespaceCatalog.namespaceExists(icebergNamespace));
  }

  @Test
  public void testDefaultDatabase() {
    sql("USE CATALOG %s", catalogName);

    Assert.assertEquals("Should use the current catalog", tEnv.getCurrentCatalog(), catalogName);
    Assert.assertEquals("Should use the configured default namespace", tEnv.getCurrentDatabase(), "default");
  }

  @Test
  public void testDropEmptyDatabase() {
    Assert.assertFalse(
        "Namespace should not already exist",
        validationNamespaceCatalog.namespaceExists(icebergNamespace));

    sql("CREATE DATABASE %s", flinkIdentifier);

    Assert.assertTrue("Namespace should exist", validationNamespaceCatalog.namespaceExists(icebergNamespace));

    sql("DROP DATABASE %s", flinkIdentifier);

    Assert.assertFalse(
        "Namespace should have been dropped",
        validationNamespaceCatalog.namespaceExists(icebergNamespace));
  }

  @Test
  public void testDropNonEmptyNamespace() {
    Assume.assumeFalse("Hadoop catalog throws IOException: Directory is not empty.", isHadoopCatalog);

    Assert.assertFalse(
        "Namespace should not already exist",
        validationNamespaceCatalog.namespaceExists(icebergNamespace));

    sql("CREATE DATABASE %s", flinkIdentifier);

    validationCatalog.createTable(
        TableIdentifier.of(icebergNamespace, "tl"),
        new Schema(Types.NestedField.optional(0, "id", Types.LongType.get())));

    Assert.assertTrue("Namespace should exist", validationNamespaceCatalog.namespaceExists(icebergNamespace));
    Assert.assertTrue("Table should exist", validationCatalog.tableExists(TableIdentifier.of(icebergNamespace, "tl")));

    AssertHelpers.assertThrowsCause(
        "Should fail if trying to delete a non-empty database",
        DatabaseNotEmptyException.class,
        String.format("Database %s in catalog %s is not empty.", DATABASE, catalogName),
        () -> sql("DROP DATABASE %s", flinkIdentifier));

    sql("DROP TABLE %s.tl", flinkIdentifier);
  }

  @Test
  public void testListTables() {
    Assert.assertFalse(
        "Namespace should not already exist",
        validationNamespaceCatalog.namespaceExists(icebergNamespace));

    sql("CREATE DATABASE %s", flinkIdentifier);
    sql("USE CATALOG %s", catalogName);
    sql("USE %s", DATABASE);

    Assert.assertTrue("Namespace should exist", validationNamespaceCatalog.namespaceExists(icebergNamespace));

    Assert.assertEquals("Should not list any tables", 0, tEnv.listTables().length);

    validationCatalog.createTable(
        TableIdentifier.of(icebergNamespace, "tl"),
        new Schema(Types.NestedField.optional(0, "id", Types.LongType.get())));

    Assert.assertEquals("Only 1 table", 1, tEnv.listTables().length);
    Assert.assertEquals("Table name should match", "tl", tEnv.listTables()[0]);
  }

  @Test
  public void testListNamespace() {
    Assert.assertFalse(
        "Namespace should not already exist",
        validationNamespaceCatalog.namespaceExists(icebergNamespace));

    sql("CREATE DATABASE %s", flinkIdentifier);
    sql("USE CATALOG %s", catalogName);

    Assert.assertTrue("Namespace should exist", validationNamespaceCatalog.namespaceExists(icebergNamespace));

    String[] databases = tEnv.listDatabases();

    if (isHadoopCatalog || baseNamespace.length > 0) {
      Assert.assertEquals("Should have 1 database", 1, databases.length);
      Assert.assertEquals("Should have only db database", "db", databases[0]);
    } else {
      Assert.assertEquals("Should have 2 databases", 2, databases.length);
      Assert.assertEquals(
          "Should have default and db databases",
          ImmutableSet.of("default", "db"),
          ImmutableSet.copyOf(databases));
    }
  }

  @Test
  public void testCreateNamespaceWithMetadata() {
    Assume.assumeFalse("HadoopCatalog does not support namespace metadata", isHadoopCatalog);

    Assert.assertFalse(
        "Namespace should not already exist",
        validationNamespaceCatalog.namespaceExists(icebergNamespace));

    sql("CREATE DATABASE %s WITH ('prop'='value')", flinkIdentifier);

    Assert.assertTrue("Namespace should exist", validationNamespaceCatalog.namespaceExists(icebergNamespace));

    Map<String, String> nsMetadata = validationNamespaceCatalog.loadNamespaceMetadata(icebergNamespace);

    Assert.assertEquals("Namespace should have expected prop value", "value", nsMetadata.get("prop"));
  }

  @Test
  public void testCreateNamespaceWithComment() {
    Assume.assumeFalse("HadoopCatalog does not support namespace metadata", isHadoopCatalog);

    Assert.assertFalse(
        "Namespace should not already exist",
        validationNamespaceCatalog.namespaceExists(icebergNamespace));

    sql("CREATE DATABASE %s COMMENT 'namespace doc'", flinkIdentifier);

    Assert.assertTrue("Namespace should exist", validationNamespaceCatalog.namespaceExists(icebergNamespace));

    Map<String, String> nsMetadata = validationNamespaceCatalog.loadNamespaceMetadata(icebergNamespace);

    Assert.assertEquals("Namespace should have expected comment", "namespace doc", nsMetadata.get("comment"));
  }

  @Test
  public void testCreateNamespaceWithLocation() throws Exception {
    Assume.assumeFalse("HadoopCatalog does not support namespace locations", isHadoopCatalog);

    Assert.assertFalse(
        "Namespace should not already exist",
        validationNamespaceCatalog.namespaceExists(icebergNamespace));

    File location = TEMPORARY_FOLDER.newFile();
    Assert.assertTrue(location.delete());

    sql("CREATE DATABASE %s WITH ('location'='%s')", flinkIdentifier, location);

    Assert.assertTrue("Namespace should exist", validationNamespaceCatalog.namespaceExists(icebergNamespace));

    Map<String, String> nsMetadata = validationNamespaceCatalog.loadNamespaceMetadata(icebergNamespace);

    Assert.assertEquals("Namespace should have expected location",
        "file:" + location.getPath(), nsMetadata.get("location"));
  }

  @Test
  public void testSetProperties() {
    Assume.assumeFalse("HadoopCatalog does not support namespace metadata", isHadoopCatalog);

    Assert.assertFalse(
        "Namespace should not already exist",
        validationNamespaceCatalog.namespaceExists(icebergNamespace));

    sql("CREATE DATABASE %s", flinkIdentifier);

    Assert.assertTrue("Namespace should exist", validationNamespaceCatalog.namespaceExists(icebergNamespace));

    Map<String, String> defaultMetadata = validationNamespaceCatalog.loadNamespaceMetadata(icebergNamespace);
    Assert.assertFalse("Default metadata should not have custom property", defaultMetadata.containsKey("prop"));

    sql("ALTER DATABASE %s SET ('prop'='value')", flinkIdentifier);

    Map<String, String> nsMetadata = validationNamespaceCatalog.loadNamespaceMetadata(icebergNamespace);

    Assert.assertEquals("Namespace should have expected prop value", "value", nsMetadata.get("prop"));
  }
}
