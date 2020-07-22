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

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.flink.data.RandomData.COMPLEX_SCHEMA;

public class TestFlinkAvroReaderWriter {
  private static final int NUM_RECORDS = 20_000;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private void testCorrectness(Schema schema, int numRecords, Iterable<RowData> iterable) throws IOException {
    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    try (FileAppender<RowData> writer = Avro.write(Files.localOutput(testFile))
        .schema(schema)
        .createWriterFunc(ignore -> new FlinkAvroWriter(FlinkSchemaUtil.convert(schema)))
        .build()) {
      writer.addAll(iterable);
    }

    try (CloseableIterable<RowData> reader = Avro.read(Files.localInput(testFile))
        .project(schema)
        .createReaderFunc(FlinkAvroReader::new)
        .build()) {
      Iterator<RowData> expected = iterable.iterator();
      Iterator<RowData> rows = reader.iterator();
      for (int i = 0; i < numRecords; i += 1) {
        Assert.assertTrue("Should have expected number of rows", rows.hasNext());
        Assert.assertEquals(expected.next(), rows.next());
      }
      Assert.assertFalse("Should not have extra rows", rows.hasNext());
    }
  }

  @Test
  public void testNormalData() throws IOException {
    testCorrectness(COMPLEX_SCHEMA, NUM_RECORDS, RandomData.generateRowData(COMPLEX_SCHEMA, NUM_RECORDS, 19982));
  }
}
