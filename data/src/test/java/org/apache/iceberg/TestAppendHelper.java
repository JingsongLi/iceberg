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

package org.apache.iceberg;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.junit.Assert;
import org.junit.rules.TemporaryFolder;

public class TestAppendHelper {

  private final Table table;
  private final FileFormat fileFormat;
  private final TemporaryFolder tmp;

  public TestAppendHelper(Table table, FileFormat fileFormat, TemporaryFolder tmp) {
    this.table = table;
    this.fileFormat = fileFormat;
    this.tmp = tmp;
  }

  public void appendToTable(DataFile... dataFiles) {
    Preconditions.checkNotNull(table, "table not set");

    AppendFiles append = table.newAppend();

    for (DataFile dataFile : dataFiles) {
      append = append.appendFile(dataFile);
    }

    append.commit();
  }

  public void appendToTable(List<Record> records) throws IOException {
    appendToTable(null, records);
  }

  public void appendToTable(StructLike partition, List<Record> records) throws IOException {
    appendToTable(writeFile(partition, records));
  }

  public DataFile writeFile(StructLike partition, List<Record> records) throws IOException {
    Preconditions.checkNotNull(table, "table not set");
    return writeFile(table, partition, records, fileFormat, tmp.newFile());
  }

  public static DataFile writeFile(
      Table table, StructLike partition, List<Record> records, FileFormat fileFormat,
      File file) throws IOException {
    Assert.assertTrue(file.delete());

    FileAppender<Record> appender;

    switch (fileFormat) {
      case AVRO:
        appender = Avro.write(Files.localOutput(file))
            .schema(table.schema())
            .createWriterFunc(DataWriter::create)
            .named(fileFormat.name())
            .build();
        break;

      case PARQUET:
        appender = Parquet.write(Files.localOutput(file))
            .schema(table.schema())
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .named(fileFormat.name())
            .build();
        break;

      case ORC:
        appender = ORC.write(Files.localOutput(file))
            .schema(table.schema())
            .createWriterFunc(GenericOrcWriter::buildWriter)
            .build();
        break;

      default:
        throw new UnsupportedOperationException("Cannot write format: " + fileFormat);
    }

    try {
      appender.addAll(records);
    } finally {
      appender.close();
    }

    DataFiles.Builder builder = DataFiles.builder(table.spec())
        .withPath(file.toURI().toString())
        .withFormat(fileFormat)
        .withFileSizeInBytes(file.length())
        .withMetrics(appender.metrics());

    if (partition != null) {
      builder.withPartition(partition);
    }

    return builder.build();
  }

}
