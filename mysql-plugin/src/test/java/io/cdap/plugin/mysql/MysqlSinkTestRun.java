/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.mysql;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.mock.batch.MockSource;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.db.batch.sink.AbstractDBSink;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import static io.cdap.plugin.mysql.TestRecord.Column.BIG;
import static io.cdap.plugin.mysql.TestRecord.Column.BINARY_COL;
import static io.cdap.plugin.mysql.TestRecord.Column.BIT_COL;
import static io.cdap.plugin.mysql.TestRecord.Column.BLOB_COL;
import static io.cdap.plugin.mysql.TestRecord.Column.CHAR_COL;
import static io.cdap.plugin.mysql.TestRecord.Column.DATETIME_COL;
import static io.cdap.plugin.mysql.TestRecord.Column.DATE_COL;
import static io.cdap.plugin.mysql.TestRecord.Column.DECIMAL_COL;
import static io.cdap.plugin.mysql.TestRecord.Column.ENUM_COL;
import static io.cdap.plugin.mysql.TestRecord.Column.FLOAT_COL;
import static io.cdap.plugin.mysql.TestRecord.Column.GRADUATED;
import static io.cdap.plugin.mysql.TestRecord.Column.ID;
import static io.cdap.plugin.mysql.TestRecord.Column.LONGBLOB_COL;
import static io.cdap.plugin.mysql.TestRecord.Column.LONGTEXT_COL;
import static io.cdap.plugin.mysql.TestRecord.Column.MEDIUMBLOB_COL;
import static io.cdap.plugin.mysql.TestRecord.Column.MEDIUMINT_COL;
import static io.cdap.plugin.mysql.TestRecord.Column.MEDIUMTEXT_COL;
import static io.cdap.plugin.mysql.TestRecord.Column.NAME;
import static io.cdap.plugin.mysql.TestRecord.Column.NOT_IMPORTED;
import static io.cdap.plugin.mysql.TestRecord.Column.NUMERIC_COL;
import static io.cdap.plugin.mysql.TestRecord.Column.REAL_COL;
import static io.cdap.plugin.mysql.TestRecord.Column.SCORE;
import static io.cdap.plugin.mysql.TestRecord.Column.SET_COL;
import static io.cdap.plugin.mysql.TestRecord.Column.SMALL;
import static io.cdap.plugin.mysql.TestRecord.Column.TEXT_COL;
import static io.cdap.plugin.mysql.TestRecord.Column.TIMESTAMP_COL;
import static io.cdap.plugin.mysql.TestRecord.Column.TIME_COL;
import static io.cdap.plugin.mysql.TestRecord.Column.TINY;
import static io.cdap.plugin.mysql.TestRecord.Column.TINYBLOB_COL;
import static io.cdap.plugin.mysql.TestRecord.Column.TINYTEXT_COL;
import static io.cdap.plugin.mysql.TestRecord.Column.VARBINARY_COL;
import static io.cdap.plugin.mysql.TestRecord.Column.YEAR_COL;

/**
 * Test for ETL using databases.
 */
public class MysqlSinkTestRun extends MysqlPluginTestBase {

  @Test
  public void testDBSink() throws Exception {
    String inputDatasetName = "input-dbsinktest";

    ETLPlugin sourceConfig = MockSource.getPlugin(inputDatasetName);
    ETLPlugin sinkConfig = new ETLPlugin(
      MysqlConstants.PLUGIN_NAME,
      BatchSink.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .putAll(BASE_PROPS)
        .put(MysqlConstants.AUTO_RECONNECT, "true")
        .put(MysqlConstants.USE_COMPRESSION, "true")
        .put(MysqlConstants.SQL_MODE, "ANSI_QUOTES,NO_ENGINE_SUBSTITUTION")
        .put(AbstractDBSink.DBSinkConfig.TABLE_NAME, "MY_DEST_TABLE")
        .put(Constants.Reference.REFERENCE_NAME, "DBTest")
        .build(),
      null);

    ApplicationManager appManager = deployETL(sourceConfig, sinkConfig, DATAPIPELINE_ARTIFACT, "testDBSink");
    createInputData(inputDatasetName);
    runETLOnce(appManager, ImmutableMap.of("logical.start.time", String.valueOf(System.currentTimeMillis())));

    for (TestRecord expected : TEST_RECORDS) {

      int expectedId = (int) expected.get(ID);
      try (Connection conn = createConnection();
           Statement stmt1 = conn.createStatement();
           ResultSet actual = stmt1.executeQuery("SELECT * FROM MY_DEST_TABLE WHERE ID=" + expectedId)) {

        Assert.assertTrue("Sink output does not contain record with ID: " + expectedId, actual.next());

        // Verify data
        Assert.assertEquals(expected.get(ID), actual.getInt(ID.name()));
        Assert.assertEquals(expected.get(NAME), actual.getString(NAME.name()));
        Assert.assertEquals(expected.get(TEXT_COL), actual.getString(TEXT_COL.name()));
        Assert.assertEquals(expected.get(TINYTEXT_COL), actual.getString(TINYTEXT_COL.name()));
        Assert.assertEquals(expected.get(MEDIUMTEXT_COL), actual.getString(MEDIUMTEXT_COL.name()));
        Assert.assertEquals(expected.get(LONGTEXT_COL), actual.getString(LONGTEXT_COL.name()));
        Assert.assertEquals(expected.get(CHAR_COL), actual.getString(CHAR_COL.name()));
        Assert.assertEquals((double) expected.get(SCORE), actual.getDouble(SCORE.name()), 0.001);
        Assert.assertEquals(expected.get(GRADUATED), actual.getBoolean(GRADUATED.name()));
        Assert.assertNull(actual.getString(NOT_IMPORTED.name()));
        Assert.assertEquals(expected.get(ENUM_COL), actual.getString(ENUM_COL.name()));
        Assert.assertEquals(expected.get(SET_COL), actual.getString(SET_COL.name()));

        Assert.assertEquals(expected.get(TINY), actual.getInt(TINY.name()));
        Assert.assertEquals(expected.get(SMALL), actual.getInt(SMALL.name()));
        Assert.assertEquals(expected.get(BIG), actual.getLong(BIG.name()));
        Assert.assertEquals(expected.get(MEDIUMINT_COL), actual.getInt(MEDIUMINT_COL.name()));
        Assert.assertEquals((float) expected.get(FLOAT_COL), actual.getFloat(FLOAT_COL.name()), 0.001);
        Assert.assertEquals((double) expected.get(REAL_COL), actual.getDouble(REAL_COL.name()), 0.001);
        Assert.assertEquals(expected.get(BIT_COL), actual.getBoolean(BIT_COL.name()));

        // verify binary columns
        Assert.assertEquals(new String((byte[]) expected.get(BINARY_COL)),
                            new String(actual.getBytes(BINARY_COL.name())).trim());
        Assert.assertEquals(new String((byte[]) expected.get(VARBINARY_COL)),
                            new String(actual.getBytes(VARBINARY_COL.name())).trim());
        Assert.assertEquals(expected.get(BLOB_COL), new String(actual.getBytes(BLOB_COL.name())).trim());
        Assert.assertEquals(expected.get(MEDIUMBLOB_COL), new String(actual.getBytes(MEDIUMBLOB_COL.name())).trim());
        Assert.assertEquals(expected.get(TINYBLOB_COL),  new String(actual.getBytes(TINYBLOB_COL.name())).trim());
        Assert.assertEquals(expected.get(LONGBLOB_COL), new String(actual.getBytes(LONGBLOB_COL.name())).trim());

      }
    }
  }

  private void createInputData(String inputDatasetName) throws Exception {
    // add some data to the input table
    DataSetManager<Table> inputManager = getDataset(inputDatasetName);
    Schema schema = Schema.recordOf(
      "dbRecord",
      ID.field(),
      NAME.field(),
      SCORE.field(),
      GRADUATED.field(),
      TINY.field(),
      SMALL.field(),
      BIG.field(),
      MEDIUMINT_COL.field(),
      FLOAT_COL.field(),
      REAL_COL.field(),
      NUMERIC_COL.field(),
      DECIMAL_COL.field(),
      BIT_COL.field(),
      DATE_COL.field(),
      TIME_COL.field(),
      TIMESTAMP_COL.field(),
      YEAR_COL.field(),
      TEXT_COL.field(),
      TINYTEXT_COL.field(),
      MEDIUMTEXT_COL.field(),
      LONGTEXT_COL.field(),
      CHAR_COL.field(),
      DATETIME_COL.field(),
      BINARY_COL.field(),
      VARBINARY_COL.field(),
      TINYBLOB_COL.field(),
      BLOB_COL.field(),
      MEDIUMBLOB_COL.field(),
      LONGBLOB_COL.field(),
      ENUM_COL.field(),
      SET_COL.field()
    );

    List<StructuredRecord> inputRecords = new ArrayList<>();
    for (TestRecord record : TEST_RECORDS) {
      inputRecords.add(StructuredRecord.builder(schema)
                         .set(ID.name(), record.get(ID))
                         .set(NAME.name(), record.get(NAME))
                         .set(SCORE.name(), record.get(SCORE))
                         .set(GRADUATED.name(), record.get(GRADUATED))
                         .set(TINY.name(), record.get(TINY))
                         .set(SMALL.name(), record.get(SMALL))
                         .set(BIG.name(), record.get(BIG))
                         .set(MEDIUMINT_COL.name(), record.get(MEDIUMINT_COL))
                         .set(FLOAT_COL.name(), record.get(FLOAT_COL))
                         .set(REAL_COL.name(), record.get(REAL_COL))
                         .setDecimal(NUMERIC_COL.name(), (BigDecimal) record.get(NUMERIC_COL))
                         .setDecimal(DECIMAL_COL.name(), (BigDecimal) record.get(DECIMAL_COL))
                         .set(BIT_COL.name(), record.get(BIT_COL))
                         .setDate(DATE_COL.name(), ((Date) record.get(DATE_COL)).toLocalDate())
                         .setTime(TIME_COL.name(), ((Time) record.get(TIME_COL)).toLocalTime())
                         .setTimestamp(TIMESTAMP_COL.name(), ZonedDateTime.ofInstant((
                                         (Timestamp) record.get(TIMESTAMP_COL)).toInstant(), ZoneId.systemDefault()))
                         .setTimestamp(DATETIME_COL.name(), ZonedDateTime.ofInstant((
                                         (Timestamp) record.get(DATETIME_COL)).toInstant(), ZoneId.systemDefault()))
                         .setDate(YEAR_COL.name(), LocalDate.ofYearDay((short) record.get(YEAR_COL), 1))
                         .set(TEXT_COL.name(), record.get(TEXT_COL))
                         .set(TINYTEXT_COL.name(), record.get(TINYTEXT_COL))
                         .set(MEDIUMTEXT_COL.name(), record.get(MEDIUMTEXT_COL))
                         .set(LONGTEXT_COL.name(), record.get(LONGTEXT_COL))
                         .set(CHAR_COL.name(), record.get(CHAR_COL))
                         .set(BINARY_COL.name(), record.get(BINARY_COL))
                         .set(VARBINARY_COL.name(), record.get(VARBINARY_COL))
                         .set(TINYBLOB_COL.name(), ((String) record.get(TINYBLOB_COL)).getBytes(Charsets.UTF_8))
                         .set(BLOB_COL.name(), ((String) record.get(BLOB_COL)).getBytes(Charsets.UTF_8))
                         .set(MEDIUMBLOB_COL.name(), ((String) record.get(MEDIUMBLOB_COL)).getBytes(Charsets.UTF_8))
                         .set(LONGBLOB_COL.name(), ((String) record.get(LONGBLOB_COL)).getBytes(Charsets.UTF_8))
                         .set(ENUM_COL.name(), record.get(ENUM_COL))
                         .set(SET_COL.name(), record.get(SET_COL))
                         .build());
    }

    MockSource.writeInput(inputManager, inputRecords);
  }
}
