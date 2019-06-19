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
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.common.Bytes;
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
import java.math.MathContext;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
    runETLOnce(appManager, ImmutableMap.of("logical.start.time", String.valueOf(CURRENT_TS)));

    try (Connection conn = createConnection();
         Statement stmt = conn.createStatement();
         ResultSet resultSet = stmt.executeQuery("SELECT * FROM MY_DEST_TABLE")) {
      Set<String> users = new HashSet<>();
      Assert.assertTrue(resultSet.next());
      users.add(resultSet.getString("NAME"));
      Assert.assertEquals(new Date(CURRENT_TS).toString(), resultSet.getDate("DATE_COL").toString());
      Assert.assertEquals(new Time(CURRENT_TS).toString(), resultSet.getTime("TIME_COL").toString());
      Assert.assertEquals(new Timestamp(CURRENT_TS),
                          resultSet.getTimestamp("TIMESTAMP_COL"));
      Assert.assertEquals(new Timestamp(CURRENT_TS),
                          resultSet.getTimestamp("DATETIME_COL"));
      Assert.assertTrue(resultSet.next());
      Assert.assertEquals("user2", Bytes.toString(resultSet.getBytes("BLOB_COL"), 0, 5));
      Assert.assertEquals("user2", Bytes.toString(resultSet.getBytes("TINYBLOB_COL"), 0, 5));
      Assert.assertEquals("user2", Bytes.toString(resultSet.getBytes("MEDIUMBLOB_COL"), 0, 5));
      Assert.assertEquals("user2", Bytes.toString(resultSet.getBytes("LONGBLOB_COL"), 0, 5));
      users.add(resultSet.getString("NAME"));
      Assert.assertEquals(ImmutableSet.of("user1", "user2"), users);

    }

    try (Connection conn = createConnection();
         Statement stmt1 = conn.createStatement();
         ResultSet row1 = stmt1.executeQuery("SELECT * FROM MY_DEST_TABLE WHERE ID=1");
         Statement stmt2 = conn.createStatement();
         ResultSet row2 = stmt2.executeQuery("SELECT * FROM MY_DEST_TABLE WHERE ID=2")) {

      Assert.assertTrue(row1.next());
      Assert.assertTrue(row2.next());

      // Verify data
      Assert.assertEquals(1, row1.getInt("ID"));
      Assert.assertEquals(2, row2.getInt("ID"));
      Assert.assertEquals("user1", row1.getString("NAME"));
      Assert.assertEquals("user2", row2.getString("NAME"));
      Assert.assertEquals("user1", row1.getString("TEXT_COL"));
      Assert.assertEquals("user2", row2.getString("TEXT_COL"));
      Assert.assertEquals("user1", row1.getString("TINYTEXT_COL"));
      Assert.assertEquals("user2", row2.getString("TINYTEXT_COL"));
      Assert.assertEquals("user1", row1.getString("MEDIUMTEXT_COL"));
      Assert.assertEquals("user2", row2.getString("MEDIUMTEXT_COL"));
      Assert.assertEquals("user1", row1.getString("LONGTEXT_COL"));
      Assert.assertEquals("user2", row2.getString("LONGTEXT_COL"));
      Assert.assertEquals("char1", row1.getString("CHAR_COL").trim());
      Assert.assertEquals("char2", row2.getString("CHAR_COL").trim());
      Assert.assertEquals(3.451, row1.getDouble("SCORE"), 0.000001);
      Assert.assertEquals(3.451, row2.getDouble("SCORE"), 0.000001);
      Assert.assertEquals(false, row1.getBoolean("GRADUATED"));
      Assert.assertEquals(true, row2.getBoolean("GRADUATED"));
      Assert.assertNull(row1.getString("NOT_IMPORTED"));
      Assert.assertEquals("Second", row1.getString("ENUM_COL"));
      Assert.assertEquals("Second", row2.getString("ENUM_COL"));
      Assert.assertEquals("a,b,c,d", row1.getString("SET_COL"));
      Assert.assertEquals("a,b,c,d", row2.getString("SET_COL"));

      Assert.assertEquals(1, row1.getShort("TINY"));
      Assert.assertEquals(2, row2.getShort("TINY"));
      Assert.assertEquals(1, row1.getShort("SMALL"));
      Assert.assertEquals(2, row2.getShort("SMALL"));
      Assert.assertEquals(3456987L, row1.getLong("BIG"));
      Assert.assertEquals(3456987L, row2.getLong("BIG"));
      Assert.assertEquals(8388607, row1.getInt("MEDIUMINT_COL"));
      Assert.assertEquals(8388607, row2.getInt("MEDIUMINT_COL"));

      Assert.assertEquals(3.456f, row1.getFloat("FLOAT_COL"), 0.00001);
      Assert.assertEquals(3.456f, row2.getFloat("FLOAT_COL"), 0.00001);
      Assert.assertEquals(3.457, row1.getDouble("REAL_COL"), 0.00001);
      Assert.assertEquals(3.457, row2.getDouble("REAL_COL"), 0.00001);
      Assert.assertEquals(3.458, row1.getDouble("NUMERIC_COL"), 0.000001);
      Assert.assertEquals(3.458, row2.getDouble("NUMERIC_COL"), 0.000001);
      Assert.assertEquals(3.459, row1.getDouble("DECIMAL_COL"), 0.000001);
      Assert.assertEquals(3.459, row2.getDouble("DECIMAL_COL"), 0.000001);
      Assert.assertTrue(row1.getBoolean("BIT_COL"));
      Assert.assertFalse(row2.getBoolean("BIT_COL"));
      // Verify time columns
      java.util.Date date = new java.util.Date(CURRENT_TS);
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
      LocalDate expectedDate = Date.valueOf(sdf.format(date)).toLocalDate();
      sdf = new SimpleDateFormat("H:mm:ss");
      LocalTime expectedTime = Time.valueOf(sdf.format(date)).toLocalTime();
      ZonedDateTime expectedTs = date.toInstant().atZone(ZoneId.ofOffset("UTC", ZoneOffset.UTC));
      Assert.assertEquals(expectedDate, row1.getDate("DATE_COL").toLocalDate());
      Assert.assertEquals(expectedTime, row1.getTime("TIME_COL").toLocalTime());
      Assert.assertEquals(expectedDate.getYear(), row1.getInt("YEAR_COL"));

      Assert.assertEquals(expectedTs, row1.getTimestamp("DATETIME_COL").toInstant()
        .atZone(ZoneId.ofOffset("UTC", ZoneOffset.UTC)));
      Assert.assertEquals(expectedTs, row1.getTimestamp("TIMESTAMP_COL").toInstant()
        .atZone(ZoneId.ofOffset("UTC", ZoneOffset.UTC)));

      // verify binary columns
      Assert.assertEquals("user1", Bytes.toString(row1.getBytes("BINARY_COL"), 0, 5));
      Assert.assertEquals("user2", Bytes.toString(row2.getBytes("BINARY_COL"), 0, 5));
      Assert.assertEquals("user1", Bytes.toString(row1.getBytes("VARBINARY_COL"), 0, 5));
      Assert.assertEquals("user2", Bytes.toString(row2.getBytes("VARBINARY_COL"), 0, 5));
      Assert.assertEquals("user1", Bytes.toString(row1.getBytes("BLOB_COL"), 0, 5));
      Assert.assertEquals("user2", Bytes.toString(row2.getBytes("BLOB_COL"), 0, 5));
      Assert.assertEquals("user1", Bytes.toString(row1.getBytes("MEDIUMBLOB_COL"), 0, 5));
      Assert.assertEquals("user2", Bytes.toString(row2.getBytes("MEDIUMBLOB_COL"), 0, 5));
      Assert.assertEquals("user1", Bytes.toString(row1.getBytes("TINYBLOB_COL"), 0, 5));
      Assert.assertEquals("user2", Bytes.toString(row2.getBytes("TINYBLOB_COL"), 0, 5));
      Assert.assertEquals("user1", Bytes.toString(row1.getBytes("LONGBLOB_COL"), 0, 5));
      Assert.assertEquals("user2", Bytes.toString(row2.getBytes("LONGBLOB_COL"), 0, 5));
    }

  }

  private void createInputData(String inputDatasetName) throws Exception {
    // add some data to the input table
    DataSetManager<Table> inputManager = getDataset(inputDatasetName);
    Schema schema = Schema.recordOf(
      "dbRecord",
      Schema.Field.of("ID", Schema.of(Schema.Type.INT)),
      Schema.Field.of("NAME", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("SCORE", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("GRADUATED", Schema.of(Schema.Type.BOOLEAN)),
      Schema.Field.of("TINY", Schema.of(Schema.Type.INT)),
      Schema.Field.of("SMALL", Schema.of(Schema.Type.INT)),
      Schema.Field.of("BIG", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("MEDIUMINT_COL", Schema.of(Schema.Type.INT)),
      Schema.Field.of("FLOAT_COL", Schema.of(Schema.Type.FLOAT)),
      Schema.Field.of("REAL_COL", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("NUMERIC_COL", Schema.decimalOf(10, 6)),
      Schema.Field.of("DECIMAL_COL", Schema.decimalOf(10, 6)),
      Schema.Field.of("BIT_COL", Schema.of(Schema.Type.BOOLEAN)),
      Schema.Field.of("DATE_COL", Schema.of(Schema.LogicalType.DATE)),
      Schema.Field.of("TIME_COL", Schema.of(Schema.LogicalType.TIME_MICROS)),
      Schema.Field.of("TIMESTAMP_COL", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
      Schema.Field.of("DATETIME_COL", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
      Schema.Field.of("YEAR_COL", Schema.of(Schema.LogicalType.DATE)),
      Schema.Field.of("TEXT_COL", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("TINYTEXT_COL", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("MEDIUMTEXT_COL", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("LONGTEXT_COL", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("CHAR_COL", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("BINARY_COL", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("VARBINARY_COL", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("TINYBLOB_COL", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("BLOB_COL", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("MEDIUMBLOB_COL", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("LONGBLOB_COL", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("ENUM_COL", Schema.enumWith("First", "Second", "Third")),
      Schema.Field.of("SET_COL", Schema.of(Schema.Type.STRING))
    );
    List<StructuredRecord> inputRecords = new ArrayList<>();
    LocalDateTime localDateTime = new Timestamp(CURRENT_TS).toLocalDateTime();
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(new Date(CURRENT_TS));
    for (int i = 1; i <= 2; i++) {
      String name = "user" + i;
      inputRecords.add(StructuredRecord.builder(schema)
                         .set("ID", i)
                         .set("NAME", name)
                         .set("SCORE", 3.451)
                         .set("GRADUATED", (i % 2 == 0))
                         .set("TINY", i)
                         .set("SMALL", i)
                         .set("BIG", 3456987L)
                         .set("MEDIUMINT_COL", 8388607)
                         .set("FLOAT_COL", 3.456f)
                         .set("REAL_COL", 3.457)
                         .setDecimal("NUMERIC_COL", new BigDecimal(3.458d, new MathContext(10)).setScale(6))
                         .setDecimal("DECIMAL_COL", new BigDecimal(3.459d, new MathContext(10)).setScale(6))
                         .set("BIT_COL", (i % 2 == 1))
                         .setDate("DATE_COL", localDateTime.toLocalDate())
                         .setTime("TIME_COL", localDateTime.toLocalTime())
                         .setTimestamp("TIMESTAMP_COL", localDateTime.atZone(ZoneId.ofOffset("UTC", ZoneOffset.UTC)))
                         .setTimestamp("DATETIME_COL",  localDateTime.atZone(ZoneId.ofOffset("UTC", ZoneOffset.UTC)))
                         .setDate("YEAR_COL",  localDateTime.toLocalDate())
                         .set("TEXT_COL", name)
                         .set("TINYTEXT_COL", name)
                         .set("MEDIUMTEXT_COL", name)
                         .set("LONGTEXT_COL", name)
                         .set("CHAR_COL", "char" + i)
                         .set("BINARY_COL", name.getBytes(Charsets.UTF_8))
                         .set("VARBINARY_COL", name.getBytes(Charsets.UTF_8))
                         .set("TINYBLOB_COL", name.getBytes(Charsets.UTF_8))
                         .set("BLOB_COL", name.getBytes(Charsets.UTF_8))
                         .set("MEDIUMBLOB_COL", name.getBytes(Charsets.UTF_8))
                         .set("LONGBLOB_COL", name.getBytes(Charsets.UTF_8))
                         .set("ENUM_COL", "Second")
                         .set("SET_COL", "a,b,c,d")
                         .build());
    }
    MockSource.writeInput(inputManager, inputRecords);
  }
}
