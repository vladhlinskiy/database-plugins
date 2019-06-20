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

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.db.ConnectionConfig;
import io.cdap.plugin.db.batch.source.AbstractDBSource;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.cdap.plugin.mysql.TestRecord.Column.BIG;
import static io.cdap.plugin.mysql.TestRecord.Column.BINARY_COL;
import static io.cdap.plugin.mysql.TestRecord.Column.BIT_COL;
import static io.cdap.plugin.mysql.TestRecord.Column.BLOB_COL;
import static io.cdap.plugin.mysql.TestRecord.Column.CHAR_COL;
import static io.cdap.plugin.mysql.TestRecord.Column.DATETIME_COL;
import static io.cdap.plugin.mysql.TestRecord.Column.DATE_COL;
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

public class MysqlSourceTestRun extends MysqlPluginTestBase {

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testDBMacroSupport() throws Exception {
    String importQuery = "SELECT * FROM my_table WHERE " + DATE_COL.name() +
      " <= '${logicalStartTime(yyyy-MM-dd,1d)}' AND $CONDITIONS";
    String boundingQuery = "SELECT MIN(" + ID.name() + "),MAX(" + ID.name() + ") from my_table";
    String splitBy = ID.name();

    ImmutableMap<String, String> sourceProps = ImmutableMap.<String, String>builder()
      .putAll(BASE_PROPS)
      .put(MysqlConstants.AUTO_RECONNECT, "true")
      .put(MysqlConstants.USE_COMPRESSION, "true")
      .put(MysqlConstants.SQL_MODE, "ANSI_QUOTES,NO_ENGINE_SUBSTITUTION")
      .put(AbstractDBSource.DBSourceConfig.IMPORT_QUERY, importQuery)
      .put(AbstractDBSource.DBSourceConfig.BOUNDING_QUERY, boundingQuery)
      .put(AbstractDBSource.DBSourceConfig.SPLIT_BY, splitBy)
      .put(Constants.Reference.REFERENCE_NAME, "DBTestSource").build();

    ETLPlugin sourceConfig = new ETLPlugin(
      MysqlConstants.PLUGIN_NAME,
      BatchSource.PLUGIN_TYPE,
      sourceProps
    );

    ETLPlugin sinkConfig = MockSink.getPlugin("macroOutputTable");

    ApplicationManager appManager = deployETL(sourceConfig, sinkConfig,
                                              DATAPIPELINE_ARTIFACT, "testDBMacro");
    runETLOnce(appManager, ImmutableMap.of("logical.start.time", String.valueOf(System.currentTimeMillis())));

    DataSetManager<Table> outputManager = getDataset("macroOutputTable");
    Assert.assertTrue(MockSink.readOutput(outputManager).isEmpty());
  }

  @Test
  @SuppressWarnings("ConstantConditions")
  public void testDBSource() throws Exception {

    Set<TestRecord.Column> selectColumns =  new HashSet<>(Arrays.asList((TestRecord.Column.values())));
    selectColumns.remove(NOT_IMPORTED);

    String selectColumnsDef = selectColumns.stream()
      .map(TestRecord.Column::name)
      .collect(Collectors.joining(", "));

    String importQuery = "SELECT " + selectColumnsDef + " FROM my_table WHERE " + ID.name() +  " < 3 AND $CONDITIONS";
    String boundingQuery = "SELECT MIN(" + ID.name() + "),MAX(" + ID.name() + ") from my_table";
    String splitBy = ID.name();
    ETLPlugin sourceConfig = new ETLPlugin(
      MysqlConstants.PLUGIN_NAME,
      BatchSource.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .putAll(BASE_PROPS)
        .put(MysqlConstants.AUTO_RECONNECT, "true")
        .put(MysqlConstants.USE_COMPRESSION, "true")
        .put(MysqlConstants.SQL_MODE, "ANSI_QUOTES,NO_ENGINE_SUBSTITUTION")
        .put(AbstractDBSource.DBSourceConfig.IMPORT_QUERY, importQuery)
        .put(AbstractDBSource.DBSourceConfig.BOUNDING_QUERY, boundingQuery)
        .put(AbstractDBSource.DBSourceConfig.SPLIT_BY, splitBy)
        .put(Constants.Reference.REFERENCE_NAME, "DBSourceTest")
        .build(),
      null
    );

    String outputDatasetName = "output-dbsourcetest";
    ETLPlugin sinkConfig = MockSink.getPlugin(outputDatasetName);

    ApplicationManager appManager = deployETL(sourceConfig, sinkConfig,
                                              DATAPIPELINE_ARTIFACT, "testDBSource");
    runETLOnce(appManager);

    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);

    Assert.assertEquals(2, outputRecords.size());

    Map<Integer, StructuredRecord> outputRecordsMap = outputRecords.stream()
      .collect(Collectors.toMap(rec -> rec.<Integer>get(ID.name()), rec -> rec));

    Stream.of(TEST_RECORDS).forEach(expected -> {
      int expectedId = (int) expected.get(ID);
      StructuredRecord actual = outputRecordsMap.get(expectedId);

      Assert.assertNotNull("Output records don't contain record with ID: " + expectedId, actual);

      // Verify data
      Assert.assertEquals(expected.get(NAME), actual.get(NAME.name()));
      Assert.assertEquals(expected.get(TINYTEXT_COL), actual.get(TINYTEXT_COL.name()));
      Assert.assertEquals(expected.get(MEDIUMTEXT_COL), actual.get(MEDIUMTEXT_COL.name()));
      Assert.assertEquals(expected.get(TEXT_COL), actual.get(TEXT_COL.name()));
      Assert.assertEquals(expected.get(LONGTEXT_COL), actual.get(LONGTEXT_COL.name()));
      Assert.assertEquals(expected.get(CHAR_COL), actual.get(CHAR_COL.name()));
      Assert.assertEquals(expected.get(SCORE), actual.get(SCORE.name()));
      Assert.assertEquals(expected.get(GRADUATED), actual.get(GRADUATED.name()));
      Assert.assertNull(actual.get(NOT_IMPORTED.name()));
      Assert.assertEquals(expected.get(ENUM_COL), actual.get(ENUM_COL.name()));
      Assert.assertEquals(expected.get(SET_COL), actual.get(SET_COL.name()));
      Assert.assertEquals(expected.get(TINY), actual.get(TINY.name()));
      Assert.assertEquals(expected.get(SMALL), actual.get(SMALL.name()));
      Assert.assertEquals(expected.get(BIG), actual.get(BIG.name()));
      Assert.assertEquals(expected.get(MEDIUMINT_COL), actual.get(MEDIUMINT_COL.name()));
      Assert.assertEquals((float) expected.get(FLOAT_COL), (float) actual.get(FLOAT_COL.name()), 0.001);
      Assert.assertEquals((double) expected.get(REAL_COL), (double) actual.get(REAL_COL.name()), 0.001);
      Assert.assertEquals(expected.get(BIT_COL), actual.get(BIT_COL.name()));

      // Verify time columns
      Assert.assertEquals(((Date) expected.get(DATE_COL)).toLocalDate(), actual.getDate(DATE_COL.name()));
      Assert.assertEquals(((Time) expected.get(TIME_COL)).toLocalTime(), actual.getTime(TIME_COL.name()));
      Assert.assertEquals(expected.get(YEAR_COL), (short) actual.getDate(YEAR_COL.name()).getYear());
      Assert.assertEquals(((Timestamp) expected.get(DATETIME_COL)).toLocalDateTime(),
                          actual.getTimestamp(DATETIME_COL.name()).toLocalDateTime());
      Assert.assertEquals(((Timestamp) expected.get(TIMESTAMP_COL)).toLocalDateTime(),
                          actual.getTimestamp(TIMESTAMP_COL.name()).toLocalDateTime());

      // verify binary columns
      String expectedBinaryString = new String((byte[]) expected.get(BINARY_COL)).trim();
      String binaryActual = new String(((ByteBuffer) actual.get(BINARY_COL.name())).array()).trim();
      Assert.assertEquals(expectedBinaryString, binaryActual);

      String expectedVarbinaryString = new String((byte[]) expected.get(VARBINARY_COL)).trim();
      String varbinaryActual = new String(((ByteBuffer) actual.get(VARBINARY_COL.name())).array()).trim();
      Assert.assertEquals(expectedVarbinaryString, varbinaryActual);

      String blobActual = new String(((ByteBuffer) actual.get(BLOB_COL.name())).array()).trim();
      Assert.assertEquals(expected.get(BLOB_COL), blobActual);

      String mediumBlobActual = new String(((ByteBuffer) actual.get(MEDIUMBLOB_COL.name())).array()).trim();
      Assert.assertEquals(expected.get(MEDIUMBLOB_COL), mediumBlobActual);

      String tinyBlobActual = new String(((ByteBuffer) actual.get(TINYBLOB_COL.name())).array()).trim();
      Assert.assertEquals(expected.get(TINYBLOB_COL), tinyBlobActual);

      String longBlobActual = new String(((ByteBuffer) actual.get(LONGBLOB_COL.name())).array()).trim();
      Assert.assertEquals(expected.get(LONGBLOB_COL), longBlobActual);

      // TODO seems there is a bug in StructuredRecord#getDecimal. ReferenceBatchSink#transform method receives
      // an instance of StructuredRecord where decimal field value serialized as java.nio.HeapBuffer, but
      // StructuredRecord#getDecimal implementation assumes field value to be raw bytes array.

      // This actually copies logic of StructuredRecord#getLogicalTypeSchema private method.
      Schema numericColSchema = actual.getSchema().getField(NUMERIC_COL.name()).getSchema();
      Schema logicalTypeSchema = null;
      for (Schema schema : numericColSchema.getUnionSchemas()) {
        Schema.LogicalType logicalType = schema.getLogicalType();
        if (logicalType != null && logicalType == Schema.LogicalType.DECIMAL) {
          logicalTypeSchema = schema;
        }
      }

      ByteBuffer numericRawValue = actual.get(NUMERIC_COL.name());
      BigDecimal actualNumeric = new BigDecimal(new BigInteger(numericRawValue.array()), logicalTypeSchema.getScale(),
                                                new MathContext(logicalTypeSchema.getPrecision()));
      BigDecimal expectedNumeric = (BigDecimal) expected.get(NUMERIC_COL);
      Assert.assertEquals(expectedNumeric.doubleValue(), actualNumeric.doubleValue(), 0.000001);
    });
  }

  @Test
  public void testDbSourceMultipleTables() throws Exception {
    String importQuery = "SELECT my_table.ID, your_table.NAME FROM my_table, your_table " +
      "WHERE my_table.ID < 3 and my_table.ID = your_table.ID and $CONDITIONS ";

    String boundingQuery = "SELECT LEAST(MIN(my_table.ID), MIN(your_table.ID)), " +
      "GREATEST(MAX(my_table.ID), MAX(your_table.ID))";
    String splitBy = "my_table.ID";
    ETLPlugin sourceConfig = new ETLPlugin(
      MysqlConstants.PLUGIN_NAME,
      BatchSource.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .putAll(BASE_PROPS)
        .put(MysqlConstants.AUTO_RECONNECT, "true")
        .put(MysqlConstants.USE_COMPRESSION, "true")
        .put(MysqlConstants.SQL_MODE, "ANSI_QUOTES,NO_ENGINE_SUBSTITUTION")
        .put(AbstractDBSource.DBSourceConfig.IMPORT_QUERY, importQuery)
        .put(AbstractDBSource.DBSourceConfig.BOUNDING_QUERY, boundingQuery)
        .put(AbstractDBSource.DBSourceConfig.SPLIT_BY, splitBy)
        .put(Constants.Reference.REFERENCE_NAME, "DBMultipleTest")
        .build(),
      null
    );

    String outputDatasetName = "output-multitabletest";
    ETLPlugin sinkConfig = MockSink.getPlugin(outputDatasetName);

    ApplicationManager appManager = deployETL(sourceConfig, sinkConfig,
                                              DATAPIPELINE_ARTIFACT, "testDBSourceWithMultipleTables");
    runETLOnce(appManager);

    // records should be written
    DataSetManager<Table> outputManager = getDataset(outputDatasetName);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);
    Assert.assertEquals(2, outputRecords.size());
    Map<Integer, StructuredRecord> outputRecordsMap = outputRecords.stream()
      .collect(Collectors.toMap(rec -> rec.<Integer>get(ID.name()), rec -> rec));

    Stream.of(TEST_RECORDS).forEach(expected -> {
      int expectedId = (int) expected.get(ID);
      StructuredRecord actual = outputRecordsMap.get(expectedId);
      Assert.assertNotNull("Output records don't contain record with ID: " + expectedId, actual);
      Assert.assertEquals(expected.get(NAME), actual.get(NAME.name()));
    });
  }

  @Test
  public void testUserNamePasswordCombinations() throws Exception {
    String importQuery = "SELECT * FROM my_table WHERE $CONDITIONS";
    String boundingQuery = "SELECT MIN(ID),MAX(ID) from my_table";
    String splitBy = "ID";

    ETLPlugin sinkConfig = MockSink.getPlugin("outputTable");

    Map<String, String> baseSourceProps = ImmutableMap.<String, String>builder()
      .put(MysqlConstants.AUTO_RECONNECT, "true")
      .put(MysqlConstants.USE_COMPRESSION, "true")
      .put(MysqlConstants.SQL_MODE, "ANSI_QUOTES,NO_ENGINE_SUBSTITUTION")
      .put(ConnectionConfig.HOST, BASE_PROPS.get(ConnectionConfig.HOST))
      .put(ConnectionConfig.PORT, BASE_PROPS.get(ConnectionConfig.PORT))
      .put(ConnectionConfig.DATABASE, BASE_PROPS.get(ConnectionConfig.DATABASE))
      .put(ConnectionConfig.JDBC_PLUGIN_NAME, JDBC_DRIVER_NAME)
      .put(AbstractDBSource.DBSourceConfig.IMPORT_QUERY, importQuery)
      .put(AbstractDBSource.DBSourceConfig.BOUNDING_QUERY, boundingQuery)
      .put(AbstractDBSource.DBSourceConfig.SPLIT_BY, splitBy)
      .put(Constants.Reference.REFERENCE_NAME, "UserPassDBTest")
      .build();

    ApplicationId appId = NamespaceId.DEFAULT.app("dbTest");

    // null user name, null password. Should succeed.
    // as source
    ETLPlugin dbConfig = new ETLPlugin(MysqlConstants.PLUGIN_NAME, BatchSource.PLUGIN_TYPE, baseSourceProps, null);
    ETLStage table = new ETLStage("uniqueTableSink", sinkConfig);
    ETLStage database = new ETLStage("databaseSource", dbConfig);
    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(database)
      .addStage(table)
      .addConnection(database.getName(), table.getName())
      .build();
    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, etlConfig);
    deployApplication(appId, appRequest);

    // null user name, non-null password. Should fail.
    // as source
    Map<String, String> noUser = new HashMap<>(baseSourceProps);
    noUser.put(ConnectionConfig.PASSWORD, "password");
    database = new ETLStage("databaseSource",
                            new ETLPlugin(MysqlConstants.PLUGIN_NAME, BatchSource.PLUGIN_TYPE, noUser, null));
    etlConfig = ETLBatchConfig.builder()
      .addStage(database)
      .addStage(table)
      .addConnection(database.getName(), table.getName())
      .build();
    assertDeploymentFailure(appId, etlConfig, DATAPIPELINE_ARTIFACT,
                            "Deploying DB Source with null username but non-null password should have failed.");

    // non-null username, non-null, but empty password. Should succeed.
    // as source
    Map<String, String> emptyPassword = new HashMap<>(baseSourceProps);
    emptyPassword.put(ConnectionConfig.USER, "root");
    emptyPassword.put(ConnectionConfig.PASSWORD, "");
    database = new ETLStage("databaseSource",
                            new ETLPlugin(MysqlConstants.PLUGIN_NAME, BatchSource.PLUGIN_TYPE, emptyPassword, null));
    etlConfig = ETLBatchConfig.builder()
      .addStage(database)
      .addStage(table)
      .addConnection(database.getName(), table.getName())
      .build();
    appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, etlConfig);
    deployApplication(appId, appRequest);
  }

  @Test
  public void testNonExistentDBTable() throws Exception {
    // source
    String importQuery = "SELECT ID, NAME FROM dummy WHERE ID < 3 AND $CONDITIONS";
    String boundingQuery = "SELECT MIN(ID),MAX(ID) FROM dummy";
    String splitBy = "ID";
    ETLPlugin sinkConfig = MockSink.getPlugin("table");
    ETLPlugin sourceBadNameConfig = new ETLPlugin(
      MysqlConstants.PLUGIN_NAME,
      BatchSource.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .putAll(BASE_PROPS)
        .put(MysqlConstants.AUTO_RECONNECT, "true")
        .put(MysqlConstants.USE_COMPRESSION, "true")
        .put(MysqlConstants.SQL_MODE, "ANSI_QUOTES,NO_ENGINE_SUBSTITUTION")
        .put(AbstractDBSource.DBSourceConfig.IMPORT_QUERY, importQuery)
        .put(AbstractDBSource.DBSourceConfig.BOUNDING_QUERY, boundingQuery)
        .put(AbstractDBSource.DBSourceConfig.SPLIT_BY, splitBy)
        .put(Constants.Reference.REFERENCE_NAME, "DBNonExistentTest")
        .build(),
      null);
    ETLStage sink = new ETLStage("sink", sinkConfig);
    ETLStage sourceBadName = new ETLStage("sourceBadName", sourceBadNameConfig);

    ETLBatchConfig etlConfig = ETLBatchConfig.builder()
      .addStage(sourceBadName)
      .addStage(sink)
      .addConnection(sourceBadName.getName(), sink.getName())
      .build();
    ApplicationId appId = NamespaceId.DEFAULT.app("dbSourceNonExistingTest");
    assertRuntimeFailure(appId, etlConfig, DATAPIPELINE_ARTIFACT,
                         "ETL Application with DB Source should have failed because of a " +
      "non-existent source table.", 1);

    // Bad connection
    ETLPlugin sourceBadConnConfig = new ETLPlugin(
      MysqlConstants.PLUGIN_NAME,
      BatchSource.PLUGIN_TYPE,
      ImmutableMap.<String, String>builder()
        .put(MysqlConstants.AUTO_RECONNECT, "true")
        .put(MysqlConstants.USE_COMPRESSION, "true")
        .put(MysqlConstants.SQL_MODE, "ANSI_QUOTES,NO_ENGINE_SUBSTITUTION")
        .put(ConnectionConfig.JDBC_PLUGIN_NAME, JDBC_DRIVER_NAME)
        .put(ConnectionConfig.HOST, BASE_PROPS.get(ConnectionConfig.HOST))
        .put(ConnectionConfig.PORT, BASE_PROPS.get(ConnectionConfig.PORT))
        .put(ConnectionConfig.DATABASE, "dumDB")
        .put(ConnectionConfig.USER, BASE_PROPS.get(ConnectionConfig.USER))
        .put(ConnectionConfig.PASSWORD, BASE_PROPS.get(ConnectionConfig.PASSWORD))
        .put(AbstractDBSource.DBSourceConfig.IMPORT_QUERY, importQuery)
        .put(AbstractDBSource.DBSourceConfig.BOUNDING_QUERY, boundingQuery)
        .put(AbstractDBSource.DBSourceConfig.SPLIT_BY, splitBy)
        .put(Constants.Reference.REFERENCE_NAME, "MySQLTest")
        .build(),
      null);
    ETLStage sourceBadConn = new ETLStage("sourceBadConn", sourceBadConnConfig);
    etlConfig = ETLBatchConfig.builder()
      .addStage(sourceBadConn)
      .addStage(sink)
      .addConnection(sourceBadConn.getName(), sink.getName())
      .build();
    assertRuntimeFailure(appId, etlConfig, DATAPIPELINE_ARTIFACT,
                         "ETL Application with DB Source should have failed because of a " +
      "non-existent source database.", 2);
  }
}
