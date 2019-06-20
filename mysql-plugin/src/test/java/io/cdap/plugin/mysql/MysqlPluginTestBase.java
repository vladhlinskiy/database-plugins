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
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.plugin.db.ConnectionConfig;
import io.cdap.plugin.db.DBRecord;
import io.cdap.plugin.db.batch.DatabasePluginTestBase;
import io.cdap.plugin.db.batch.sink.ETLDBOutputFormat;
import io.cdap.plugin.db.batch.source.DataDrivenETLDBInputFormat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Collections;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

public class MysqlPluginTestBase extends DatabasePluginTestBase {
  protected static final ArtifactId DATAPIPELINE_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("data-pipeline", "3.2.0");
  protected static final ArtifactSummary DATAPIPELINE_ARTIFACT = new ArtifactSummary("data-pipeline", "3.2.0");
  protected static final String DRIVER_CLASS = "com.mysql.jdbc.Driver";
  protected static final String JDBC_DRIVER_NAME = "mysql";

  protected static String connectionUrl;
  protected static boolean tearDown = true;
  private static int startCount;

  protected static final TestRecord FIRST_RECORD = TestRecord.newRecord()
    .set(ID, 1)
    .set(TestRecord.Column.NAME, "some name")
    .set(SCORE, 123.45)
    .set(GRADUATED, true)
    .set(NOT_IMPORTED, "some record")
    .set(TINY, 10)
    .set(SMALL, 10)
    .set(BIG, 15121L)
    .set(MEDIUMINT_COL, 1515135)
    .set(FLOAT_COL, 123.451f)
    .set(REAL_COL, 123.454d)
    .set(NUMERIC_COL, new BigDecimal(123.451, new MathContext(10)).setScale(6))
    .set(DECIMAL_COL, new BigDecimal(123.451, new MathContext(10)).setScale(6))
    .set(BIT_COL, true)
    .set(DATE_COL, new Date(System.currentTimeMillis()))
    .set(TIME_COL, new Time(System.currentTimeMillis()))
    .set(TIMESTAMP_COL, new Timestamp(System.currentTimeMillis()))
    .set(DATETIME_COL, new Timestamp(System.currentTimeMillis()))
    .set(YEAR_COL, (short) Calendar.getInstance().get(Calendar.YEAR))
    .set(TEXT_COL, "some record")
    .set(TINYTEXT_COL, "some record")
    .set(MEDIUMTEXT_COL, "some record")
    .set(LONGTEXT_COL, "some record")
    .set(CHAR_COL, "some record")
    .set(BINARY_COL, "some record".getBytes(Charsets.UTF_8))
    .set(VARBINARY_COL, "some record".getBytes(Charsets.UTF_8))
    .set(TINYBLOB_COL, "some record")
    .set(BLOB_COL, "some record")
    .set(MEDIUMBLOB_COL, "some record")
    .set(LONGBLOB_COL, "some record")
    .set(ENUM_COL, "Second")
    .set(SET_COL, "a,b");

  protected static final TestRecord SECOND_RECORD = TestRecord.copyOf(FIRST_RECORD)
    .set(ID, 2)
    .set(GRADUATED, false)
    .set(BIT_COL, false);

  protected static final TestRecord[] TEST_RECORDS = {
    FIRST_RECORD,
    SECOND_RECORD
  };

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  protected static final Map<String, String> BASE_PROPS = ImmutableMap.<String, String>builder()
    .put(ConnectionConfig.HOST, System.getProperty("mysql.host"))
    .put(ConnectionConfig.PORT, System.getProperty("mysql.port"))
    .put(ConnectionConfig.DATABASE, System.getProperty("mysql.database"))
    .put(ConnectionConfig.USER, System.getProperty("mysql.username"))
    .put(ConnectionConfig.PASSWORD, System.getProperty("mysql.password"))
    .put(ConnectionConfig.JDBC_PLUGIN_NAME, JDBC_DRIVER_NAME)
    .build();

  @BeforeClass
  public static void setupTest() throws Exception {
    if (startCount++ > 0) {
      return;
    }

    setupBatchArtifacts(DATAPIPELINE_ARTIFACT_ID, DataPipelineApp.class);

    addPluginArtifact(NamespaceId.DEFAULT.artifact(JDBC_DRIVER_NAME, "1.0.0"),
                      DATAPIPELINE_ARTIFACT_ID,
                      MysqlSource.class, MysqlSink.class, DBRecord.class, ETLDBOutputFormat.class,
                      DataDrivenETLDBInputFormat.class, DBRecord.class, MysqlPostAction.class, MysqlAction.class);

    Class<?> driverClass = Class.forName(DRIVER_CLASS);

    // add mysql 3rd party plugin
    PluginClass mysqlDriver = new PluginClass(ConnectionConfig.JDBC_PLUGIN_TYPE, JDBC_DRIVER_NAME, "mysql driver class",
                                              driverClass.getName(),
                                              null, Collections.emptyMap());
    addPluginArtifact(NamespaceId.DEFAULT.artifact("mysql-jdbc-connector", "1.0.0"),
                      DATAPIPELINE_ARTIFACT_ID,
                      Sets.newHashSet(mysqlDriver), driverClass);

    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

    connectionUrl = "jdbc:mysql://" + BASE_PROPS.get(ConnectionConfig.HOST) + ":" +
      BASE_PROPS.get(ConnectionConfig.PORT) + "/" + BASE_PROPS.get(ConnectionConfig.DATABASE);
    Connection conn = createConnection();
    createTestTables(conn);
    prepareTestData(conn);
  }

  protected static void createTestTables(Connection conn) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      // create a table that the action will truncate at the end of the run
      stmt.execute("CREATE TABLE dbActionTest (x int, day varchar(10))");
      // create a table that the action will truncate at the end of the run
      stmt.execute("CREATE TABLE postActionTest (x int, day varchar(10))");

      String columnsDefinition = Stream.of(TestRecord.Column.values())
        .map(column -> String.format("%s %s", column.name(), column.sqlType()))
        .collect(Collectors.joining(", "));

      stmt.execute("CREATE TABLE my_table (" + columnsDefinition + ")");
      stmt.execute("CREATE TABLE MY_DEST_TABLE AS SELECT * FROM my_table");
      stmt.execute("CREATE TABLE your_table AS SELECT * FROM my_table");
    }
  }

  protected static void prepareTestData(Connection conn) throws SQLException {
    String placeholders = Stream.of(TestRecord.Column.values())
      .map(column -> "?")
      .collect(Collectors.joining(", "));

    try (
      Statement stmt = conn.createStatement();
      PreparedStatement pStmt1 = conn.prepareStatement("INSERT INTO my_table VALUES(" + placeholders + ")");
      PreparedStatement pStmt2 = conn.prepareStatement("INSERT INTO your_table VALUES(" + placeholders + ")")) {

      stmt.execute("insert into dbActionTest values (1, '1970-01-01')");
      stmt.execute("insert into postActionTest values (1, '1970-01-01')");

      populateData(pStmt1, pStmt2);
    }
  }

  private static void populateData(PreparedStatement ...stmts) throws SQLException {
    // insert the same record into both tables: my_table and your_table
    for (PreparedStatement pStmt : stmts) {
      for (TestRecord record : TEST_RECORDS) {
        pStmt.setInt(ID.index(), (int) record.get(ID));
        pStmt.setString(NAME.index(), (String) record.get(NAME));
        pStmt.setDouble(SCORE.index(), (double) record.get(SCORE));
        pStmt.setBoolean(GRADUATED.index(), (boolean) record.get(GRADUATED));
        pStmt.setString(NOT_IMPORTED.index(), (String) record.get(NOT_IMPORTED));
        pStmt.setInt(TINY.index(), (int) record.get(TINY));
        pStmt.setInt(SMALL.index(), (int) record.get(SMALL));
        pStmt.setInt(MEDIUMINT_COL.index(), (int) record.get(MEDIUMINT_COL));
        pStmt.setLong(BIG.index(), (long) record.get(BIG));
        pStmt.setFloat(FLOAT_COL.index(), (float) record.get(FLOAT_COL));
        pStmt.setDouble(REAL_COL.index(), (double) record.get(REAL_COL));
        pStmt.setBigDecimal(NUMERIC_COL.index(), (BigDecimal) record.get(NUMERIC_COL));
        pStmt.setBigDecimal(DECIMAL_COL.index(), (BigDecimal) record.get(DECIMAL_COL));
        pStmt.setBoolean(BIT_COL.index(), (boolean) record.get(BIT_COL));
        pStmt.setDate(DATE_COL.index(), (Date) record.get(DATE_COL));
        pStmt.setTime(TIME_COL.index(), (Time) record.get(TIME_COL));
        pStmt.setTimestamp(TIMESTAMP_COL.index(), (Timestamp) record.get(TIMESTAMP_COL));
        pStmt.setTimestamp(DATETIME_COL.index(), (Timestamp) record.get(DATETIME_COL));
        pStmt.setShort(YEAR_COL.index(), (short) record.get(YEAR_COL));
        pStmt.setString(TEXT_COL.index(), (String) record.get(TEXT_COL));
        pStmt.setString(TINYTEXT_COL.index(), (String) record.get(TINYTEXT_COL));
        pStmt.setString(MEDIUMTEXT_COL.index(), (String) record.get(MEDIUMTEXT_COL));
        pStmt.setString(LONGTEXT_COL.index(), (String) record.get(LONGTEXT_COL));
        pStmt.setString(CHAR_COL.index(), (String) record.get(CHAR_COL));
        pStmt.setBytes(BINARY_COL.index(), (byte[]) record.get(BINARY_COL));
        pStmt.setBytes(VARBINARY_COL.index(), (byte[]) record.get(VARBINARY_COL));
        pStmt.setBlob(TINYBLOB_COL.index(), record.getBlobOf(TINYBLOB_COL));
        pStmt.setBlob(BLOB_COL.index(), record.getBlobOf(BLOB_COL));
        pStmt.setBlob(MEDIUMBLOB_COL.index(), record.getBlobOf(MEDIUMBLOB_COL));
        pStmt.setBlob(LONGBLOB_COL.index(), record.getBlobOf(LONGBLOB_COL));
        pStmt.setString(ENUM_COL.index(), (String) record.get(ENUM_COL));
        pStmt.setString(SET_COL.index(), (String) record.get(SET_COL));

        pStmt.executeUpdate();
      }
    }
  }

  public static Connection createConnection() {
    try {
      Class.forName(DRIVER_CLASS);
      return DriverManager.getConnection(connectionUrl, BASE_PROPS.get(ConnectionConfig.USER),
                                         BASE_PROPS.get(ConnectionConfig.PASSWORD));
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @AfterClass
  public static void tearDownDB() throws SQLException {
    if (!tearDown) {
      return;
    }

    try (Connection conn = createConnection();
         Statement stmt = conn.createStatement()) {
      stmt.execute("DROP TABLE my_table");
      stmt.execute("DROP TABLE your_table");
      stmt.execute("DROP TABLE postActionTest");
      stmt.execute("DROP TABLE dbActionTest");
      stmt.execute("DROP TABLE MY_DEST_TABLE");
    }
  }
}
