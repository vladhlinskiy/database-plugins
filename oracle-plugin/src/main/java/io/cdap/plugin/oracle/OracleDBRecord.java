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

package io.cdap.plugin.oracle;

import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.InvalidStageException;
import io.cdap.plugin.db.ColumnType;
import io.cdap.plugin.db.DBRecord;
import io.cdap.plugin.db.SchemaReader;

import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Writable class for Oracle Source/Sink
 */
public class OracleDBRecord extends DBRecord {

  public OracleDBRecord(StructuredRecord record, List<ColumnType> columnTypes) {
    this.record = record;
    this.columnTypes = columnTypes;
  }

  /**
   * Used in map-reduce. Do not remove.
   */
  @SuppressWarnings("unused")
  public OracleDBRecord() {
  }

  @Override
  protected SchemaReader getSchemaReader() {
    return new OracleSchemaReader();
  }

  /**
   * Builds the {@link #record} using the specified {@link ResultSet} for Oracle DB
   *
   * @param resultSet the {@link ResultSet} to build the {@link StructuredRecord} from
   */
  @Override
  public void readFields(ResultSet resultSet) throws SQLException {
    Schema schema = getSchema();
    ResultSetMetaData metadata = resultSet.getMetaData();
    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(schema);

    // All LONG or LONG RAW columns have to be retrieved from the ResultSet prior to all the other columns.
    // Otherwise, we will face java.sql.SQLException: Stream has already been closed
    for (int i = 0; i < schema.getFields().size(); i++) {
      if (isLongOrLongRaw(metadata.getColumnType(i + 1))) {
        readField(i, metadata, resultSet, schema, recordBuilder);
      }
    }

    // Read fields of other types
    for (int i = 0; i < schema.getFields().size(); i++) {
      if (!isLongOrLongRaw(metadata.getColumnType(i + 1))) {
        readField(i, metadata, resultSet, schema, recordBuilder);
      }
    }

    record = recordBuilder.build();
  }

  @Override
  protected void handleField(ResultSet resultSet, StructuredRecord.Builder recordBuilder, Schema.Field field,
                             int columnIndex, int sqlType, int sqlPrecision, int sqlScale) throws SQLException {
    if (OracleSchemaReader.ORACLE_TYPES.contains(sqlType) || sqlType == Types.NCLOB) {
      handleOracleSpecificType(resultSet, recordBuilder, field, columnIndex, sqlType, sqlPrecision, sqlScale);
    } else {
      setField(resultSet, recordBuilder, field, columnIndex, sqlType, sqlPrecision, sqlScale);
    }
  }

  @Override
  protected void writeToDB(PreparedStatement stmt, @Nullable Schema.Field field, int fieldIndex) throws SQLException {
    int sqlType = columnTypes.get(fieldIndex).getType();
    int sqlIndex = fieldIndex + 1;
    switch (sqlType) {
      case OracleSchemaReader.TIMESTAMP_TZ:
        if (field != null && record.get(field.getName()) != null) {
          // Set value of Oracle 'TIMESTAMP WITH TIME ZONE' data type as instance of 'oracle.sql.TIMESTAMPTZ',
          // created from timestamp string, such as "2019-07-15 15:57:46.65 GMT".
          String timestampString = record.get(field.getName());
          Object timestampWithTimeZone = createOracleTimestampWithTimeZone(stmt.getConnection(), timestampString);
          stmt.setObject(sqlIndex, timestampWithTimeZone);
        } else {
          stmt.setNull(sqlIndex, sqlType);
        }
        break;
      case OracleSchemaReader.BFILE:
        if (field != null && record.get(field.getName()) != null) {
          // Set value of Oracle 'BFILE' data type as instance of 'oracle.sql.BFILE'.
          // Note, that we create only locator (link) to an external binary file (file stored outside of the database)
          // and not content of the file.
          Object value = record.get(field.getName());
          byte[] bytes = value instanceof ByteBuffer ? Bytes.toBytes((ByteBuffer) value) : (byte[]) value;
          Object bfile = createOracleBfile(stmt.getConnection(), bytes);
          stmt.setObject(sqlIndex, bfile);
        } else {
          stmt.setNull(sqlIndex, sqlType);
        }
        break;
      default:
        super.writeToDB(stmt, field, fieldIndex);
    }
  }

  /**
   * Creates an instance of 'oracle.sql.TIMESTAMPTZ' which corresponds to the specified timestamp with time zone string.
   * @param connection sql connection.
   * @param timestampString timestamp with time zone string, such as "2019-07-15 15:57:46.65 GMT".
   * @return instance of 'oracle.sql.TIMESTAMPTZ' which corresponds to the specified timestamp with time zone string.
   */
  private Object createOracleTimestampWithTimeZone(Connection connection, String timestampString) {
    try {
      ClassLoader classLoader = connection.getClass().getClassLoader();
      Class<?> timestampTZClass = classLoader.loadClass("oracle.sql.TIMESTAMPTZ");
      return timestampTZClass.getConstructor(Connection.class, String.class).newInstance(connection, timestampString);
    } catch (ClassNotFoundException e) {
      throw new InvalidStageException("Unable to load 'oracle.sql.TIMESTAMPTZ'.", e);
    } catch (InstantiationException | InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
      throw new InvalidStageException("Unable to instantiate 'oracle.sql.TIMESTAMPTZ'.", e);
    }
  }

  /**
   * Creates an instance of 'oracle.sql.BFILE'. Note, that we can not create an operating system file that a 'BFILE'
   * would refer to. <a href="https://docs.oracle.com/cd/B19306_01/java.102/b14355/oralob.htm#BABJJEIC">Those
   * are created only externally.</a>
   * @param connection sql connection.
   * @param bytes BFILE locator's bytes.
   * @return instance of 'oracle.sql.BFILE'.
   */
  private Object createOracleBfile(Connection connection, byte[] bytes) {
    try {
      ClassLoader classLoader = connection.getClass().getClassLoader();
      Class<?> oracleConnectionClass = classLoader.loadClass("oracle.jdbc.OracleConnection");
      Class<?> bfileClass = classLoader.loadClass("oracle.sql.BFILE");
      return bfileClass.getConstructor(oracleConnectionClass, byte[].class).newInstance(connection, bytes);
    } catch (ClassNotFoundException e) {
      throw new InvalidStageException("Unable to load Oracle JDBC connector.", e);
    } catch (InstantiationException | InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
      throw new InvalidStageException("Unable to instantiate 'oracle.sql.BFILE'.", e);
    }
  }

  /**
   * Retrieves bytes representation of 'oracle.sql.BFILE' via 'oracle.sql.BFILE#getBytes'. Note, that this method
   * retrieves only locator (link) to an external binary file (file stored outside of the database) and not content of
   * the file. <a href="https://docs.oracle.com/cd/B19306_01/java.102/b14355/oralob.htm#BABJJEIC">Files are created
   * only externally.</a>
   * @param bfile instance of 'oracle.sql.BFILE'.
   * @return bytes representation of 'oracle.sql.BFILE' obtained via 'oracle.sql.BFILE#getBytes'.
   */
  private byte[] getBfileBytes(ResultSet resultSet, Object bfile) {
    if (bfile == null) {
      return null;
    }
    try {
      ClassLoader classLoader = resultSet.getClass().getClassLoader();
      Class<?> bfileClass = classLoader.loadClass("oracle.sql.BFILE");
      return (byte[]) bfileClass.getMethod("getBytes").invoke(bfile);
    } catch (ClassNotFoundException e) {
      throw new InvalidStageException("Unable to load 'oracle.sql.BFILE'.", e);
    } catch (InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
      throw new InvalidStageException("Error while invoking 'oracle.sql.BFILE#getBytes()'.", e);
    }
  }

  private void handleOracleSpecificType(ResultSet resultSet, StructuredRecord.Builder recordBuilder, Schema.Field field,
                                        int columnIndex, int sqlType, int precision, int scale)
    throws SQLException {
    switch (sqlType) {
      case OracleSchemaReader.INTERVAL_YM:
      case OracleSchemaReader.INTERVAL_DS:
      case OracleSchemaReader.LONG:
      case Types.NCLOB:
        recordBuilder.set(field.getName(), resultSet.getString(columnIndex));
        break;
      case OracleSchemaReader.TIMESTAMP_TZ:
        recordBuilder.set(field.getName(), resultSet.getString(columnIndex));
        break;
      case OracleSchemaReader.TIMESTAMP_LTZ:
        Instant instant = resultSet.getTimestamp(columnIndex).toInstant();
        recordBuilder.setTimestamp(field.getName(), instant.atZone(ZoneId.ofOffset("UTC", ZoneOffset.UTC)));
        break;
      case OracleSchemaReader.BINARY_FLOAT:
        recordBuilder.set(field.getName(), resultSet.getFloat(columnIndex));
        break;
      case OracleSchemaReader.BINARY_DOUBLE:
        recordBuilder.set(field.getName(), resultSet.getDouble(columnIndex));
        break;
      case OracleSchemaReader.BFILE:
        // Note, that ResultSet#getObject retrieves only locator (link) to an external binary file (file stored outside
        // of the database) and not content of the file.
        Object bfile = resultSet.getObject(columnIndex);
        recordBuilder.set(field.getName(), getBfileBytes(resultSet, bfile));
        break;
      case OracleSchemaReader.LONG_RAW:
        recordBuilder.set(field.getName(), resultSet.getBytes(columnIndex));
        break;
      case Types.DECIMAL:
      case Types.NUMERIC:
        // This is the only way to differentiate FLOAT/REAL columns from other numeric columns, that based on NUMBER.
        // Since FLOAT is a subtype of the NUMBER data type, 'getColumnType' and 'getColumnTypeName' can not be used.
        if (Double.class.getTypeName().equals(resultSet.getMetaData().getColumnClassName(columnIndex))) {
          recordBuilder.set(field.getName(), resultSet.getDouble(columnIndex));
        } else {
          // It's required to pass 'scale' parameter since in the case of Oracle, scale of 'BigDecimal' depends on the
          // scale of actual value. For example for value '77.12' scale will be '2' even if sql scale is '6'
          BigDecimal decimal = resultSet.getBigDecimal(columnIndex, scale);
          recordBuilder.setDecimal(field.getName(), decimal);
        }
    }
  }

  private boolean isLongOrLongRaw(int columnType) {
    return columnType == OracleSchemaReader.LONG || columnType == OracleSchemaReader.LONG_RAW;
  }

  private void readField(int index, ResultSetMetaData metadata, ResultSet resultSet, Schema schema,
                         StructuredRecord.Builder recordBuilder) throws SQLException {
    Schema.Field field = schema.getFields().get(index);
    int columnIndex = index + 1;
    int sqlType = metadata.getColumnType(columnIndex);
    int sqlPrecision = metadata.getPrecision(columnIndex);
    int sqlScale = metadata.getScale(columnIndex);

    handleField(resultSet, recordBuilder, field, columnIndex, sqlType, sqlPrecision, sqlScale);
  }

  @Override
  protected void writeBytes(PreparedStatement stmt, int fieldIndex, int sqlIndex, Object fieldValue)
    throws SQLException {
    byte[] byteValue = fieldValue instanceof ByteBuffer ? Bytes.toBytes((ByteBuffer) fieldValue) : (byte[]) fieldValue;
    // handles BINARY, VARBINARY and LOGVARBINARY
    stmt.setBytes(sqlIndex, byteValue);
  }
}
