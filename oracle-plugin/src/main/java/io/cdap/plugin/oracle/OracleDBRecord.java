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
import io.cdap.plugin.db.DBRecord;
import io.cdap.plugin.db.SchemaReader;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;

/**
 * Writable class for Oracle Source/Sink
 */
public class OracleDBRecord extends DBRecord {

  public OracleDBRecord(StructuredRecord record, int[] columnTypes, List<String> columns) {
    this.record = record;
    this.columnTypes = columnTypes;
    this.columns = columns;
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
      case OracleSchemaReader.TIMESTAMP_LTZ:
      case OracleSchemaReader.TIMESTAMP_TZ:
        Instant instant = resultSet.getTimestamp(columnIndex).toInstant();
        recordBuilder.setTimestamp(field.getName(), instant.atZone(ZoneId.ofOffset("UTC", ZoneOffset.UTC)));
        break;
      case OracleSchemaReader.BINARY_FLOAT:
        recordBuilder.set(field.getName(), canonicalFormatBytesToFloat(resultSet.getBytes(columnIndex)));
        break;
      case OracleSchemaReader.BINARY_DOUBLE:
        recordBuilder.set(field.getName(), canonicalFormatBytesToDouble(resultSet.getBytes(columnIndex)));
        break;
      case OracleSchemaReader.BFILE:
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

  private static float canonicalFormatBytesToFloat(byte[] binaryArray) {
    // BINARY_FLOAT value requires 4 bytes.
    int b0 = (int) binaryArray[0];
    int b1 = (int) binaryArray[1];
    int b2 = (int) binaryArray[2];
    int b3 = (int) binaryArray[3];

    if ((b0 & 128) != 0) {
      b0 &= 127;
      b1 &= 255;
      b2 &= 255;
      b3 &= 255;
    } else {
      b0 = ~b0 & 255;
      b1 = ~b1 & 255;
      b2 = ~b2 & 255;
      b3 = ~b3 & 255;
    }

    int intBits = b0 << 24 | b1 << 16 | b2 << 8 | b3;

    return Float.intBitsToFloat(intBits);
  }

  private static double canonicalFormatBytesToDouble(byte[] binaryArray) {
    // BINARY_DOUBLE value requires 8 bytes
    int b0 = (int) binaryArray[0];
    int b1 = (int) binaryArray[1];
    int b2 = (int) binaryArray[2];
    int b3 = (int) binaryArray[3];
    int b4 = (int) binaryArray[4];
    int b5 = (int) binaryArray[5];
    int b6 = (int) binaryArray[6];
    int b7 = (int) binaryArray[7];

    if ((b0 & 128) != 0) {
      b0 &= 127;
      b1 &= 255;
      b2 &= 255;
      b3 &= 255;
      b4 &= 255;
      b5 &= 255;
      b6 &= 255;
      b7 &= 255;
    } else {
      b0 = ~b0 & 255;
      b1 = ~b1 & 255;
      b2 = ~b2 & 255;
      b3 = ~b3 & 255;
      b4 = ~b4 & 255;
      b5 = ~b5 & 255;
      b6 = ~b6 & 255;
      b7 = ~b7 & 255;
    }

    int hiBits = b0 << 24 | b1 << 16 | b2 << 8 | b3;
    int loBits = b4 << 24 | b5 << 16 | b6 << 8 | b7;
    long longBits = (long) hiBits << 32 | (long) loBits & 4294967295L;

    return Double.longBitsToDouble(longBits);
  }
}
