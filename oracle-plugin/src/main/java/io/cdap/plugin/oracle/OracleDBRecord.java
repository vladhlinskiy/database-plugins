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
import io.cdap.plugin.db.ColumnType;
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
        recordBuilder.set(field.getName(), resultSet.getFloat(columnIndex));
        break;
      case OracleSchemaReader.BINARY_DOUBLE:
        recordBuilder.set(field.getName(), resultSet.getDouble(columnIndex));
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
}
