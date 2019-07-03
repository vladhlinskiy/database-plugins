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

import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.db.CommonSchemaReader;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Objects;
import java.util.Set;

/**
 * Oracle schema reader.
 */
public class OracleSchemaReader extends CommonSchemaReader {
  /**
   * Oracle type constants, from Oracle JDBC Implementation.
   */
  public static final int INTERVAL_YM = -103;
  public static final int INTERVAL_DS = -104;
  public static final int TIMESTAMP_TZ = -101;
  public static final int TIMESTAMP_LTZ = -102;
  public static final int BINARY_FLOAT = 100;
  public static final int BINARY_DOUBLE = 101;
  public static final int BFILE = -13;
  public static final int LONG = -1;
  public static final int LONG_RAW = -4;

  public static final Set<Integer> ORACLE_TYPES = ImmutableSet.of(
    INTERVAL_DS,
    INTERVAL_YM,
    TIMESTAMP_TZ,
    TIMESTAMP_LTZ,
    BINARY_FLOAT,
    BINARY_DOUBLE,
    BFILE,
    LONG,
    LONG_RAW,
    Types.NUMERIC,
    Types.DECIMAL
  );

  @Override
  public Schema getSchema(ResultSetMetaData metadata, int index) throws SQLException {
    int sqlType = metadata.getColumnType(index);

    switch (sqlType) {
      case TIMESTAMP_TZ:
      case TIMESTAMP_LTZ:
        return Schema.of(Schema.LogicalType.TIMESTAMP_MICROS);
      case BINARY_FLOAT:
        return Schema.of(Schema.Type.FLOAT);
      case BINARY_DOUBLE:
        return Schema.of(Schema.Type.DOUBLE);
      case BFILE:
      case LONG_RAW:
        return Schema.of(Schema.Type.BYTES);
      case INTERVAL_DS:
      case INTERVAL_YM:
      case LONG:
        return Schema.of(Schema.Type.STRING);
      case Types.NUMERIC:
      case Types.DECIMAL:
        // This is the only way to differentiate FLOAT/REAL columns from other numeric columns, that based on NUMBER.
        // Since in Oracle FLOAT is a subtype of the NUMBER data type, 'getColumnType' and 'getColumnTypeName' can not
        // be used.
        if (Double.class.getTypeName().equals(metadata.getColumnClassName(index))) {
          return Schema.of(Schema.Type.DOUBLE);
        } else {
          return super.getSchema(metadata, index);
        }
      default:
        return super.getSchema(metadata, index);
    }
  }

  @Override
  public boolean isTypeCompatible(Schema.Field field, ResultSetMetaData metadata, int index) throws SQLException {

    Schema outputFieldSchema = getSchema(metadata, index);
    Schema outputFieldNonNullableSchema = outputFieldSchema.isNullable()
      ? outputFieldSchema.getNonNullable()
      : outputFieldSchema;
    Schema inputFieldNonNullableSchema = field.getSchema().isNullable()
      ? field.getSchema().getNonNullable()
      : field.getSchema();

    // This is the only way to differentiate FLOAT/REAL columns from other numeric columns, that based on NUMBER.
    // Since FLOAT is a subtype of the NUMBER data type, 'getColumnType' and 'getColumnTypeName' can not be used.
    if (Double.class.getTypeName().equals(metadata.getColumnClassName(index))) {
      return Objects.equals(inputFieldNonNullableSchema.getType(), outputFieldNonNullableSchema.getType());
    }

    // Handle the case when output schema expects Decimal Logical Type but we got valid primitive.
    // This allows primitive values to be converted into corresponding instances of
    // BigDecimal(honoring scale and precision)
    if (Schema.LogicalType.DECIMAL == outputFieldNonNullableSchema.getLogicalType()) {
      int precision = metadata.getPrecision(index);
      switch (inputFieldNonNullableSchema.getType()) {
        case INT:
          // With 10 digits we can represent Integer.MAX_VALUE so it's safe to w
          // It is equal to the value returned by (new BigDecimal(Integer.MAX_VALUE)).precision()
          return precision >= 10;
        case LONG:
          // With 19 digits we can represent Long.MAX_VALUE.
          // It is equal to the value returned by (new BigDecimal(Long.MAX_VALUE)).precision()
          return precision >= 19;
        case FLOAT:
        case DOUBLE:
          // Actual value can be rounded to match output schema
          return true;
        default:
          return super.isTypeCompatible(field, metadata, index);
      }
    } else {
      return super.isTypeCompatible(field, metadata, index);
    }
  }
}
