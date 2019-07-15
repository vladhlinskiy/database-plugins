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

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.plugin.db.DBRecord;
import io.cdap.plugin.db.SchemaReader;
import io.cdap.plugin.db.batch.config.DBSpecificSinkConfig;
import io.cdap.plugin.db.batch.sink.AbstractDBSink;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Sink support for Oracle database.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(OracleConstants.PLUGIN_NAME)
@Description("Writes records to Oracle table. Each record will be written in a row in the table")
public class OracleSink extends AbstractDBSink {

  private final OracleSinkConfig oracleSinkConfig;

  public OracleSink(OracleSinkConfig oracleSinkConfig) {
    super(oracleSinkConfig);
    this.oracleSinkConfig = oracleSinkConfig;
  }

  @Override
  protected DBRecord getDBRecord(StructuredRecord output) {
    return new OracleDBRecord(driverClass, output, columnTypes);
  }

  @Override
  public boolean isFieldCompatible(Schema.Field field, ResultSetMetaData metadata, int index) throws SQLException {
    Schema outputFieldSchema = getSchemaReader().getSchema(metadata, index);
    Schema outputFieldNonNullableSchema = outputFieldSchema.isNullable()
      ? outputFieldSchema.getNonNullable()
      : outputFieldSchema;
    Schema inputFieldNonNullableSchema = field.getSchema().isNullable()
      ? field.getSchema().getNonNullable()
      : field.getSchema();
    // Handle the case when output schema expects decimal logical type but we got valid primitive.
    // It's safe to write primitives as values of decimal logical type in the case of valid precision.
    if (Schema.LogicalType.DECIMAL == outputFieldNonNullableSchema.getLogicalType()) {
      int precision = metadata.getPrecision(index);
      switch (inputFieldNonNullableSchema.getType()) {
        case INT:
          // With 10 digits we can represent Integer.MAX_VALUE.
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
          return super.isFieldCompatible(field, metadata, index);
      }
    } else if (OracleSchemaReader.ORACLE_TYPES.contains(metadata.getColumnType(index))) {
      return Objects.equals(inputFieldNonNullableSchema.getType(), outputFieldNonNullableSchema.getType()) &&
        Objects.equals(inputFieldNonNullableSchema.getLogicalType(), outputFieldNonNullableSchema.getLogicalType());
    } else {
      return super.isFieldCompatible(field, metadata, index);
    }
  }

  @Override
  protected SchemaReader getSchemaReader() {
    return new OracleSchemaReader();
  }

  /**
   * Oracle action configuration.
   */
  public static class OracleSinkConfig extends DBSpecificSinkConfig {
    @Name(OracleConstants.DEFAULT_BATCH_VALUE)
    @Description("The default batch value that triggers an execution request.")
    @Nullable
    public Integer defaultBatchValue;

    @Name(OracleConstants.CONNECTION_TYPE)
    @Description("Whether to use an SID or Service Name when connecting to the database.")
    public String connectionType;

    @Override
    public String getConnectionString() {
      if (OracleConstants.SERVICE_CONNECTION_TYPE.equals(this.connectionType)) {
        return String.format(OracleConstants.ORACLE_CONNECTION_SERVICE_NAME_STRING_FORMAT, host, port, database);
      }
      return String.format(OracleConstants.ORACLE_CONNECTION_STRING_FORMAT, host, port, database);
    }

    @Override
    protected Map<String, String> getDBSpecificArguments() {
      return ImmutableMap.of(OracleConstants.DEFAULT_BATCH_VALUE, String.valueOf(defaultBatchValue));
    }
  }
}
