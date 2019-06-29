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

import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.Types;
import java.util.Collections;
import java.util.Map;
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
  protected DBRecord getDBRecord(StructuredRecord.Builder output) {
    return new OracleDBRecord(output.build(), columnTypes);
  }

  @Override
  protected void transformField(StructuredRecord input, int index, StructuredRecord.Builder output) {
    String fieldName = columns.get(index);
    Schema.Field field = input.getSchema().getField(fieldName);
    if (field == null) {
      super.transformField(input, index, output);
      return;
    }

    int sqlType = columnTypes[index];
    Schema inputFieldSchema = field.getSchema().isNullable() ? field.getSchema().getNonNullable() : field.getSchema();
    Schema outputFieldSchema = outputSchema.getField(fieldName).getSchema().isNullable() ?
      outputSchema.getField(fieldName).getSchema() : outputSchema.getField(fieldName).getSchema();
    Schema decimalSchema = OracleUtil.getLogicalTypeSchema(outputFieldSchema,
                                                           Collections.singleton(Schema.LogicalType.DECIMAL));
    if (decimalSchema == null || !(sqlType == Types.NUMERIC || sqlType == Types.DECIMAL)) {
      super.transformField(input, index, output);
      return;
    }

    // Handle the case when output expects Decimal Logical Type but we got valid primitive.
    // This allows valid primitive values to be converted into corresponding instances of
    // BigDecimal(honoring scale and precision)
    // Thus we can support the following schema compatibility and do writes for the Sink:
    // 1) Schema.Type.INT -> Schema.LogicalType.DECIMAL (if precision of actual decimal logical type >= 10)
    // 2) Schema.Type.LONG -> Schema.LogicalType.DECIMAL (if precision of actual decimal logical type >= 19)
    // 3) Schema.Type.FLOAT -> Schema.LogicalType.DECIMAL (primitive value can be rounded to match actual schema)
    // 4) Schema.Type.DOUBLE -> Schema.LogicalType.DECIMAL (primitive value can be rounded to match actual schema)
    int precision = decimalSchema.getPrecision();
    int scale = decimalSchema.getScale();
    switch (inputFieldSchema.getType()) {
      case INT:
        BigDecimal decimalOfInt = new BigDecimal(input.<Integer>get(fieldName), new MathContext(precision));
        output.setDecimal(fieldName, decimalOfInt.setScale(scale, BigDecimal.ROUND_HALF_UP));
        break;
      case LONG:
        BigDecimal decimalOfLong = new BigDecimal(input.<Long>get(fieldName), new MathContext(precision));
        output.setDecimal(fieldName, decimalOfLong.setScale(scale, BigDecimal.ROUND_HALF_UP));
        break;
      case FLOAT:
        BigDecimal decimalOfFloat = new BigDecimal(input.<Float>get(fieldName), new MathContext(precision));
        output.setDecimal(fieldName, decimalOfFloat.setScale(scale, BigDecimal.ROUND_HALF_UP));
        break;
      case DOUBLE:
        BigDecimal decimalOfDouble = new BigDecimal(input.<Double>get(fieldName), new MathContext(precision));
        output.setDecimal(fieldName, decimalOfDouble.setScale(scale, BigDecimal.ROUND_HALF_UP));
        break;
      default:
        output.set(fieldName, input.get(fieldName));
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
