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

package io.cdap.plugin.db2;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Objects;


/**
 * Sink support for a DB2 database.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(Db2Constants.PLUGIN_NAME)
@Description("Writes records to a DB2 table. Each record will be written in a row in the table.")
public class Db2Sink extends AbstractDBSink {
  private static final Logger LOG = LoggerFactory.getLogger(Db2Sink.class);

  private final Db2SinkConfig db2SinkConfig;

  public Db2Sink(Db2SinkConfig db2SinkConfig) {
    super(db2SinkConfig);
    this.db2SinkConfig = db2SinkConfig;
  }

  /**
   * DB2 action configuration.
   */
  public static class Db2SinkConfig extends DBSpecificSinkConfig {
    @Override
    public String getConnectionString() {
      return String.format(Db2Constants.DB2_CONNECTION_STRING_FORMAT, host, port, database);
    }
  }

  @Override
  protected DBRecord getDBRecord(StructuredRecord output) {
    return new DB2Record(output, columnTypes, columns);
  }

  @Override
  protected SchemaReader getSchemaReader() {
    return new DB2SchemaReader();
  }

  @Override
  protected boolean isFieldCompatible(Schema.Field field, ResultSetMetaData metadata, int index) throws SQLException {
    Schema.Type fieldType = field.getSchema().isNullable() ? field.getSchema().getNonNullable().getType()
      : field.getSchema().getType();

    //DECFLOAT is mapped to string
    String colTypeName = metadata.getColumnTypeName(index);
    if (DB2SchemaReader.DB2_DECFLOAT.equals(colTypeName)) {
      if (Objects.equals(fieldType, Schema.Type.STRING)) {
        return true;
      } else {
        LOG.error("Field '{}' was given as type '{}' but must be of type 'string' for the DB2 column of " +
                    "DECFLOAT type.", field.getName(), fieldType);
        return false;
      }
    }

    return super.isFieldCompatible(field, metadata, index);
  }
}
