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

package io.cdap.plugin.postgres;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.plugin.db.ColumnType;
import io.cdap.plugin.db.batch.config.DBSpecificSinkConfig;
import io.cdap.plugin.db.batch.sink.AbstractDBSink;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Sink support for a PostgreSQL database.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(PostgresConstants.PLUGIN_NAME)
@Description("Writes records to a PostgreSQL table. Each record will be written in a row in the table")
public class PostgresSink extends AbstractDBSink {
  private static final Character ESCAPE_CHAR = '"';

  private final PostgresSinkConfig postgresSinkConfig;

  public PostgresSink(PostgresSinkConfig postgresSinkConfig) {
    super(postgresSinkConfig);
    this.postgresSinkConfig = postgresSinkConfig;
  }

  @Override
  protected void setColumnsInfo(List<Schema.Field> fields) {
    super.columnTypes = fields.stream()
      .map(Schema.Field::getName)
      .map(name -> ESCAPE_CHAR + name + ESCAPE_CHAR)
      .map(ColumnType::new)
      .collect(Collectors.collectingAndThen(Collectors.toList(), Collections::unmodifiableList));

    super.dbColumns = columnTypes.stream()
      .map(ColumnType::getName)
      .collect(Collectors.joining(","));
  }

  /**
   * PostgreSQL action configuration.
   */
  public static class PostgresSinkConfig extends DBSpecificSinkConfig {

    @Name(PostgresConstants.CONNECTION_TIMEOUT)
    @Description("The timeout value used for socket connect operations. If connecting to the server takes longer" +
      " than this value, the connection is broken. " +
      "The timeout is specified in seconds and a value of zero means that it is disabled")
    @Nullable
    public Integer connectionTimeout;

    @Override
    public String getConnectionString() {
      return String.format(PostgresConstants.POSTGRES_CONNECTION_STRING_FORMAT, host, port, database);
    }

    @Override
    protected String getEscapedTableName() {
      return ESCAPE_CHAR + tableName + ESCAPE_CHAR;
    }

    @Override
    public Map<String, String> getDBSpecificArguments() {
      return ImmutableMap.of(PostgresConstants.CONNECTION_TIMEOUT, String.valueOf(connectionTimeout));
    }
  }
}
