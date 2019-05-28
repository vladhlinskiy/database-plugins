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
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.plugin.db.batch.config.DBSpecificSourceConfig;
import io.cdap.plugin.db.batch.source.AbstractDBSource;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Batch source to read from MySQL.
 */
@Plugin(type = "batchsource")
@Name(MysqlConstants.PLUGIN_NAME)
@Description("Reads from a database table(s) using a configurable SQL query." +
  " Outputs one record for each row returned by the query.")
public class MysqlSource extends AbstractDBSource {

  private final MysqlSourceConfig mysqlSourceConfig;

  public MysqlSource(MysqlSourceConfig mysqlSourceConfig) {
    super(mysqlSourceConfig);
    this.mysqlSourceConfig = mysqlSourceConfig;
  }

  @Override
  protected String createConnectionString() {
    return String.format(MysqlConstants.MYSQL_CONNECTION_STRING_FORMAT,
                         mysqlSourceConfig.host, mysqlSourceConfig.port, mysqlSourceConfig.database);
  }

  /**
   * MySQL source config.
   */
  public static class MysqlSourceConfig extends DBSpecificSourceConfig {

    @Name(MysqlConstants.AUTO_RECONNECT)
    @Description("Should the driver try to re-establish stale and/or dead connections")
    @Nullable
    public Boolean autoReconnect;

    @Name(MysqlConstants.USE_COMPRESSION)
    @Description("Select this option for WAN connections")
    @Nullable
    public Boolean useCompression;

    @Override
    public String getConnectionString() {
      return String.format(MysqlConstants.MYSQL_CONNECTION_STRING_FORMAT, host, port, database);
    }

    @Override
    public Map<String, String> getDBSpecificArguments() {
      ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

      if (autoReconnect != null) {
        builder.put(MysqlConstants.AUTO_RECONNECT, String.valueOf(autoReconnect));
      }
      if (useCompression != null) {
        builder.put(MysqlConstants.USE_COMPRESSION, String.valueOf(useCompression));
      }

      return builder.build();
    }
  }
}
