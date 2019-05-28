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
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.plugin.db.batch.action.AbstractDBAction;
import io.cdap.plugin.db.batch.config.DBSpecificQueryConfig;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Action that runs MySQL command.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name(MysqlConstants.PLUGIN_NAME)
@Description("Action that runs a MySQL command")
public class MysqlAction extends AbstractDBAction {

  private final MysqlActionConfig mysqlActionConfig;

  public MysqlAction(MysqlActionConfig mysqlActionConfig) {
    super(mysqlActionConfig, false);
    this.mysqlActionConfig = mysqlActionConfig;
  }

  /**
   * Mysql Action Config.
   */
  public static class MysqlActionConfig extends DBSpecificQueryConfig {

    @Name(MysqlConstants.AUTO_RECONNECT)
    @Description("Should the driver try to re-establish stale and/or dead connections")
    @Nullable
    public Boolean autoReconnect;

    @Name(MysqlConstants.USE_COMPRESSION)
    @Description("Select this option for WAN connections")
    @Nullable
    public Boolean useCompression;

    @Name(MysqlConstants.SQL_MODE)
    @Description("Override the default SQL_MODE session variable used by the server")
    @Nullable
    public Boolean sqlMode;

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
      if (sqlMode != null) {
        builder.put(MysqlConstants.SESSION_VARIABLES, String.format("%s='%s'", MysqlConstants.SQL_MODE, sqlMode));
      }

      return builder.build();
    }
  }
}
