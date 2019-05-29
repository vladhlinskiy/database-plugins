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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.batch.PostAction;
import io.cdap.plugin.db.batch.action.AbstractQueryAction;
import io.cdap.plugin.db.batch.config.DBSpecificQueryActionConfig;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Represents MySQL post action.
 */
@Plugin(type = PostAction.PLUGIN_TYPE)
@Name(MysqlUtil.PLUGIN_NAME)
@Description("Runs a MySQL query after a pipeline run.")
public class MysqlPostAction extends AbstractQueryAction {

  private final MysqlQueryActionConfig mysqlQueryActionConfig;

  public MysqlPostAction(MysqlQueryActionConfig mysqlQueryActionConfig) {
    super(mysqlQueryActionConfig, false);
    this.mysqlQueryActionConfig = mysqlQueryActionConfig;
  }

  /**
   * MySQL post action mysqlQueryActionConfig.
   */
  public static class MysqlQueryActionConfig extends DBSpecificQueryActionConfig {

    @Name(MysqlUtil.AUTO_RECONNECT)
    @Description("Should the driver try to re-establish stale and/or dead connections")
    @Nullable
    public Boolean autoReconnect;

    @Name(MysqlUtil.USE_COMPRESSION)
    @Description("Select this option for WAN connections")
    @Nullable
    public Boolean useCompression;

    @Name(MysqlUtil.SQL_MODE)
    @Description("Override the default SQL_MODE session variable used by the server")
    @Nullable
    public String sqlMode;

    @Name(MysqlUtil.USE_SSL)
    @Description("Turns on SSL encryption. Connection will fail if SSL is not available")
    @Nullable
    public String useSSL;

    @Name(MysqlUtil.USE_ANSI_QUOTES)
    @Description("Treats \" as an identifier quote character and not as a string quote character")
    @Nullable
    public Boolean useAnsiQuotes;

    @Name(MysqlUtil.CLIENT_CERT_KEYSTORE_URL)
    @Description("URL to the client certificate KeyStore (if not specified, use defaults)")
    @Nullable
    public String clientCertificateKeyStoreUrl;

    @Name(MysqlUtil.CLIENT_CERT_KEYSTORE_PASSWORD)
    @Description("Password for the client certificates KeyStore")
    @Nullable
    public String clientCertificateKeyStorePassword;

    @Name(MysqlUtil.TRUST_CERT_KEYSTORE_URL)
    @Description("URL to the trusted root certificate KeyStore (if not specified, use defaults)")
    @Nullable
    public String trustCertificateKeyStoreUrl;

    @Name(MysqlUtil.TRUST_CERT_KEYSTORE_PASSWORD)
    @Description("Password for the trusted root certificates KeyStore")
    @Nullable
    public String trustCertificateKeyStorePassword;

    @Override
    public String getConnectionString() {
      return MysqlUtil.getConnectionString(host, port, database);
    }

    @Override
    public Map<String, String> getDBSpecificArguments() {
      return MysqlUtil.composeImmutableDbSpecificArgumentsMap(autoReconnect, useCompression, sqlMode, useSSL,
                                                              clientCertificateKeyStoreUrl,
                                                              clientCertificateKeyStorePassword,
                                                              trustCertificateKeyStoreUrl,
                                                              trustCertificateKeyStorePassword);
    }

    @Override
    public String getInitQueriesString() {
      if (useAnsiQuotes != null && useAnsiQuotes) {
        return MysqlUtil.ANSI_QUOTES_QUERY;
      }
      return "";
    }
  }
}
