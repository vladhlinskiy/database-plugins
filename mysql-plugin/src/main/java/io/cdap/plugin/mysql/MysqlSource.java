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
import io.cdap.plugin.db.batch.config.DBSpecificSourceConfig;
import io.cdap.plugin.db.batch.source.AbstractDBSource;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Batch source to read from MySQL.
 */
@Plugin(type = "batchsource")
@Name(MysqlUtil.PLUGIN_NAME)
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
    return String.format(MysqlUtil.MYSQL_CONNECTION_STRING_FORMAT,
                         mysqlSourceConfig.host, mysqlSourceConfig.port, mysqlSourceConfig.database);
  }

  /**
   * MySQL source config.
   */
  public static class MysqlSourceConfig extends DBSpecificSourceConfig {

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
