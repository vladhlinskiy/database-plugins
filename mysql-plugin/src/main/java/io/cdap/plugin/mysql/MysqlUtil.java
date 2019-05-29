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

import java.util.Map;

/**
 * MySQL constants and util methods.
 */
public final class MysqlUtil {
  private MysqlUtil() {
    throw new AssertionError("Should not instantiate static utility class.");
  }

  public static final String PLUGIN_NAME = "Mysql";
  public static final String AUTO_RECONNECT = "autoReconnect";
  public static final String USE_COMPRESSION = "useCompression";
  public static final String SESSION_VARIABLES = "sessionVariables";
  public static final String ALLOW_MULTIPLE_QUERIES = "allowMultiQueries";
  public static final String USE_SSL = "useSSL";
  public static final String USE_ANSI_QUOTES = "useAnsiQuotes";
  public static final String NO_SSL_OPTION = "No";
  public static final String REQUIRE_SSL_OPTION = "Require";
  public static final String CLIENT_CERT_KEYSTORE_URL = "clientCertificateKeyStoreUrl";
  public static final String CLIENT_CERT_KEYSTORE_PASSWORD = "clientCertificateKeyStorePassword";
  public static final String TRUST_CERT_KEYSTORE_URL = "trustCertificateKeyStoreUrl";
  public static final String TRUST_CERT_KEYSTORE_PASSWORD = "trustCertificateKeyStorePassword";
  public static final String MYSQL_CONNECTION_STRING_FORMAT = "jdbc:mysql://%s:%s/%s";

  /**
   * Corresponds to the SQL_MODE session variable. Passed to the Connector/J via '{@value #SESSION_VARIABLES}' JDBC
   * URL parameter.
   */
  public static final String SQL_MODE = "sql_mode";

  /**
   * Query to append 'ANSI_QUOTES' sql mode to the current value of SQL_MODE system variable.
   */
  public static final String ANSI_QUOTES_QUERY = "SET SESSION sql_mode = (CONCAT(@@sql_mode , ',', 'ANSI_QUOTES'));";

  /**
   * Composes immutable map of the MySQL specific arguments.
   *
   * @param autoReconnect                     should the driver try to re-establish stale and/or dead connections.
   * @param useCompression                    specifies if compression must be enabled.
   * @param sqlMode                           override the default SQL_MODE session variable used by the server.
   * @param useSSL                            specifies if SSL encryption must be turned on.
   * @param clientCertificateKeyStoreUrl      URL of the client certificate KeyStore.
   * @param clientCertificateKeyStorePassword password for the client certificates KeyStore.
   * @param trustCertificateKeyStoreUrl       URL of the trusted root certificate KeyStore.
   * @param trustCertificateKeyStorePassword  password for the trusted root certificates KeyStore.
   * @return immutable map of the MySQL specific arguments
   */
  public static Map<String, String> composeImmutableDbSpecificArgumentsMap(Boolean autoReconnect,
                                                                           Boolean useCompression,
                                                                           String sqlMode,
                                                                           String useSSL,
                                                                           String clientCertificateKeyStoreUrl,
                                                                           String clientCertificateKeyStorePassword,
                                                                           String trustCertificateKeyStoreUrl,
                                                                           String trustCertificateKeyStorePassword) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

    if (autoReconnect != null) {
      builder.put(MysqlUtil.AUTO_RECONNECT, String.valueOf(autoReconnect));
    }
    if (useCompression != null) {
      builder.put(MysqlUtil.USE_COMPRESSION, String.valueOf(useCompression));
    }
    if (sqlMode != null) {
      builder.put(MysqlUtil.SESSION_VARIABLES, String.format("%s='%s'", MysqlUtil.SQL_MODE, sqlMode));
    }
    if (MysqlUtil.REQUIRE_SSL_OPTION.equals(useSSL)) {
      builder.put(MysqlUtil.USE_SSL, "true");
    } else if (MysqlUtil.NO_SSL_OPTION.equals(useSSL)) {
      builder.put(MysqlUtil.USE_SSL, "false");
    }
    if (clientCertificateKeyStoreUrl != null) {
      builder.put(MysqlUtil.CLIENT_CERT_KEYSTORE_URL, String.valueOf(clientCertificateKeyStoreUrl));
    }
    if (clientCertificateKeyStorePassword != null) {
      builder.put(MysqlUtil.CLIENT_CERT_KEYSTORE_PASSWORD, String.valueOf(clientCertificateKeyStorePassword));
    }
    if (trustCertificateKeyStoreUrl != null) {
      builder.put(MysqlUtil.TRUST_CERT_KEYSTORE_URL, String.valueOf(trustCertificateKeyStoreUrl));
    }
    if (trustCertificateKeyStorePassword != null) {
      builder.put(MysqlUtil.TRUST_CERT_KEYSTORE_PASSWORD, String.valueOf(trustCertificateKeyStorePassword));
    }

    return builder.build();
  }

  /**
   * Creates MySQL specific JDBC connection string.
   *
   * @param host     server host.
   * @param port     server port.
   * @param database database name.
   * @return MySQL specific JDBC connection string
   */
  public static String getConnectionString(String host, Integer port, String database) {
    return String.format(MysqlUtil.MYSQL_CONNECTION_STRING_FORMAT, host, port, database);
  }
}
