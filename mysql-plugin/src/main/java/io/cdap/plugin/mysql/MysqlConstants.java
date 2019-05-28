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

/**
 * MySQL Constants.
 */
public final class MysqlConstants {
  private MysqlConstants() {
    throw new AssertionError("Should not instantiate static utility class.");
  }

  public static final String PLUGIN_NAME = "Mysql";
  public static final String AUTO_RECONNECT = "autoReconnect";
  public static final String USE_COMPRESSION = "useCompression";
  public static final String SESSION_VARIABLES = "sessionVariables";
  public static final String ALLOW_MULTIPLE_QUERIES = "allowMultiQueries";
  public static final String USE_SSL = "useSSL";
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

}
