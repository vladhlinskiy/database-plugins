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

import com.google.common.base.Charsets;
import io.cdap.cdap.api.data.schema.Schema;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import javax.sql.rowset.serial.SerialBlob;

/**
 * test
 */
public class TestRecord {

  enum Column {
    ID("INT NOT NULL", Schema.of(Schema.Type.INT)),
    NAME("VARCHAR(40) NOT NULL",  Schema.of(Schema.Type.STRING)),
    SCORE("DOUBLE", Schema.of(Schema.Type.DOUBLE)),
    GRADUATED("BOOLEAN", Schema.of(Schema.Type.BOOLEAN)),
    NOT_IMPORTED("VARCHAR(30)", Schema.of(Schema.Type.STRING)),
    TINY("TINYINT", Schema.of(Schema.Type.INT)),
    SMALL("SMALLINT", Schema.of(Schema.Type.INT)),
    BIG("BIGINT", Schema.of(Schema.Type.LONG)),
    MEDIUMINT_COL("MEDIUMINT", Schema.of(Schema.Type.INT)),
    FLOAT_COL("FLOAT", Schema.of(Schema.Type.FLOAT)),
    REAL_COL("REAL", Schema.of(Schema.Type.DOUBLE)),
    NUMERIC_COL("NUMERIC(10, 6)", Schema.decimalOf(10, 6)),
    DECIMAL_COL("DECIMAL(10, 6)", Schema.decimalOf(10, 6)),
    BIT_COL("BIT", Schema.of(Schema.Type.BOOLEAN)),
    DATE_COL("DATE", Schema.of(Schema.LogicalType.DATE)),
    TIME_COL("TIME", Schema.of(Schema.LogicalType.TIME_MICROS)),
    TIMESTAMP_COL("TIMESTAMP(3)", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
    DATETIME_COL("DATETIME(3)", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
    YEAR_COL("YEAR", Schema.of(Schema.LogicalType.DATE)),
    TEXT_COL("TEXT", Schema.of(Schema.Type.STRING)),
    TINYTEXT_COL("TINYTEXT", Schema.of(Schema.Type.STRING)),
    MEDIUMTEXT_COL("MEDIUMTEXT", Schema.of(Schema.Type.STRING)),
    LONGTEXT_COL("LONGTEXT", Schema.of(Schema.Type.STRING)),
    CHAR_COL("CHAR(100)", Schema.of(Schema.Type.STRING)),
    BINARY_COL("BINARY(100)", Schema.of(Schema.Type.BYTES)),
    VARBINARY_COL("VARBINARY(20)", Schema.of(Schema.Type.BYTES)),
    TINYBLOB_COL("TINYBLOB", Schema.of(Schema.Type.BYTES)),
    BLOB_COL("BLOB(100)", Schema.of(Schema.Type.BYTES)),
    MEDIUMBLOB_COL("MEDIUMBLOB", Schema.of(Schema.Type.BYTES)),
    LONGBLOB_COL("LONGBLOB", Schema.of(Schema.Type.BYTES)),
    ENUM_COL("ENUM('First', 'Second', 'Third')", Schema.enumWith("First", "Second", "Third")),
    SET_COL("SET('a', 'b', 'c', 'd')", Schema.of(Schema.Type.STRING));

    final String sqlType;
    final Schema schema;

    Column(String sqlType, Schema schema) {
      this.sqlType = sqlType;
      this.schema = schema;
    }

    public String sqlType() {
      return sqlType;
    }

    public Schema.Field field() {
      return Schema.Field.of(name(), schema);
    }

    public int index() {
      return ordinal() + 1;
    }
  }

  private Map<Column, Object> data = new HashMap<>();

  private TestRecord(Map<Column, Object> data) {
    this.data = data;
  }

  public static TestRecord newRecord() {
    return new TestRecord(new HashMap<>());
  }

  public static TestRecord copyOf(TestRecord source) {
    return new TestRecord(new HashMap<>(source.data));
  }

  public TestRecord set(Column column, Object value) {
    data.put(column, value);
    return this;
  }

  public SerialBlob getBlobOf(Column column) {
    try {
      String value = (String) data.get(column);
      if (value == null) {
        return null;
      }
      return new SerialBlob(value.getBytes(Charsets.UTF_8));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public Object get(Column column) {
    return data.get(column);
  }
}
