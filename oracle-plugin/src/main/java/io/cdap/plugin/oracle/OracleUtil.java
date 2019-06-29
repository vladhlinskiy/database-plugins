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

import io.cdap.cdap.api.data.schema.Schema;

import java.util.Collections;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Oracle util methods.
 */
public final class OracleUtil {
  private OracleUtil() {
    throw new AssertionError("Should not instantiate static utility class.");
  }

  /**
   * Determines if the given schema is of decimal logical type.
   * If the given {@link Schema} is a {@link Schema.Type#UNION}, this method will search for
   * the {@link Schema.LogicalType#DECIMAL} recursively and return 'true' if {@link Schema.LogicalType#DECIMAL} is
   * encountered.
   *
   * @param schema the {@link Schema} for searching the {@link Schema.LogicalType#DECIMAL}
   * @return 'true' if {@link Schema.LogicalType#DECIMAL} is encountered.
   */
  public static boolean isDecimalLogicalType(Schema schema) {
    return getLogicalTypeSchema(schema, Collections.singleton(Schema.LogicalType.DECIMAL)) != null;
  }

  /**
   * Gets the {@link Schema} with logical type.
   * If the given {@link Schema} is a {@link Schema.Type#UNION}, this method will find
   * the schema recursively and return the schema for the first {@link Schema.LogicalType} it encountered.
   *
   * @param schema       the {@link Schema} for finding the {@link Schema.LogicalType}
   * @param allowedTypes acceptable logical types
   * @return the {@link Schema} for logical type or {@code null} if no {@link Schema.LogicalType} was found
   */
  @Nullable
  public static Schema getLogicalTypeSchema(Schema schema, Set<Schema.LogicalType> allowedTypes) {
    Schema.LogicalType logicalType = schema.getLogicalType();

    if (logicalType != null && allowedTypes.contains(logicalType)) {
      return schema;
    }

    if (schema.getType() == Schema.Type.UNION) {
      for (Schema unionSchema : schema.getUnionSchemas()) {
        Schema logicalTypeSchema = getLogicalTypeSchema(unionSchema, allowedTypes);

        if (logicalTypeSchema != null) {
          return logicalTypeSchema;
        }
      }
    }
    return null;
  }
}
