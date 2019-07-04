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

package io.cdap.plugin.db;

import org.junit.Assert;

import java.util.Arrays;

/**
 * Test util methods for custom assertions.
 */
public final class CustomAssertions {

  /**
   * The maximum delta between expected and actual floating point number for which both numbers are still considered
   * equal.
   */
  public static final double DELTA = 0.01;

  private CustomAssertions() {
    throw new AssertionError("Should not instantiate static utility class.");
  }

  /**
   * Reuses {@link Assert#assertEquals(Object, Object)}. Added to prevent 'Ambiguous method call' issue.
   * Asserts that two objects are equal. If they are not, an {@link AssertionError} without a message is thrown.
   * If expected and actual are null, they are considered equal.
   *
   * @param expected expected value
   * @param actual   the value to check against expected
   */
  public static void assertObjectEquals(Object expected, Object actual) {
    Assert.assertEquals(expected, actual);
  }

  /**
   * Reuses {@link Assert#assertArrayEquals(byte[], byte[])}.
   * Asserts that values of two byte arrays are equal for the length of the expected one, since it's common that actual
   * arrays are larger. If values are not equal size of actual array is less than size of expected,
   * an {@link AssertionError} is thrown.
   *
   * @param expected byte array with expected values.
   * @param actual   byte array with actual values
   */
  public static void assertBytesEquals(byte[] expected, byte[] actual) {
    // handle the case when 'null' values passed
    if (expected == null || actual == null) {
      Assert.assertEquals(expected, actual);
    } else {
      Assert.assertTrue(actual.length >= expected.length);
      Assert.assertArrayEquals(expected, Arrays.copyOf(actual, expected.length));
    }
  }

  /**
   * Reuses {@link Assert#assertEquals(double, double, double)} with default {@link CustomAssertions#DELTA}.
   * Added to prevent repetitive casts to 'double' and specifying delta. Asserts that two doubles are equal to within
   * the delta. If they are not, an AssertionError is thrown with the given message.
   *
   * @param expected expected value
   * @param actual   the value to check against expected
   */
  public static void assertNumericEquals(double expected, double actual) {
    Assert.assertEquals(expected, actual, 0.01);
  }

  /**
   * Reuses {@link Assert#assertEquals(double, double, double)} with default {@link CustomAssertions#DELTA}.
   * Added to prevent repetitive casts to 'float' and specifying delta. Asserts that two doubles are equal to within
   * the delta. If they are not, an AssertionError is thrown with the given message.
   *
   * @param expected expected value
   * @param actual   the value to check against expected
   */
  public static void assertNumericEquals(float expected, float actual) {
    Assert.assertEquals(expected, actual, 0.01);
  }
}
