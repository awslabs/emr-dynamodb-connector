/**
 * Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file
 * except in compliance with the License. A copy of the License is located at
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "LICENSE.TXT" file accompanying this file. This file is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under the License.
 */

package org.apache.hadoop.hive.dynamodb.shims;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import org.apache.hive.common.util.HiveVersionInfo;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@PrepareForTest(HiveVersionInfo.class)
@PowerMockIgnore("javax.management.*")
@RunWith(PowerMockRunner.class)
public class ShimsLoaderTest {

  private static final String HIVE_1_VERSION = "1.0.0";

  private static final String HIVE_1_2_VERSION = "1.2.0";

  private static final String HIVE_2_VERSION = "2.1.0";

  @Before
  public void setup() throws ClassNotFoundException {
    mockStatic(HiveVersionInfo.class);
  }

  @Test
  public void hive2ShimsClassSupportsCorrectVersion() {
    assertTrue(DynamoDbHive2Shims.supportsVersion(HIVE_2_VERSION));
  }

  @Test
  public void hive1Dot2ShimsClassSupportsCorrectVersion() {
    assertTrue(DynamoDbHive1Dot2Shims.supportsVersion(HIVE_1_2_VERSION));
  }

  @Test
  public void hive1ShimsClassSupportsCorrectVersion() {
    assertTrue(DynamoDbHive1Shims.supportsVersion(HIVE_1_VERSION));
  }

  @Test
  public void returnsCorrectShimsImplementationForHive2() {
    assertGetsCorrectShimsClassForVersion(DynamoDbHive2Shims.class, HIVE_2_VERSION);
  }

  @Test
  public void returnsCorrectShimsImplementationForHive1Dot2() {
    assertGetsCorrectShimsClassForVersion(DynamoDbHive1Dot2Shims.class, HIVE_1_2_VERSION);
  }

  @Test
  public void returnsCorrectShimsImplementationForHive1() {
    assertGetsCorrectShimsClassForVersion(DynamoDbHive1Shims.class, HIVE_1_VERSION);
  }

  @Test(expected = RuntimeException.class)
  public void throwsWhenSupportingClassDoesNotExist() {
    when(HiveVersionInfo.getShortVersion()).thenReturn("this.is.not.a.real.hive.version");
    ShimsLoader.clearShimClass();
    ShimsLoader.getHiveShims();
  }

  private void assertGetsCorrectShimsClassForVersion(Class expectedClass, String version) {
    when(HiveVersionInfo.getShortVersion()).thenReturn(version);
    DynamoDbHiveShims shims = ShimsLoader.getHiveShims();
    assertTrue(expectedClass.isInstance(shims));
    ShimsLoader.clearShimClass();
  }

}
