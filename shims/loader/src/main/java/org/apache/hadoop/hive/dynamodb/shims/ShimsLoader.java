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

import com.google.common.annotations.VisibleForTesting;

import org.apache.hive.common.util.HiveVersionInfo;

public final class ShimsLoader {

  private static DynamoDbHiveShims hiveShims;

  public static synchronized DynamoDbHiveShims getHiveShims() {
    if (hiveShims == null) {
      hiveShims = loadHiveShims();
    }
    return hiveShims;
  }

  /**
   * Due to Java 7 not allowing static methods/method implementations inside an interface, we're
   * not really able to abstract the shim version support logic in any meaningful way. Once the
   * language level for the project is able to be bumped to Java 8, this can definitely be
   * refactored.
   */
  private static DynamoDbHiveShims loadHiveShims() {
    String hiveVersion = HiveVersionInfo.getShortVersion();
    if (DynamoDbHive1Shims.supportsVersion(hiveVersion)) {
      try {
        return DynamoDbHive1Shims.class.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        throw new RuntimeException("unable to get instance of Hive 1.x shim class");
      }
    } else if (DynamoDbHive1Dot2Shims.supportsVersion(hiveVersion)) {
      try {
        return DynamoDbHive1Dot2Shims.class.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        throw new RuntimeException("unable to get instance of Hive 1.2.x shim class");
      }
    } else if (DynamoDbHive2Shims.supportsVersion(hiveVersion)) {
      try {
        return DynamoDbHive2Shims.class.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        throw new RuntimeException("unable to get instance of Hive 2.x shim class");
      }
    } else {
      throw new RuntimeException("Shim class for Hive version " + hiveVersion + " does not exist");
    }
  }

  @VisibleForTesting
  static synchronized void clearShimClass() {
    hiveShims = null;
  }
}
