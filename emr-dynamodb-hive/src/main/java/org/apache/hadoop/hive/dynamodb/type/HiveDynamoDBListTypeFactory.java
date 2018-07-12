/**
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

package org.apache.hadoop.hive.dynamodb.type;

import org.apache.hadoop.hive.dynamodb.DerivedHiveTypeConstants;

import java.util.HashMap;
import java.util.Map;

public class HiveDynamoDBListTypeFactory extends HiveDynamoDBTypeFactory {

  private static final HiveDynamoDBType NUMBER_LIST_TYPE = new HiveDynamoDBListType();
  private static final HiveDynamoDBType STRING_LIST_TYPE = new HiveDynamoDBListType();
  private static final HiveDynamoDBType LIST_ITEM_TYPE = new HiveDynamoDBListType();
  private static final HiveDynamoDBType MAP_TYPE = new HiveDynamoDBMapType();

  private static final Map<String, HiveDynamoDBType> HIVE_TYPE_MAP = new HashMap<>();
  static {
    HIVE_TYPE_MAP.put(DerivedHiveTypeConstants.BIGINT_ARRAY_LIST_TYPE_NAME, NUMBER_LIST_TYPE);
    HIVE_TYPE_MAP.put(DerivedHiveTypeConstants.DOUBLE_ARRAY_LIST_TYPE_NAME, NUMBER_LIST_TYPE);
    HIVE_TYPE_MAP.put(DerivedHiveTypeConstants.STRING_ARRAY_LIST_TYPE_NAME, STRING_LIST_TYPE);
    HIVE_TYPE_MAP.put(DerivedHiveTypeConstants.STRING_BIGINT_MAP_TYPE_NAME, MAP_TYPE);
    HIVE_TYPE_MAP.put(DerivedHiveTypeConstants.LIST_STRING_BIG_INT_MAP_TYPE_NAME, LIST_ITEM_TYPE);
    HIVE_TYPE_MAP.put(DerivedHiveTypeConstants.LIST_STRING_BIG_DOUBLE_MAP_TYPE_NAME, LIST_ITEM_TYPE);
    HIVE_TYPE_MAP.put(DerivedHiveTypeConstants.LIST_ITEM_MAP_TYPE_NAME, LIST_ITEM_TYPE);
  }

  public static HiveDynamoDBType getTypeObjectFromHiveType(String hiveType) {
    HiveDynamoDBType aType = HIVE_TYPE_MAP.get(hiveType.toLowerCase());
    if (aType != null) {
      return aType;
    }
    return HiveDynamoDBTypeFactory
      .getTypeObjectFromHiveType(hiveType.toLowerCase());
  }
}
