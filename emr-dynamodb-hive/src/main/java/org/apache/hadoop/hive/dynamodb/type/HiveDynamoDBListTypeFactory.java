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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.serde.serdeConstants;

public class HiveDynamoDBListTypeFactory extends HiveDynamoDBTypeFactory {

  private static final HiveDynamoDBType LIST_TYPE = new HiveDynamoDBListType();

  public static HiveDynamoDBType getTypeObjectFromHiveType(String hiveType) {
    if (StringUtils.startsWithIgnoreCase(hiveType, serdeConstants.LIST_TYPE_NAME)) {
      return LIST_TYPE;
    }
    return HiveDynamoDBTypeFactory.getTypeObjectFromHiveType(hiveType.toLowerCase());
  }
}
