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

package org.apache.hadoop.hive.dynamodb;

import org.apache.hadoop.hive.dynamodb.type.HiveDynamoDBListTypeFactory;
import org.apache.hadoop.hive.dynamodb.type.HiveDynamoDBType;
import org.apache.hadoop.hive.serde2.AbstractSerDe;

public class DynamoDBListStorageHandler extends DynamoDBStorageHandler {
  @Override
  protected boolean isHiveDynamoDBItemMapType(String type){
    return HiveDynamoDBListTypeFactory.isHiveDynamoDBItemMapType(type);
  }

  @Override
  protected HiveDynamoDBType getTypeObjectFromHiveType(String type){
    return HiveDynamoDBListTypeFactory.getTypeObjectFromHiveType(type);
  }

  @Override
  public Class<? extends AbstractSerDe> getSerDeClass() {
    return DynamoDBListSerDe.class;
  }
}
