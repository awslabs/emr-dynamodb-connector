/**
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

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import org.apache.hadoop.hive.dynamodb.util.DynamoDBDataParser;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import static org.apache.hadoop.hive.dynamodb.type.HiveDynamoDBTypeUtil.parseMap;

public class HiveDynamoDBMapType  extends HiveDynamoDBItemType {

  private final DynamoDBDataParser parser = new DynamoDBDataParser();

  @Override
  public AttributeValue getDynamoDBData(Object data, ObjectInspector objectInspector) {
    return parseMap(parser.getMap(data, objectInspector));
  }

  @Override
  public Object getHiveData(AttributeValue data, String hiveType) {
    if (data == null) {
      return null;
    }
    return data.getM();
  }

}
