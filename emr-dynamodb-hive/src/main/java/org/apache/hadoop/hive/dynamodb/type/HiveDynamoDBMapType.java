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
import org.apache.hadoop.dynamodb.type.DynamoDBMapType;
import org.apache.hadoop.hive.dynamodb.util.DynamoDBDataParser;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.util.Map;

public class HiveDynamoDBMapType extends DynamoDBMapType implements HiveDynamoDBType {

  @Override
  public AttributeValue getDynamoDBData(Object data, ObjectInspector objectInspector) {
    Map<String, AttributeValue> values = DynamoDBDataParser.getMapAttribute(data, objectInspector);
    return values == null ? null : new AttributeValue().withM(values);
  }

  @Override
  public Object getHiveData(AttributeValue data, ObjectInspector objectInspector) {
    return data.getM() == null ? null : DynamoDBDataParser.getMapObject(data.getM(), objectInspector);
  }

}
