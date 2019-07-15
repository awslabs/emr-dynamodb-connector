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
import org.apache.hadoop.dynamodb.type.DynamoDBListType;
import org.apache.hadoop.hive.dynamodb.DerivedHiveTypeConstants;
import org.apache.hadoop.hive.dynamodb.util.DynamoDBDataParser;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.util.ArrayList;
import java.util.List;

public class HiveDynamoDBListType extends DynamoDBListType implements HiveDynamoDBType {

  @Override
  public AttributeValue getDynamoDBData(Object data, ObjectInspector objectInspector) {
    List<AttributeValue> values = DynamoDBDataParser.getListAttribute(data, objectInspector);
    return values == null ? null : new AttributeValue().withL(values);
  }

  @Override
  public Object getHiveData(AttributeValue data, String hiveType) {
    if (data == null) {
      return null;
    }

    String elementType = DerivedHiveTypeConstants.getArrayElementType(hiveType);
    HiveDynamoDBType ddType = HiveDynamoDBListTypeFactory.getTypeObjectFromHiveType(elementType);
    List<Object> values = new ArrayList<>();
    for (AttributeValue av : data.getL()) {
      values.add(ddType.getHiveData(av, elementType));
    }
    return values;
  }

}
