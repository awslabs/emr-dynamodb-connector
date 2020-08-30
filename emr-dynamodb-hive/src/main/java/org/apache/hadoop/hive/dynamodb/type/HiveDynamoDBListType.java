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
import java.util.List;
import org.apache.hadoop.dynamodb.type.DynamoDBListType;
import org.apache.hadoop.hive.dynamodb.util.DynamoDBDataParser;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

public class HiveDynamoDBListType extends DynamoDBListType implements HiveDynamoDBType {

  @Override
  public AttributeValue getDynamoDBData(Object data, ObjectInspector objectInspector,
      boolean nullSerialization) {
    List<AttributeValue> values =
        DynamoDBDataParser.getListAttribute(data, objectInspector, nullSerialization);
    return values == null
        ? DynamoDBDataParser.getNullAttribute(nullSerialization)
        : new AttributeValue().withL(values);
  }

  @Override
  public TypeInfo getSupportedHiveType() {
    throw new UnsupportedOperationException(getClass().toString()
        + " does not support this operation.");
  }

  @Override
  public boolean supportsHiveType(TypeInfo typeInfo) {
    if (typeInfo.getCategory() != ObjectInspector.Category.LIST) {
      return false;
    }

    TypeInfo elementTypeInfo = ((ListTypeInfo) typeInfo).getListElementTypeInfo();
    try {
      HiveDynamoDBTypeFactory.getTypeObjectFromHiveType(elementTypeInfo);
    } catch (IllegalArgumentException e) {
      return false;
    }
    return true;
  }

  @Override
  public Object getHiveData(AttributeValue data, ObjectInspector objectInspector) {
    return data.getL() == null ? null
        : DynamoDBDataParser.getListObject(data.getL(), objectInspector);
  }

}
