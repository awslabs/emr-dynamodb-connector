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
import java.util.Map;
import org.apache.hadoop.dynamodb.type.DynamoDBMapType;
import org.apache.hadoop.hive.dynamodb.util.DynamoDBDataParser;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

public class HiveDynamoDBMapType extends DynamoDBMapType implements HiveDynamoDBType {

  @Override
  public AttributeValue getDynamoDBData(Object data, ObjectInspector objectInspector,
      boolean nullSerialization) {
    Map<String, AttributeValue> values =
        DynamoDBDataParser.getMapAttribute(data, objectInspector, nullSerialization);
    return values == null
        ? DynamoDBDataParser.getNullAttribute(nullSerialization)
        : new AttributeValue().withM(values);
  }

  @Override
  public TypeInfo getSupportedHiveType() {
    throw new UnsupportedOperationException(getClass().toString()
        + " does not support this operation.");
  }

  @Override
  public boolean supportsHiveType(TypeInfo typeInfo) {
    try {
      switch (typeInfo.getCategory()) {
        case MAP:
          MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
          if (!mapTypeInfo.getMapKeyTypeInfo().equals(TypeInfoFactory.stringTypeInfo)) {
            return false;
          }

          TypeInfo valueTypeInfo = mapTypeInfo.getMapValueTypeInfo();
          HiveDynamoDBTypeFactory.getTypeObjectFromHiveType(valueTypeInfo);
          return true;

        case STRUCT:
          StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
          for (TypeInfo fieldTypeInfo : structTypeInfo.getAllStructFieldTypeInfos()) {
            HiveDynamoDBTypeFactory.getTypeObjectFromHiveType(fieldTypeInfo);
          }
          return true;

        default:
          return false;
      }
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  @Override
  public Object getHiveData(AttributeValue data, ObjectInspector objectInspector) {
    Map<String, AttributeValue> dataMap = data.getM();
    if (dataMap == null) {
      return null;
    }
    switch (objectInspector.getCategory()) {
      case MAP:
        return DynamoDBDataParser.getMapObject(dataMap, objectInspector);
      case STRUCT:
        return DynamoDBDataParser.getStructObject(dataMap, objectInspector);
      default:
        throw new IllegalArgumentException("Unsupported Hive type: "
            + objectInspector.getTypeName());
    }
  }

}
