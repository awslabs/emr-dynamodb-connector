package org.apache.hadoop.hive.dynamodb.type;

import org.apache.hadoop.dynamodb.type.DynamoDBBooleanType;
import org.apache.hadoop.hive.dynamodb.util.DynamoDBDataParser;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class HiveDynamoDBBooleanType extends DynamoDBBooleanType implements HiveDynamoDBType {

  @Override
  public AttributeValue getDynamoDBData(Object data, ObjectInspector objectInspector,
      boolean nullSerialization) {
    Boolean value = DynamoDBDataParser.getBoolean(data, objectInspector);
    return value == null
        ? DynamoDBDataParser.getNullAttribute(nullSerialization)
        : AttributeValue.fromBool(value);
  }

  @Override
  public TypeInfo getSupportedHiveType() {
    return TypeInfoFactory.booleanTypeInfo;
  }

  @Override
  public boolean supportsHiveType(TypeInfo typeInfo) {
    return typeInfo.equals(getSupportedHiveType());
  }

  @Override
  public Object getHiveData(AttributeValue data, ObjectInspector objectInspector) {
    return data.bool();
  }
}
