package org.apache.hadoop.hive.dynamodb.type;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import org.apache.hadoop.dynamodb.type.DynamoDBBooleanType;
import org.apache.hadoop.hive.dynamodb.util.DynamoDBDataParser;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

public class HiveDynamoDBBooleanType extends DynamoDBBooleanType implements HiveDynamoDBType {

  @Override
  public AttributeValue getDynamoDBData(Object data, ObjectInspector objectInspector) {
    Boolean value = DynamoDBDataParser.getBoolean(data, objectInspector);
    return value == null ? null : new AttributeValue().withBOOL(value);
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
    return data.getBOOL();
  }
}
