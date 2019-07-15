package org.apache.hadoop.hive.dynamodb.type;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import org.apache.hadoop.dynamodb.type.DynamoDBBooleanType;
import org.apache.hadoop.hive.dynamodb.util.DynamoDBDataParser;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public class HiveDynamoDBBooleanType extends DynamoDBBooleanType implements HiveDynamoDBType {

  @Override
  public AttributeValue getDynamoDBData(Object data, ObjectInspector objectInspector) {
    Boolean value = DynamoDBDataParser.getBoolean(data, objectInspector);
    return value == null ? null : new AttributeValue().withBOOL(value);
  }

  @Override
  public Object getHiveData(AttributeValue data, String hiveType) {
    return data.getBOOL();
  }
}
