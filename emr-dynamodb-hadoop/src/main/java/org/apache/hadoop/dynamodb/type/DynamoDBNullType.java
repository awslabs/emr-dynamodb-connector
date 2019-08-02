package org.apache.hadoop.dynamodb.type;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import org.apache.hadoop.dynamodb.key.DynamoDBKey;

public class DynamoDBNullType implements DynamoDBType {
  @Override
  public AttributeValue getAttributeValue(String... values) {
    return new AttributeValue().withNULL(true);
  }

  @Override
  public String getDynamoDBType() {
    return DynamoDBTypeConstants.NULL;
  }

  @Override
  public DynamoDBKey getKey(String key) {
    throw new RuntimeException("Unexpected type " + getDynamoDBType());
  }
}
