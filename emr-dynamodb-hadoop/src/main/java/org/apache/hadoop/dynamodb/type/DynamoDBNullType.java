package org.apache.hadoop.dynamodb.type;

import org.apache.hadoop.dynamodb.key.DynamoDBKey;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class DynamoDBNullType implements DynamoDBType {
  @Override
  public AttributeValue getAttributeValue(String... values) {
    return AttributeValue.fromNul(true);
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
