package org.apache.hadoop.hive.dynamodb;

import org.apache.hadoop.hive.dynamodb.type.HiveDynamoDBListTypeFactory;
import org.apache.hadoop.hive.dynamodb.type.HiveDynamoDBType;

public class DynamoDBListSerDe extends DynamoDBSerDe {
  @Override
  protected HiveDynamoDBType getTypeObjectFromHiveType(String type) {
    return HiveDynamoDBListTypeFactory.getTypeObjectFromHiveType(type);
  }
}
