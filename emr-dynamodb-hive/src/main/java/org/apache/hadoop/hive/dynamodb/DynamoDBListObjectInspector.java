package org.apache.hadoop.hive.dynamodb;

import org.apache.hadoop.hive.dynamodb.type.HiveDynamoDBListTypeFactory;
import org.apache.hadoop.hive.dynamodb.type.HiveDynamoDBType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.List;
import java.util.Map;

public class DynamoDBListObjectInspector extends DynamoDBObjectInspector {

  public DynamoDBListObjectInspector(List<String> columnNames, List<TypeInfo> columnTypes,
                                     Map<String, String> columnMappings) {
    super(columnNames, columnTypes, columnMappings);
  }

  @Override
  protected HiveDynamoDBType getTypeObjectFromHiveType(ObjectInspector objectInspector) {
    return HiveDynamoDBListTypeFactory.getTypeObjectFromHiveType(objectInspector);
  }
}
