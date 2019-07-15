package org.apache.hadoop.hive.dynamodb;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.List;
import java.util.Map;

public class DynamoDBListSerDe extends DynamoDBSerDe {

  @Override
  protected DynamoDBObjectInspector newObjectInspector(List<String> columnNames, List<TypeInfo> columnTypes,
                                                       Map<String,String> columnMappings) {
    return new DynamoDBListObjectInspector(columnNames, columnTypes, columnMappings);
  }
}
