/**
 * Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file
 * except in compliance with the License. A copy of the License is located at
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "LICENSE.TXT" file accompanying this file. This file is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under the License.
 */

package org.apache.hadoop.hive.dynamodb;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import org.apache.hadoop.dynamodb.DynamoDBItemWritable;
import org.apache.hadoop.hive.dynamodb.type.HiveDynamoDBItemType;
import org.apache.hadoop.hive.dynamodb.type.HiveDynamoDBType;
import org.apache.hadoop.hive.dynamodb.type.HiveDynamoDBTypeFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DynamoDBObjectInspector extends StructObjectInspector {

  private final List<String> columnNames;
  private final Map<String, String> hiveDynamoDBColumnMappings;
  private List<StructField> structFields;
  private Map<String, DynamoDBField> columnNameStructFieldMap;

  public DynamoDBObjectInspector(List<String> columnNames, List<TypeInfo> columnTypes,
      Map<String, String> columnMappings) {
    this.columnNames = columnNames;
    this.hiveDynamoDBColumnMappings = columnMappings;

    if (columnNames == null) {
      throw new RuntimeException("Null columns names passed");
    }

    if (columnTypes == null) {
      throw new RuntimeException("Null columns types passed");
    }

    structFields = new ArrayList<>();
    columnNameStructFieldMap = new HashMap<>();

    // Constructing struct field list for each column
    for (int i = 0; i < columnNames.size(); i++) {
      DynamoDBField field = new DynamoDBField(i, columnNames.get(i).toLowerCase(), TypeInfoUtils
          .getStandardJavaObjectInspectorFromTypeInfo(columnTypes.get(i)), columnTypes.get(i)
          .getTypeName());
      structFields.add(field);
      columnNameStructFieldMap.put(columnNames.get(i), field);
    }
  }

  @Override
  public List<? extends StructField> getAllStructFieldRefs() {
    return structFields;
  }

  @Override
  public Object getStructFieldData(Object data, StructField fieldRef) {
    DynamoDBItemWritable rowData = (DynamoDBItemWritable) data;
    return getColumnData(fieldRef, rowData);
  }

  private Object getColumnData(StructField fieldRef, DynamoDBItemWritable rowData) {
    try {
      /* Get the hive data type for this column. */
      String hiveType = ((DynamoDBField) fieldRef).getType();

      /* Get the Hive to DynamoDB type mapper for this column. */
      HiveDynamoDBType ddType = HiveDynamoDBTypeFactory.getTypeObjectFromHiveType(hiveType);

      if (ddType == null) {
        throw new RuntimeException("Unsupported hive type " + hiveType);
      }

      /* See if column is of hive map<string,string> type. */
      if (ddType instanceof HiveDynamoDBItemType) {
        /*
         * User has mapped a DynamoDB item to a single hive column of
         * type map<string,string>.
         */
        HiveDynamoDBItemType ddItemType = (HiveDynamoDBItemType) ddType;

        return ddItemType.buildHiveData(rowData.getItem());

      } else {
        /* User has mapped individual attributes in DyanamoDB to hive. */
        if (rowData.getItem()
            .containsKey(hiveDynamoDBColumnMappings.get(fieldRef.getFieldName()))) {
          AttributeValue fieldValue = rowData.getItem()
              .get(hiveDynamoDBColumnMappings.get(fieldRef.getFieldName()));
          return ddType.getHiveData(fieldValue, hiveType);
        } else {
          return null;
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Exception while processing record: " + rowData.toString(), e);
    }
  }

  @Override
  public StructField getStructFieldRef(String columnName) {
    return columnNameStructFieldMap.get(columnName);
  }

  @Override
  public List<Object> getStructFieldsDataAsList(Object data) {
    DynamoDBItemWritable rowData = (DynamoDBItemWritable) data;
    List<Object> columnData = new ArrayList<>();
    for (String columnName : columnNames) {
      columnData.add(getColumnData(columnNameStructFieldMap.get(columnName), rowData));
    }

    return columnData;
  }

  @Override
  public Category getCategory() {
    return Category.STRUCT;
  }

  @Override
  public String getTypeName() {
    return "struct";
  }

  private static class DynamoDBField implements StructField {

    private final int fieldID;
    private final String fieldName;
    private final ObjectInspector objectInspector;
    private final String type;

    public DynamoDBField(int fieldID, String fieldName, ObjectInspector objectInspector, String
        fieldType) {
      super();
      this.fieldID = fieldID;
      this.fieldName = fieldName;
      this.objectInspector = objectInspector;
      this.type = fieldType;
    }

    @Override
    public String getFieldName() {
      return fieldName;
    }

    @Override
    public ObjectInspector getFieldObjectInspector() {
      return objectInspector;
    }

    public String getType() {
      return type;
    }

    @Override
    public String getFieldComment() {
      return null;
    }

    @Override
    public int getFieldID() {
      return fieldID;
    }
  }

}
