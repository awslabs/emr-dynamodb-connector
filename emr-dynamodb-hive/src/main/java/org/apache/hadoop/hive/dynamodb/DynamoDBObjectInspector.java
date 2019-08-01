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
import org.apache.hadoop.hive.serde.serdeConstants;
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
      DynamoDBField field = new DynamoDBField(i, columnNames.get(i).toLowerCase(), columnTypes.get(i));
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
      ObjectInspector fieldOI = fieldRef.getFieldObjectInspector();

      /* Get the Hive to DynamoDB type mapper for this column. */
      HiveDynamoDBType ddType = HiveDynamoDBTypeFactory.getTypeObjectFromHiveType(fieldOI);

      /* See if column is of hive map<string,string> type. */
      if (HiveDynamoDBTypeFactory.isHiveDynamoDBItemMapType(fieldOI.getTypeName())) {
        /*
         * User has mapped a DynamoDB item to a single hive column of
         * type map<string,string>.
         */
        HiveDynamoDBItemType ddItemType = (HiveDynamoDBItemType) ddType;

        return ddItemType.buildHiveData(rowData.getItem());

      } else {
        /* User has mapped individual attributes in DynamoDB to hive. */
        String attributeName = hiveDynamoDBColumnMappings.get(fieldRef.getFieldName());
        if (rowData.getItem().containsKey(attributeName)) {
          AttributeValue fieldValue = rowData.getItem().get(attributeName);
          return fieldValue == null ? null : ddType.getHiveData(fieldValue, fieldOI);
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
    return serdeConstants.STRUCT_TYPE_NAME;
  }

  private static class DynamoDBField implements StructField {

    private final int fieldID;
    private final String fieldName;
    private final ObjectInspector objectInspector;
    private final String type;

    DynamoDBField(int fieldID, String fieldName, TypeInfo typeInfo) {
      super();
      this.fieldID = fieldID;
      this.fieldName = fieldName;
      this.objectInspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo);
      this.type = typeInfo.getTypeName();
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
