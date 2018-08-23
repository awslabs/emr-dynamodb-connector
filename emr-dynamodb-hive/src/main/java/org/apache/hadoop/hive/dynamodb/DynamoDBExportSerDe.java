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

import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.dynamodb.DynamoDBConstants;
import org.apache.hadoop.dynamodb.DynamoDBItemWritable;
import org.apache.hadoop.hive.dynamodb.shims.SerDeParametersShim;
import org.apache.hadoop.hive.dynamodb.shims.ShimsLoader;
import org.apache.hadoop.hive.dynamodb.type.HiveDynamoDBItemType;
import org.apache.hadoop.hive.dynamodb.type.HiveDynamoDBTypeFactory;
import org.apache.hadoop.hive.dynamodb.util.HiveDynamoDBUtil;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * This class is used to read the DynanmoDB backup format and allow querying individual columns from
 * the schemaless backup.
 */
public class DynamoDBExportSerDe extends AbstractSerDe {

  private static final Log log = LogFactory.getLog(DynamoDBExportSerDe.class);

  private DynamoDBObjectInspector objectInspector;
  private Map<String, String> columnMappings;
  private SerDeParametersShim serdeParams;

  @Override
  public void initialize(Configuration conf, Properties tbl) throws SerDeException {
    serdeParams = ShimsLoader.getHiveShims()
        .getSerDeParametersShim(conf, tbl, getClass().getName());
    String specifiedColumnMapping = tbl.getProperty(DynamoDBConstants.DYNAMODB_COLUMN_MAPPING);

    for (TypeInfo type : serdeParams.getColumnTypes()) {
      if (HiveDynamoDBTypeFactory.getTypeObjectFromHiveType(type.getTypeName()) == null) {
        throw new SerDeException("Unsupported type: " + type.getTypeName());
      }
    }

    log.info("Provided column mapping: " + specifiedColumnMapping);
    columnMappings = Maps.newHashMap();
    if (!Strings.isNullOrEmpty(specifiedColumnMapping)) {
      columnMappings = HiveDynamoDBUtil.getHiveToDynamoDBSchemaMapping(specifiedColumnMapping);
    }
    addDefaultColumnMappings(serdeParams.getColumnNames());

    log.info("Final column mapping: " + columnMappings);
    objectInspector = new DynamoDBObjectInspector(serdeParams.getColumnNames(), serdeParams
        .getColumnTypes(), columnMappings);
  }

  @Override
  public Object deserialize(Writable inputData) throws SerDeException {
    if (inputData == null) {
      return null;
    }
    if (inputData instanceof Text) {
      String data = inputData.toString();
      if (Strings.isNullOrEmpty(data)) {
        return null;
      }
      String collectionSplitCharacter = byteToString(1);

      List<String> fields = Arrays.asList(data.split(collectionSplitCharacter));

      if (fields.isEmpty()) {
        return null;
      }

      Map<String, AttributeValue> item = Maps.newHashMap();
      String mapSplitCharacter = byteToString(2);

      for (String field : fields) {
        if (Strings.isNullOrEmpty(field)) {
          throw new SerDeException("Empty fields in data: " + data);
        }
        List<String> values = Arrays.asList(field.split(mapSplitCharacter));
        if (values.size() != 2) {
          throw new SerDeException("Invalid record with map value: " + values);
        }
        String dynamoDBAttributeName = values.get(0);
        String dynamoDBAttributeValue = values.get(1);

        /* Deserialize the AttributeValue string */
        AttributeValue deserializedAttributeValue = new HiveDynamoDBItemType()
            .deserializeAttributeValue(dynamoDBAttributeValue);

        item.put(dynamoDBAttributeName, deserializedAttributeValue);
      }

      DynamoDBItemWritable dynamoDBItem = new DynamoDBItemWritable(item);
      return dynamoDBItem;
    } else {
      throw new SerDeException(getClass().toString() + ": expects Text object!");
    }
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return objectInspector;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return Text.class;
  }

  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
    throw new UnsupportedOperationException("The SerDe supports only reading data");
  }

  @Override
  public SerDeStats getSerDeStats() {
    // no support for statistics
    return null;
  }

  private String byteToString(int separatorIndex) {
    char[] charArray = new char[1];
    charArray[0] = (char) serdeParams.getSeparators()[separatorIndex];
    return new String(charArray);
  }

  private void addDefaultColumnMappings(List<String> hiveColumnNames) {
    for (String hiveColumnName : hiveColumnNames) {
      if (!columnMappings.containsKey(hiveColumnName)) {
        columnMappings.put(hiveColumnName, hiveColumnName);
      }
    }
  }

}
