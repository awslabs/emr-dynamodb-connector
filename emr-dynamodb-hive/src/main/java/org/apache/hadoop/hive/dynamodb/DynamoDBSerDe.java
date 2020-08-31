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
import com.amazonaws.services.dynamodbv2.model.BillingModeSummary;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.dynamodb.DynamoDBClient;
import org.apache.hadoop.dynamodb.DynamoDBConstants;
import org.apache.hadoop.dynamodb.DynamoDBItemWritable;
import org.apache.hadoop.hive.dynamodb.shims.SerDeParametersShim;
import org.apache.hadoop.hive.dynamodb.shims.ShimsLoader;
import org.apache.hadoop.hive.dynamodb.type.HiveDynamoDBItemType;
import org.apache.hadoop.hive.dynamodb.type.HiveDynamoDBType;
import org.apache.hadoop.hive.dynamodb.type.HiveDynamoDBTypeFactory;
import org.apache.hadoop.hive.dynamodb.util.DynamoDBDataParser;
import org.apache.hadoop.hive.dynamodb.util.HiveDynamoDBUtil;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class DynamoDBSerDe extends AbstractSerDe {

  private static final Log log = LogFactory.getLog(DynamoDBSerDe.class);
  // Hive initializes SerDe multiple times and we need to make sure that the
  // user is warned exactly once
  private static boolean warningPrinted;

  protected SerDeParametersShim serdeParams;
  private DynamoDBObjectInspector objectInspector;
  private Map<String, String> columnMappings;
  private Map<String, HiveDynamoDBType> typeMappings;
  private boolean nullSerialization;
  private List<String> columnNames;

  @Override
  public void initialize(Configuration conf, Properties tbl) throws SerDeException {
    serdeParams = ShimsLoader.getHiveShims().getSerDeParametersShim(conf, tbl,
        getClass().getName());
    columnNames = serdeParams.getColumnNames();
    List<TypeInfo> columnTypes = serdeParams.getColumnTypes();

    columnMappings = HiveDynamoDBUtil.getHiveToDynamoDBColumnMapping(tbl);
    log.info("Column mapping: " + columnMappings);
    typeMappings = HiveDynamoDBUtil.getHiveToDynamoDBTypeMapping(columnNames, columnTypes, tbl);
    log.info("Type mapping: " + typeMappings);
    nullSerialization = HiveDynamoDBUtil.getHiveToDynamoDBNullSerialization(tbl);
    log.info("Null serialization: " + nullSerialization);

    objectInspector =
        new DynamoDBObjectInspector(columnNames, columnTypes, columnMappings, typeMappings);

    verifyDynamoDBWriteThroughput(conf, tbl);
  }

  @Override
  public Object deserialize(Writable dataMap) throws SerDeException {
    if (!(dataMap instanceof DynamoDBItemWritable)) {
      throw new SerDeException("Expected DynamoDBMapWritable data type, got "
          + dataMap.getClass().getName() + " data: " + dataMap.toString());
    }
    return dataMap;
  }

  @Override
  public ObjectInspector getObjectInspector() {
    return objectInspector;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return Text.class;
  }

  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
    // Prepare the field ObjectInspectors
    StructObjectInspector soi = (StructObjectInspector) objInspector;
    List<? extends StructField> fields = soi.getAllStructFieldRefs();
    List<Object> rowData = soi.getStructFieldsDataAsList(obj);
    Map<String, AttributeValue> item = Maps.newHashMap();

    validateData(fields, rowData);

    for (int i = 0; i < fields.size(); i++) {
      StructField field = fields.get(i);
      Object data = rowData.get(i);
      String columnName = columnNames.get(i);
      ObjectInspector fieldOI = field.getFieldObjectInspector();

      // Get the Hive to DynamoDB mapper
      HiveDynamoDBType ddType = typeMappings.get(columnName);

      // Check if this column maps a DynamoDB item.
      if (HiveDynamoDBTypeFactory.isHiveDynamoDBItemMapType(ddType)) {
        HiveDynamoDBItemType ddItemType = (HiveDynamoDBItemType) ddType;
        Map<String, AttributeValue> backupItem = ddItemType.parseDynamoDBData(data, fieldOI);

        // We give higher priority to attributes directly mapped to
        // columns. So we do not update the value of an attribute if
        // it already exists. This can happen in case of partial schemas
        // when there is a full backup column and attribute mapped
        // columns.
        for (Map.Entry<String, AttributeValue> entry : backupItem.entrySet()) {
          if (!columnMappings.containsValue(entry.getKey())) {
            item.put(entry.getKey(), entry.getValue());
          }
        }
      } else {
        // User has mapped individual attribute in DynamoDB to
        // corresponding Hive columns.
        AttributeValue attributeValue = data == null
            ? DynamoDBDataParser.getNullAttribute(nullSerialization)
            : ddType.getDynamoDBData(data, fieldOI, nullSerialization);

        if (attributeValue != null) {
          item.put(columnMappings.get(columnName), attributeValue);
        }
      }
    }

    return new DynamoDBItemWritable(item);
  }

  @Override
  public SerDeStats getSerDeStats() {
    // no support for statistics
    return null;
  }

  private void validateData(List<? extends StructField> fields, List<Object> rowData) {
    if (rowData == null) {
      throw new RuntimeException("No data found in the row.");
    }

    if (fields == null) {
      throw new RuntimeException("Field information not available");
    }

    if (rowData.size() != fields.size()) {
      throw new RuntimeException("Number of data objects do not match number of columns. Data: "
          + rowData);
    }
  }

  private void verifyDynamoDBWriteThroughput(Configuration conf, Properties tbl) {
    if (conf == null) {
      // In a lot of places Hive creates a SerDe with null conf.
      // In this case it is not possible to get the cluster status.
      return;
    }

    if (warningPrinted) {
      return;
    }

    String dynamoDBTableName = tbl.getProperty(DynamoDBConstants.TABLE_NAME);
    // Hive uses partition metadata to initialize serde's. We may not need
    // to verify write throughput at column level. dynamoDBTableName is null
    // in this case, don't proceed and return
    if (dynamoDBTableName == null) {
      return;
    }

    log.info("Table Properties:" + tbl);
    DynamoDBClient client = new DynamoDBClient(conf, tbl.getProperty(DynamoDBConstants.REGION));
    long writesPerSecond = client.describeTable(dynamoDBTableName).getProvisionedThroughput()
        .getWriteCapacityUnits();
    long maxMapTasks;

    try {
      JobClient jc = new JobClient(new JobConf(conf));
      maxMapTasks = jc.getClusterStatus().getMaxMapTasks();
    } catch (IOException e) {
      throw new RuntimeException("Could not get cluster capacity.", e);
    }

    BillingModeSummary billingModeSummary =
        client.describeTable(dynamoDBTableName).getBillingModeSummary();
    if (maxMapTasks > writesPerSecond
        && (billingModeSummary == null
        || billingModeSummary.getBillingMode().equals(
            DynamoDBConstants.BILLING_MODE_PROVISIONED))) {
      String message = "WARNING: Configured write throughput of the dynamodb table "
          + dynamoDBTableName + " is less than the cluster map capacity." + " ClusterMapCapacity: "
          + maxMapTasks + " WriteThroughput: " + writesPerSecond + "\nWARNING: Writes to this "
          + "table might result in a write outage on the table.";
      LogHelper console = SessionState.getConsole();
      if (console != null) {
        console.printInfo(message);
      }
      log.warn(message);
      warningPrinted = true;
    }
  }
}
