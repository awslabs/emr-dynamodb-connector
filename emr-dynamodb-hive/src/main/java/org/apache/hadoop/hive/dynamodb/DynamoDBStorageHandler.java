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

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.google.common.base.Strings;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.dynamodb.DynamoDBClient;
import org.apache.hadoop.dynamodb.DynamoDBConstants;
import org.apache.hadoop.dynamodb.DynamoDBItemWritable;
import org.apache.hadoop.dynamodb.DynamoDBUtil;
import org.apache.hadoop.hive.dynamodb.filter.DynamoDBFilterPushdown;
import org.apache.hadoop.hive.dynamodb.read.HiveDynamoDBInputFormat;
import org.apache.hadoop.hive.dynamodb.type.HiveDynamoDBType;
import org.apache.hadoop.hive.dynamodb.type.HiveDynamoDBTypeFactory;
import org.apache.hadoop.hive.dynamodb.util.HiveDynamoDBUtil;
import org.apache.hadoop.hive.dynamodb.write.HiveDynamoDBOutputFormat;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class DynamoDBStorageHandler
    implements HiveMetaHook, HiveStoragePredicateHandler, HiveStorageHandler {

  private static final Log log = LogFactory.getLog(DynamoDBStorageHandler.class);

  private Configuration conf;

  @Override
  public void commitCreateTable(Table table) throws MetaException {
  }

  @Override
  public void commitDropTable(Table table, boolean deleteData) throws MetaException {
  }

  @Override
  public void preCreateTable(Table table) throws MetaException {
    DynamoDBClient client = createDynamoDBClient(table);
    try {

      boolean isExternal = MetaStoreUtils.isExternalTable(table);

      if (!isExternal) {
        throw new MetaException("Only EXTERNAL tables are supported for DynamoDB.");
      }

      String tableName = HiveDynamoDBUtil.getDynamoDBTableName(table.getParameters()
          .get(DynamoDBConstants.TABLE_NAME), table.getTableName());
      TableDescription tableDescription = client.describeTable(tableName);

      checkTableStatus(tableDescription);
      checkTableSchemaMapping(tableDescription, table);
      checkTableSchemaType(tableDescription, table);
    } finally {
      client.close();
    }
  }

  @Override
  public void preDropTable(Table table) throws MetaException {
  }

  @Override
  public void rollbackCreateTable(Table table) throws MetaException {
  }

  @Override
  public void rollbackDropTable(Table table) throws MetaException {
  }

  @Override
  public DecomposedPredicate decomposePredicate(JobConf jobConf, Deserializer deserializer,
      ExprNodeDesc predicate) {
    if (jobConf.getBoolean(DynamoDBConstants.DYNAMODB_FILTER_PUSHDOWN, true)) {
      return new DynamoDBFilterPushdown()
          .pushPredicate(HiveDynamoDBUtil.extractHiveTypeMapping(jobConf), predicate);
    } else {
      return null;
    }
  }

  @Override
  public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
    DynamoDBClient client =
        new DynamoDBClient(conf, tableDesc.getProperties().getProperty(DynamoDBConstants.REGION));

    try {
      String tableName = HiveDynamoDBUtil.getDynamoDBTableName(tableDesc.getProperties()
          .getProperty(DynamoDBConstants.TABLE_NAME), tableDesc.getTableName());
      TableDescription description = client.describeTable(tableName);
      Double averageItemSize = DynamoDBUtil.calculateAverageItemSize(description);
      log.info("Average item size: " + averageItemSize);

      String endpoint = conf.get(DynamoDBConstants.ENDPOINT);
      if (!Strings.isNullOrEmpty(tableDesc.getProperties().getProperty(DynamoDBConstants
          .ENDPOINT))) {
        endpoint = tableDesc.getProperties().getProperty(DynamoDBConstants.ENDPOINT);
      }

      if (!Strings.isNullOrEmpty(endpoint)) {
        jobProperties.put(DynamoDBConstants.ENDPOINT, endpoint);
      }

      if (!Strings.isNullOrEmpty(tableDesc.getProperties().getProperty(DynamoDBConstants.REGION))) {
        jobProperties.put(DynamoDBConstants.REGION,
            tableDesc.getProperties().getProperty(DynamoDBConstants.REGION));
      }

      jobProperties.put(DynamoDBConstants.OUTPUT_TABLE_NAME, tableName);
      jobProperties.put(DynamoDBConstants.INPUT_TABLE_NAME, tableName);
      jobProperties.put(DynamoDBConstants.TABLE_NAME, tableName);

      Map<String, String> hiveToDynamoDBSchemaMapping = HiveDynamoDBUtil
          .getHiveToDynamoDBSchemaMapping(tableDesc.getProperties().getProperty(DynamoDBConstants
              .DYNAMODB_COLUMN_MAPPING));

      // Column map can be null if only full backup is being used
      if (hiveToDynamoDBSchemaMapping != null) {
        jobProperties.put(DynamoDBConstants.DYNAMODB_COLUMN_MAPPING, HiveDynamoDBUtil
            .toJsonString(hiveToDynamoDBSchemaMapping));
      }

      if (tableDesc.getProperties().getProperty(DynamoDBConstants.THROUGHPUT_READ_PERCENT)
          != null) {
        jobProperties.put(DynamoDBConstants.THROUGHPUT_READ_PERCENT, tableDesc.getProperties()
            .getProperty(DynamoDBConstants.THROUGHPUT_READ_PERCENT));
      }

      if (tableDesc.getProperties().getProperty(DynamoDBConstants.THROUGHPUT_WRITE_PERCENT)
          != null) {
        jobProperties.put(DynamoDBConstants.THROUGHPUT_WRITE_PERCENT, tableDesc.getProperties()
            .getProperty(DynamoDBConstants.THROUGHPUT_WRITE_PERCENT));
      }

      if (description.getBillingModeSummary() == null
          || description.getBillingModeSummary().getBillingMode()
          .equals(DynamoDBConstants.BILLING_MODE_PROVISIONED)) {
        useExplicitThroughputIfRequired(jobProperties, tableDesc);
      } else {
        // If not specified at the table level, set default value
        jobProperties.put(DynamoDBConstants.READ_THROUGHPUT, tableDesc.getProperties()
            .getProperty(DynamoDBConstants.READ_THROUGHPUT,
                DynamoDBConstants.DEFAULT_CAPACITY_FOR_ON_DEMAND.toString()));
        jobProperties.put(DynamoDBConstants.WRITE_THROUGHPUT, tableDesc.getProperties()
            .getProperty(DynamoDBConstants.WRITE_THROUGHPUT,
                DynamoDBConstants.DEFAULT_CAPACITY_FOR_ON_DEMAND.toString()));
      }

      jobProperties.put(DynamoDBConstants.ITEM_COUNT, description.getItemCount().toString());
      jobProperties.put(DynamoDBConstants.TABLE_SIZE_BYTES, description.getTableSizeBytes()
          .toString());
      jobProperties.put(DynamoDBConstants.AVG_ITEM_SIZE, averageItemSize.toString());

      log.info("Average item size: " + averageItemSize);
      log.info("Item count: " + description.getItemCount());
      log.info("Table size: " + description.getTableSizeBytes());
      log.info("Read throughput: " + jobProperties.get(DynamoDBConstants.READ_THROUGHPUT));
      log.info("Write throughput: " + jobProperties.get(DynamoDBConstants.WRITE_THROUGHPUT));

    } finally {
      client.close();
    }
  }

  private void useExplicitThroughputIfRequired(Map<String, String> jobProperties, TableDesc tableDesc) {
    String userRequiredReadThroughput = tableDesc.getProperties().getProperty(DynamoDBConstants.READ_THROUGHPUT);
    if (userRequiredReadThroughput != null) {
      jobProperties.put(DynamoDBConstants.READ_THROUGHPUT, userRequiredReadThroughput);
    }

    String userRequiredWriteThroughput = tableDesc.getProperties().getProperty(DynamoDBConstants.WRITE_THROUGHPUT);
    if (userRequiredWriteThroughput != null) {
      jobProperties.put(DynamoDBConstants.WRITE_THROUGHPUT, userRequiredWriteThroughput);
    }
  }

  @Override
  public Class<? extends InputFormat<Text, DynamoDBItemWritable>> getInputFormatClass() {
    return HiveDynamoDBInputFormat.class;
  }

  @Override
  public HiveMetaHook getMetaHook() {
    return this;
  }

  @Override
  public Class<? extends OutputFormat<Text, DynamoDBItemWritable>> getOutputFormatClass() {
    return HiveDynamoDBOutputFormat.class;
  }

  @Override
  public Class<? extends AbstractSerDe> getSerDeClass() {
    return DynamoDBSerDe.class;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public HiveAuthorizationProvider getAuthorizationProvider() throws HiveException {
    return new DefaultHiveAuthorizationProvider();
  }

  @Override
  public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
    configureTableJobProperties(tableDesc, jobProperties);
  }

  @Override
  public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
    configureTableJobProperties(tableDesc, jobProperties);
  }

  protected boolean isHiveDynamoDBItemMapType(String type) {
    return HiveDynamoDBTypeFactory.isHiveDynamoDBItemMapType(type);
  }

  protected HiveDynamoDBType getTypeObjectFromHiveType(String type) {
    return HiveDynamoDBTypeFactory.getTypeObjectFromHiveType(type);
  }

  void checkTableSchemaMapping(TableDescription tableDescription, Table table) throws
      MetaException {
    String mapping = table.getParameters().get(DynamoDBConstants.DYNAMODB_COLUMN_MAPPING);
    Map<String, String> columnMapping = HiveDynamoDBUtil.getHiveToDynamoDBSchemaMapping(mapping);

    List<FieldSchema> tableSchema = table.getSd().getCols();
    for (FieldSchema fieldSchema : tableSchema) {
      String fieldSchemaName = fieldSchema.getName().toLowerCase();

      if (isHiveDynamoDBItemMapType(fieldSchema.getType())) {
        // We don't need column mapping as this column contains full
        // DynamoDB row
        columnMapping.remove(fieldSchemaName);
        continue;
      }

      if (columnMapping.containsKey(fieldSchemaName)) {
        if (columnMapping.get(fieldSchemaName).isEmpty()) {
          throw new MetaException("Invalid column mapping for column: " + fieldSchemaName);
        }
        columnMapping.remove(fieldSchemaName);
      } else {
        throw new MetaException("Could not find column mapping for column: " + fieldSchemaName);
      }
    }

    if (!columnMapping.isEmpty()) {
      StringBuilder exMessage = new StringBuilder("Could not find column(s) for column mapping(s): ");
      String delim = "";
      for (String extraMapping : columnMapping.keySet()) {
        exMessage.append(delim + extraMapping + ":" + columnMapping.get(extraMapping));
        delim = ",";
      }
      throw new MetaException(exMessage.toString());
    }
  }

  public void checkTableSchemaType(TableDescription tableDescription, Table table) throws
      MetaException {
    List<FieldSchema> tableSchema = table.getSd().getCols();

    for (FieldSchema fieldSchema : tableSchema) {
      for (AttributeDefinition definition : tableDescription.getAttributeDefinitions()) {
        validateKeySchema(definition.getAttributeName(), definition.getAttributeType(),
            fieldSchema);
      }

      // Check for each field type
      if (getTypeObjectFromHiveType(fieldSchema.getType()) == null) {
        throw new MetaException("The hive type " + fieldSchema.getType() + " is not supported in "
            + "DynamoDB");
      }
    }
  }

  private DynamoDBClient createDynamoDBClient(Table table) {
    String region = table.getParameters().get(DynamoDBConstants.REGION);
    return new DynamoDBClient(conf, region);
  }

  private void validateKeySchema(String attributeName, String attributeType, FieldSchema
      fieldSchema) throws MetaException {
    if (fieldSchema.getName().equalsIgnoreCase(attributeName)) {
      HiveDynamoDBType ddType = getTypeObjectFromHiveType(fieldSchema
          .getType());
      if ((ddType == null) || (ddType.equals(HiveDynamoDBTypeFactory.DYNAMODB_ITEM_TYPE))
          || (!ddType.getDynamoDBType().equals(attributeType))) {
        throw new MetaException("The key element " + fieldSchema.getName() + " does not match "
            + "type. DynamoDB Type: " + attributeType + " Hive type: " + fieldSchema.getType());
      }
    }
  }

  private void checkTableStatus(TableDescription tableDescription) throws MetaException {
    String status = tableDescription.getTableStatus();

    if ("CREATING".equals(status) || "DELETING".equals(status)) {
      throw new MetaException("Table " + tableDescription.getTableName() + " is in state "
          + status);
    }
  }

  @Override
  public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {
    Map<String, String> jobProperties = new HashMap<>();
    configureTableJobProperties(tableDesc, jobProperties);
    for (Entry<String, String> entry : jobProperties.entrySet()) {
      jobConf.set(entry.getKey(), entry.getValue());
    }
  }
}
