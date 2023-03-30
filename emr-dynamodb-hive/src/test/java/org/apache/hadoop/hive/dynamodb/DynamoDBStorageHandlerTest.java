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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.dynamodb.DynamoDBClient;
import org.apache.hadoop.dynamodb.DynamoDBConstants;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.mockito.Mockito;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.BillingModeSummary;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputDescription;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;

public class DynamoDBStorageHandlerTest {

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();
  protected DynamoDBStorageHandler storageHandler;

  @Before
  public void setup() {
    storageHandler = new DynamoDBStorageHandler();
    storageHandler.setConf(new Configuration());
  }

  @Test
  public void testCheckTableSchemaMappingMissingColumn() throws MetaException {
    TableDescription description = getHashRangeTable();

    Table table = new Table();
    Map<String, String> parameters = Maps.newHashMap();
    parameters.put(DynamoDBConstants.DYNAMODB_COLUMN_MAPPING, "col1:dynamo_col1$,hashMap:hashMap");
    table.setParameters(parameters);
    StorageDescriptor sd = new StorageDescriptor();
    List<FieldSchema> cols = Lists.newArrayList();
    cols.add(new FieldSchema("col1", "string", ""));
    cols.add(new FieldSchema("col2", "tinyint", ""));
    cols.add(new FieldSchema("col3", "string", ""));
    cols.add(new FieldSchema("hashMap", "map<string,string>", ""));
    sd.setCols(cols);
    table.setSd(sd);

    exceptionRule.expect(MetaException.class);
    exceptionRule.expectMessage("Could not find column mapping for column: col2");
    storageHandler.checkTableSchemaMapping(description, table);
  }

  @Test
  public void testCheckTableSchemaMappingMissingColumnMapping() throws MetaException {
    TableDescription description = getHashRangeTable();

    Table table = new Table();
    Map<String, String> parameters = Maps.newHashMap();
    parameters.put(DynamoDBConstants.DYNAMODB_COLUMN_MAPPING, "col1:dynamo_col1$," +
	    "col2:dynamo_col2#,hashKey:hashKey,hashMap:hashMap");
    table.setParameters(parameters);
    StorageDescriptor sd = new StorageDescriptor();
    List<FieldSchema> cols = Lists.newArrayList();
    cols.add(new FieldSchema("col1", "string", ""));
    cols.add(new FieldSchema("hashMap", "map<string,string>", ""));
    sd.setCols(cols);
    table.setSd(sd);

    exceptionRule.expect(MetaException.class);
    exceptionRule.expectMessage("Could not find column(s) for column mapping(s): ");
    exceptionRule.expectMessage("col2:dynamo_col2#");
    exceptionRule.expectMessage("hashkey:hashKey");
    storageHandler.checkTableSchemaMapping(description, table);
  }

  @Test
  public void testCheckTableSchemaMappingValid() throws MetaException {
    TableDescription description = getHashRangeTable();

    Table table = new Table();
    Map<String, String> parameters = Maps.newHashMap();
    parameters.put(DynamoDBConstants.DYNAMODB_COLUMN_MAPPING, "col1:dynamo_col1$," +
        "col2:dynamo_col2#,hashKey:hashKey");
    table.setParameters(parameters);
    StorageDescriptor sd = new StorageDescriptor();
    List<FieldSchema> cols = Lists.newArrayList();
    cols.add(new FieldSchema("col1", "string", ""));
    cols.add(new FieldSchema("col2", "bigint", ""));
    cols.add(new FieldSchema("hashKey", "string", ""));
    sd.setCols(cols);
    table.setSd(sd);
    storageHandler.checkTableSchemaMapping(description, table);
  }

  @Test
  public void testCheckTableSchemaTypeInvalidType() throws MetaException {
    TableDescription description = getHashRangeTable();

    Table table = new Table();
    Map<String, String> parameters = Maps.newHashMap();
    parameters.put(DynamoDBConstants.DYNAMODB_COLUMN_MAPPING, "col1:dynamo_col1$," +
        "col2:dynamo_col2#,hashKey:hashKey");
    table.setParameters(parameters);
    StorageDescriptor sd = new StorageDescriptor();
    List<FieldSchema> cols = Lists.newArrayList();
    cols.add(new FieldSchema("col1", "string", ""));
    cols.add(new FieldSchema("col2", "tinyint", ""));
    cols.add(new FieldSchema("hashKey", "string", ""));
    sd.setCols(cols);
    table.setSd(sd);

    exceptionRule.expect(MetaException.class);
    exceptionRule.expectMessage("The hive type tinyint is not supported in DynamoDB");
    storageHandler.checkTableSchemaType(description, table);
  }

  @Test
  public void testCheckTableSchemaTypeInvalidHashKeyType() throws MetaException {
    TableDescription description = getHashRangeTable();

    Table table = new Table();
    Map<String, String> parameters = Maps.newHashMap();
    parameters.put(DynamoDBConstants.DYNAMODB_COLUMN_MAPPING, "col1:dynamo_col1$," +
        "col2:dynamo_col2#,hashKey:hashKey");
    table.setParameters(parameters);
    StorageDescriptor sd = new StorageDescriptor();
    List<FieldSchema> cols = Lists.newArrayList();
    cols.add(new FieldSchema("col1", "string", ""));
    cols.add(new FieldSchema("col2", "bigint", ""));
    cols.add(new FieldSchema("hashKey", "map<string,string>", ""));
    sd.setCols(cols);
    table.setSd(sd);

    exceptionRule.expect(MetaException.class);
    exceptionRule.expectMessage("The key element hashKey does not match type. DynamoDB Type: S " +
        "Hive type: " + "map<string,string>");
    storageHandler.checkTableSchemaType(description, table);
  }

  @Test
  public void testCheckTableSchemaTypeValid() throws MetaException {
    TableDescription description = getHashRangeTable();

    Table table = new Table();
    Map<String, String> parameters = Maps.newHashMap();
    parameters.put(DynamoDBConstants.DYNAMODB_COLUMN_MAPPING, "col1:dynamo_col1$," +
        "col2:dynamo_col2#,hashKey:hashKey");
    table.setParameters(parameters);
    StorageDescriptor sd = new StorageDescriptor();
    List<FieldSchema> cols = Lists.newArrayList();
    cols.add(new FieldSchema("col1", "string", ""));
    cols.add(new FieldSchema("col2", "bigint", ""));
    cols.add(new FieldSchema("hashKey", "string", ""));
    sd.setCols(cols);
    table.setSd(sd);
    // This check is expected to pass for the given input
    storageHandler.checkTableSchemaType(description, table);
  }

  @Test
  public void testCheckListTableSchemaTypeValid() throws MetaException {
    TableDescription description = getHashRangeTable();

    Table table = new Table();
    Map<String, String> parameters = Maps.newHashMap();
    parameters.put(DynamoDBConstants.DYNAMODB_COLUMN_MAPPING, "col1:dynamo_col1$," +
            "col2:dynamo_col2#,col3:dynamo_col3#,col4:dynamo_col4#,col5:dynamo_col5#," +
            "col6:dynamo_col6#,col7:dynamo_col7#,hashKey:hashKey");
    table.setParameters(parameters);
    StorageDescriptor sd = new StorageDescriptor();
    List<FieldSchema> cols = Lists.newArrayList();
    cols.add(new FieldSchema("col1", "map<string,bigint>", ""));
    cols.add(new FieldSchema("col2", "array<map<string,bigint>>", ""));
    cols.add(new FieldSchema("col3", "array<map<string,double>>", ""));
    cols.add(new FieldSchema("col4", "array<map<string,string>>", ""));
    cols.add(new FieldSchema("col5", "array<bigint>", ""));
    cols.add(new FieldSchema("col6", "array<double>", ""));
    cols.add(new FieldSchema("col7", "array<string>", ""));
    cols.add(new FieldSchema("hashKey", "string", ""));
    sd.setCols(cols);
    table.setSd(sd);
    // This check is expected to pass for the given input
    storageHandler.checkTableSchemaType(description, table);
  }

  @Test
  public void testCheckStructTableSchemaTypeInvalid() throws MetaException {
    TableDescription description = getHashRangeTable();

    Table table = new Table();
    Map<String, String> parameters = Maps.newHashMap();
    parameters.put(DynamoDBConstants.DYNAMODB_COLUMN_MAPPING, "col1:dynamo_col1$," +
        "col2:dynamo_col2#,hashKey:hashKey");
    table.setParameters(parameters);
    StorageDescriptor sd = new StorageDescriptor();
    List<FieldSchema> cols = Lists.newArrayList();
    cols.add(new FieldSchema("col1", "struct<bignum:bigint,smallnum:tinyint>", ""));
    cols.add(new FieldSchema("col2", "array<map<string,bigint>>", ""));
    cols.add(new FieldSchema("hashKey", "string", ""));
    sd.setCols(cols);
    table.setSd(sd);

    exceptionRule.expect(MetaException.class);
    exceptionRule.expectMessage("The hive type struct<bignum:bigint,smallnum:tinyint> is not " +
        "supported in DynamoDB");
    storageHandler.checkTableSchemaType(description, table);
  }

  @Test
  public void testCheckStructTableSchemaTypeValid() throws MetaException {
    TableDescription description = getHashRangeTable();

    Table table = new Table();
    Map<String, String> parameters = Maps.newHashMap();
    parameters.put(DynamoDBConstants.DYNAMODB_COLUMN_MAPPING, "col1:dynamo_col1$," +
        "col2:dynamo_col2#,hashKey:hashKey");
    table.setParameters(parameters);
    StorageDescriptor sd = new StorageDescriptor();
    List<FieldSchema> cols = Lists.newArrayList();
    cols.add(new FieldSchema("col1", "struct<numarray:array<bigint>,num:double>", ""));
    cols.add(new FieldSchema("col2", "array<struct<numarray:array<bigint>,num:double>>", ""));
    cols.add(new FieldSchema("hashKey", "string", ""));
    sd.setCols(cols);
    table.setSd(sd);
    // This check is expected to pass for the given input
    storageHandler.checkTableSchemaType(description, table);
  }

  @Test
  public void testCheckTableSchemaTypeMappingInvalid() throws MetaException {
    TableDescription description = getHashRangeTable();

    Table table = new Table();
    Map<String, String> parameters = Maps.newHashMap();
    parameters.put(DynamoDBConstants.DYNAMODB_COLUMN_MAPPING, "col1:dynamo_col1$," +
            "col2:dynamo_col2#,hashKey:hashKey");
    parameters.put(DynamoDBConstants.DYNAMODB_TYPE_MAPPING, "col2:NS");
    table.setParameters(parameters);
    StorageDescriptor sd = new StorageDescriptor();
    List<FieldSchema> cols = Lists.newArrayList();
    cols.add(new FieldSchema("col1", "string", ""));
    cols.add(new FieldSchema("col2", "bigint", ""));
    cols.add(new FieldSchema("hashKey", "string", ""));
    sd.setCols(cols);
    table.setSd(sd);

    exceptionRule.expect(MetaException.class);
    exceptionRule.expectMessage("The DynamoDB type NS does not support Hive type bigint");
    storageHandler.checkTableSchemaType(description, table);
  }

  @Test
  public void testCheckTableSchemaTypeMappingValid() throws MetaException {
    TableDescription description = getHashRangeTable();

    Table table = new Table();
    Map<String, String> parameters = Maps.newHashMap();
    parameters.put(DynamoDBConstants.DYNAMODB_COLUMN_MAPPING, "col1:dynamo_col1$," +
            "col2:dynamo_col2#,hashKey:hashKey");
    parameters.put(DynamoDBConstants.DYNAMODB_TYPE_MAPPING, "col2:NS");
    table.setParameters(parameters);
    StorageDescriptor sd = new StorageDescriptor();
    List<FieldSchema> cols = Lists.newArrayList();
    cols.add(new FieldSchema("col1", "string", ""));
    cols.add(new FieldSchema("col2", "array<bigint>", ""));
    cols.add(new FieldSchema("hashKey", "string", ""));
    sd.setCols(cols);
    table.setSd(sd);

    // This check is expected to pass for the given input
    storageHandler.checkTableSchemaType(description, table);
  }

  @Test
  public void testCheckTableSchemaNullSerializationValid() throws MetaException {
    TableDescription description = getHashRangeTable();

    Table table = new Table();
    Map<String, String> parameters = Maps.newHashMap();
    parameters.put(DynamoDBConstants.DYNAMODB_COLUMN_MAPPING, "col1:dynamo_col1$," +
        "col2:dynamo_col2#,hashKey:hashKey");
    parameters.put(DynamoDBConstants.DYNAMODB_NULL_SERIALIZATION, "true");
    table.setParameters(parameters);
    StorageDescriptor sd = new StorageDescriptor();
    List<FieldSchema> cols = Lists.newArrayList();
    cols.add(new FieldSchema("col1", "string", ""));
    cols.add(new FieldSchema("col2", "array<bigint>", ""));
    cols.add(new FieldSchema("hashKey", "string", ""));
    sd.setCols(cols);
    table.setSd(sd);

    // This check is expected to pass for the given input
    storageHandler.checkTableSchemaType(description, table);
  }

  @Test
  public void testReadWriteThroughputConfiguredProvisionedTable() {
    DynamoDBClient mockDynamoClient = Mockito.mock(DynamoDBClient.class);
    TableDescription mockDescribeTableResponse =
        getHashRangeTableByBillingMode(BillingMode.PROVISIONED);
    doReturn(mockDescribeTableResponse).when(mockDynamoClient).describeTable(any());

    DynamoDBStorageHandler spyDynamoStorageHandler = Mockito.spy(storageHandler);
    doReturn(mockDynamoClient).when(spyDynamoStorageHandler).createDynamoDBClient((TableDesc) any());

    TableDesc testTableDesc = new TableDesc();
    Properties properties = new Properties();
    properties.setProperty(DynamoDBConstants.REGION, "us-east-1");
    properties.setProperty(DynamoDBConstants.TABLE_NAME, "test-table");
    testTableDesc.setProperties(properties);

    // User has not specified explicit throughput settings, fetch from dynamo table
    Map<String, String> jobProperties1 = new HashMap<>();
    spyDynamoStorageHandler.configureTableJobProperties(testTableDesc, jobProperties1);
    assertEquals(jobProperties1.get(DynamoDBConstants.READ_THROUGHPUT), "400");
    assertEquals(jobProperties1.get(DynamoDBConstants.WRITE_THROUGHPUT), "100");
    // Provisioned tables should be configured to dynamically calculate throughput during tasks
    assertTrue(Boolean.parseBoolean(jobProperties1.get(DynamoDBConstants.READ_THROUGHPUT_AUTOSCALING)));
    assertTrue(Boolean.parseBoolean(jobProperties1.get(DynamoDBConstants.WRITE_THROUGHPUT_AUTOSCALING)));

    // User has specified throughput settings, override what is in table
    properties.setProperty(DynamoDBConstants.READ_THROUGHPUT, "50");
    properties.setProperty(DynamoDBConstants.WRITE_THROUGHPUT, "50");

    Map<String, String> jobProperties2 = new HashMap<>();
    spyDynamoStorageHandler.configureTableJobProperties(testTableDesc, jobProperties2);
    assertEquals(jobProperties2.get(DynamoDBConstants.READ_THROUGHPUT), "50");
    assertEquals(jobProperties2.get(DynamoDBConstants.WRITE_THROUGHPUT), "50");
    // Provisioned tables with user specified settings should not dynamically configure throughput
    assertFalse(Boolean.parseBoolean(jobProperties2.get(DynamoDBConstants.READ_THROUGHPUT_AUTOSCALING)));
    assertFalse(Boolean.parseBoolean(jobProperties2.get(DynamoDBConstants.WRITE_THROUGHPUT_AUTOSCALING)));
  }

  @Test
  public void testReadWriteThroughputConfiguredOnDemandTable() {
    DynamoDBClient mockDynamoClient = Mockito.mock(DynamoDBClient.class);
    TableDescription mockDescribeTableResponse =
        getHashRangeTableByBillingMode(BillingMode.PAY_PER_REQUEST);
    doReturn(mockDescribeTableResponse).when(mockDynamoClient).describeTable(any());

    DynamoDBStorageHandler spyDynamoStorageHandler = Mockito.spy(storageHandler);
    doReturn(mockDynamoClient).when(spyDynamoStorageHandler).createDynamoDBClient((TableDesc) any());

    TableDesc testTableDesc = new TableDesc();
    Properties properties = new Properties();
    properties.setProperty(DynamoDBConstants.REGION, "us-east-1");
    properties.setProperty(DynamoDBConstants.TABLE_NAME, "test-table");
    testTableDesc.setProperties(properties);

    // User has not specified explicit throughput settings, default for on-demand are set
    Map<String, String> jobProperties1 = new HashMap<>();
    spyDynamoStorageHandler.configureTableJobProperties(testTableDesc, jobProperties1);
    assertEquals(jobProperties1.get(DynamoDBConstants.READ_THROUGHPUT), DynamoDBConstants.DEFAULT_CAPACITY_FOR_ON_DEMAND.toString());
    assertEquals(jobProperties1.get(DynamoDBConstants.WRITE_THROUGHPUT), DynamoDBConstants.DEFAULT_CAPACITY_FOR_ON_DEMAND.toString());
    // On demand tables should never dynamically configure throughput
    assertFalse(Boolean.parseBoolean(jobProperties1.get(DynamoDBConstants.READ_THROUGHPUT_AUTOSCALING)));
    assertFalse(Boolean.parseBoolean(jobProperties1.get(DynamoDBConstants.WRITE_THROUGHPUT_AUTOSCALING)));

    // User has specified throughput settings
    properties.setProperty(DynamoDBConstants.READ_THROUGHPUT, "50");
    properties.setProperty(DynamoDBConstants.WRITE_THROUGHPUT, "50");

    Map<String, String> jobProperties2 = new HashMap<>();
    spyDynamoStorageHandler.configureTableJobProperties(testTableDesc, jobProperties2);
    assertEquals(jobProperties2.get(DynamoDBConstants.READ_THROUGHPUT), "50");
    assertEquals(jobProperties2.get(DynamoDBConstants.WRITE_THROUGHPUT), "50");
    assertFalse(Boolean.parseBoolean(jobProperties2.get(DynamoDBConstants.READ_THROUGHPUT_AUTOSCALING)));
    assertFalse(Boolean.parseBoolean(jobProperties2.get(DynamoDBConstants.WRITE_THROUGHPUT_AUTOSCALING)));
  }


  private TableDescription getHashRangeTable() {
    return getHashRangeTableByBillingMode(BillingMode.PROVISIONED);
  }

  private TableDescription getHashRangeTableByBillingMode(BillingMode billingMode) {
    TableDescription description = TableDescription.builder()
        .keySchema(Arrays.asList(
            KeySchemaElement.builder().attributeName("hashKey").build(),
            KeySchemaElement.builder().attributeName("rangeKey").build()))
        .attributeDefinitions(Arrays.asList(
            AttributeDefinition.builder()
                .attributeName("hashKey")
                .attributeType(ScalarAttributeType.S)
                .build(),
            AttributeDefinition.builder()
                .attributeName("rangeKey")
                .attributeType(ScalarAttributeType.N)
                .build()))
        .provisionedThroughput(ProvisionedThroughputDescription.builder()
            .readCapacityUnits(400L)
            .writeCapacityUnits(100L)
            .build())
        .billingModeSummary(BillingModeSummary.builder()
            .billingMode(billingMode)
            .build())
        .itemCount(0L)
        .tableSizeBytes(0L)
        .build();
    return description;
  }
}