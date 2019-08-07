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
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import org.apache.hadoop.dynamodb.DynamoDBConstants;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class DynamoDBStorageHandlerTest {

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();
  protected DynamoDBStorageHandler storageHandler;

  @Before
  public void setup() {
    storageHandler = new DynamoDBStorageHandler();
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

  private TableDescription getHashRangeTable() {
    TableDescription description = new TableDescription().withKeySchema(Arrays.asList(
            new KeySchemaElement().withAttributeName("hashKey"),
            new KeySchemaElement().withAttributeName("rangeKey"))
    ).withAttributeDefinitions(Arrays.asList(
            new AttributeDefinition("hashKey", ScalarAttributeType.S),
            new AttributeDefinition("rangeKey", ScalarAttributeType.N))
    );
    return description;
  }
}