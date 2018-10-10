/**
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
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class DynamoDBListStorageHandlerTest extends DynamoDBStorageHandlerTest {

  @Before
  @Override
  public void setup() {
    storageHandler = new DynamoDBListStorageHandler();
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

  private TableDescription getHashRangeTable() {
    TableDescription description = new TableDescription().withKeySchema(Arrays.asList(new
      KeySchemaElement().withAttributeName("hashKey"), new KeySchemaElement().withAttributeName
      ("rangeKey"))).withAttributeDefinitions(Arrays.asList(new AttributeDefinition("hashKey",
      ScalarAttributeType.S), new AttributeDefinition("rangeKey", ScalarAttributeType.N)));
    return description;
  }
}
