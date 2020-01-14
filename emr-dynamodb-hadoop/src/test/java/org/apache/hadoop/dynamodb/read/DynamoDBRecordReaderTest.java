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

package org.apache.hadoop.dynamodb.read;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputDescription;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;

import org.apache.hadoop.dynamodb.DynamoDBClient;
import org.apache.hadoop.dynamodb.DynamoDBConstants;
import org.apache.hadoop.dynamodb.DynamoDBFibonacciRetryer.RetryResult;
import org.apache.hadoop.dynamodb.DynamoDBItemWritable;
import org.apache.hadoop.dynamodb.filter.DynamoDBQueryFilter;
import org.apache.hadoop.dynamodb.preader.DynamoDBRecordReaderContext;
import org.apache.hadoop.dynamodb.split.DynamoDBSegmentsSplit;
import org.apache.hadoop.dynamodb.split.DynamoDBSplit;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DynamoDBRecordReaderTest {

  private static final String HASH_KEY = "hashKey";
  private static final String RANGE_KEY = "rangeKey";
  private static final int NUM_RANGE_KEYS_PER_HASH_KEY = 3;
  private static final String[] HASH_KEYS = {"6d9KhslLlNWKoPUpOrYX",
      "gi0aeHusUZPjgpNJtXNDLzkipmeft", "9w3ZDZyiFheGE", "cfk9gcCgFA6M5g"};

  @Test
  public void testPaginatedReads() {
    DynamoDBSplit split = new DynamoDBSegmentsSplit(null, 0, 0, Arrays.asList(0), 4, new
        DynamoDBQueryFilter());

    DynamoDBRecordReaderContext context = buildContext();
    context.setSplit(split);
    context.setClient(new DynamoDBClient() {
      int i = 0;

      @Override
      public TableDescription describeTable(String tableName) {
        return getTableDescription("S", null);
      }

      @Override
      public RetryResult<ScanResult> scanTable(String tableName, DynamoDBQueryFilter
          dynamoDBQueryFilter, Integer segment, Integer totalSegments, Map<String,
          AttributeValue> exclusiveStartKey, long limit, Reporter reporter) {

        List<Map<String, AttributeValue>> items = getItems();
        if (i == 0) {
          Map<String, AttributeValue> lastEvaluatedKey = new HashMap<>();
          lastEvaluatedKey.put("test", new AttributeValue("test"));
          i++;
          return new RetryResult<>(new ScanResult().withScannedCount(items.size())
              .withConsumedCapacity(new ConsumedCapacity().withCapacityUnits(1d)).withItems
                  (items).withLastEvaluatedKey(lastEvaluatedKey), 0);
        } else {
          assertEquals("test", exclusiveStartKey.get("test").getS());
          return new RetryResult<>(new ScanResult().withScannedCount(items.size())
              .withConsumedCapacity(new ConsumedCapacity().withCapacityUnits(1d)).withItems
                  (items), 0);
        }
      }

      private List<Map<String, AttributeValue>> getItems() {
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        for (String key : HASH_KEYS) {
          Map<String, AttributeValue> item = new HashMap<>();
          item.put("hashKey", new AttributeValue(key));
          items.add(item);
        }
        return items;
      }
    });

    // Setup mock client
    DefaultDynamoDBRecordReader reader = new DefaultDynamoDBRecordReader(context);

    // Check result
    try {
      for (int i = 0; i < 8; i++) {
        DynamoDBItemWritable value = reader.createValue();
        reader.next(reader.createKey(), value);
        assertEquals(HASH_KEYS[(i / 2) % 4], getHashKeyValue(value, "S"));
      }
      boolean ret = reader.next(reader.createKey(), null);
      assertEquals(false, ret);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testConsumingRemainingElementsIfEndKeyIsNull() {
    DynamoDBSplit split = new DynamoDBSegmentsSplit(null, 0, 0, Arrays.asList(0), 4, new
        DynamoDBQueryFilter());

    DynamoDBRecordReaderContext context = buildContext();
    context.setSplit(split);
    context.setClient(new DynamoDBClient() {

      @Override
      public TableDescription describeTable(String tableName) {
        return getTableDescription("S", "N");
      }

      @Override
      public RetryResult<ScanResult> scanTable(String tableName, DynamoDBQueryFilter
          dynamoDBQueryFilter, Integer segment, Integer totalSegments, Map<String,
          AttributeValue> exclusiveStartKey, long limit, Reporter reporter) {
        return new RetryResult<>(getHashNumberRangeKeyItems(HASH_KEYS, "S"), 0);
      }
    });

    // Setup mock client
    DefaultDynamoDBRecordReader reader = new DefaultDynamoDBRecordReader(context);

    // Check result
    try {
      for (String hashKey : HASH_KEYS) {
        for (int j = 0; j < NUM_RANGE_KEYS_PER_HASH_KEY; j++) {
          DynamoDBItemWritable value = reader.createValue();
          reader.next(reader.createKey(), value);
          assertEquals(hashKey, getHashKeyValue(value, "S"));
          assertEquals(j, Integer.parseInt(getRangeKeyValue(value, "N")));
        }
      }
      boolean ret = reader.next(reader.createKey(), reader.createValue());
      assertEquals(false, ret);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testConsumingAllBeginningElementsIfStartKeyIsNull() {
    DynamoDBSplit split = new DynamoDBSegmentsSplit(null, 0, 0, Arrays.asList(0), 4, new
        DynamoDBQueryFilter());

    DynamoDBRecordReaderContext context = buildContext();
    context.setSplit(split);
    context.setClient(new DynamoDBClient() {

      @Override
      public TableDescription describeTable(String tableName) {
        return getTableDescription("S", "N");
      }

      @Override
      public RetryResult<ScanResult> scanTable(String tableName, DynamoDBQueryFilter
          dynamoDBQueryFilter, Integer segment, Integer totalSegments, Map<String,
          AttributeValue> exclusiveStartKey, long limit, Reporter reporter) {
        assertNull(exclusiveStartKey);
        return new RetryResult<>(getHashNumberRangeKeyItems(HASH_KEYS, "S"), 0);
      }
    });

    // Setup mock client
    DefaultDynamoDBRecordReader reader = new DefaultDynamoDBRecordReader(context);

    // Check result
    try {
      for (String hashKey : HASH_KEYS) {
        for (int j = 0; j < NUM_RANGE_KEYS_PER_HASH_KEY; j++) {
          DynamoDBItemWritable value = reader.createValue();
          reader.next(reader.createKey(), value);
          assertEquals(hashKey, getHashKeyValue(value, "S"));
          assertEquals(j, Integer.parseInt(getRangeKeyValue(value, "N")));
        }
      }
      boolean ret = reader.next(reader.createKey(), reader.createValue());
      assertEquals(false, ret);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testScanFirstSegment() {
    DynamoDBSplit split = new DynamoDBSegmentsSplit(null, 0, 0, Arrays.asList(0), 4, new
        DynamoDBQueryFilter());

    DynamoDBRecordReaderContext context = buildContext();
    context.setSplit(split);
    context.setClient(new DynamoDBClient() {
      @Override
      public TableDescription describeTable(String tableName) {
        return getTableDescription("S", null);
      }

      @Override
      public RetryResult<ScanResult> scanTable(String tableName, DynamoDBQueryFilter
          dynamoDBQueryFilter, Integer segment, Integer totalSegments, Map<String,
          AttributeValue> exclusiveStartKey, long limit, Reporter reporter) {
        assertEquals(0, (int) segment);
        assertEquals(4, (int) totalSegments);
        return new RetryResult<>(getHashKeyItems(HASH_KEYS), 0);
      }
    });

    // Setup mock client
    DefaultDynamoDBRecordReader reader = new DefaultDynamoDBRecordReader(context);

    // Check result
    try {
      for (String hashKey : HASH_KEYS) {
        DynamoDBItemWritable value = reader.createValue();
        reader.next(reader.createKey(), value);
        assertEquals(hashKey, getHashKeyValue(value, "S"));
      }
      boolean ret = reader.next(reader.createKey(), null);
      assertEquals(false, ret);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test(expected = IOException.class, timeout = 10000)
  public void testExceptionInDbClient() throws IOException {
    DynamoDBSplit split = new DynamoDBSegmentsSplit(null, 0, 0, Arrays.asList(0), 4, new
        DynamoDBQueryFilter());
    DynamoDBRecordReaderContext context = buildContext();
    context.setSplit(split);
    context.setClient(new DynamoDBClient() {
      @Override
      public TableDescription describeTable(String tableName) {
        return getTableDescription("S", null);
      }

      @Override
      public RetryResult<ScanResult> scanTable(String tableName, DynamoDBQueryFilter
          dynamoDBQueryFilter, Integer segment, Integer totalSegments, Map<String,
          AttributeValue> exclusiveStartKey, long limit, Reporter reporter) {
        throw new RuntimeException("Unrecoverable Exception");
      }
    });

    DefaultDynamoDBRecordReader reader = new DefaultDynamoDBRecordReader(context);

    DynamoDBItemWritable value = reader.createValue();
    reader.next(reader.createKey(), value);
  }

  private DynamoDBRecordReaderContext buildContext() {
    DynamoDBRecordReaderContext context = new DynamoDBRecordReaderContext();
    context.setAttributes(null);
    context.setConf(getTestConf());
    context.setReporter(new Reporter() {

      @Override
      public void progress() {
      }

      @Override
      public void setStatus(String arg0) {
      }

      @Override
      public void incrCounter(String arg0, String arg1, long arg2) {
      }

      @Override
      public void incrCounter(Enum<?> arg0, long arg1) {
      }

      @Override
      public InputSplit getInputSplit() throws UnsupportedOperationException {
        return null;
      }

      @Override
      public Counter getCounter(String arg0, String arg1) {
        return null;
      }

      @Override
      public Counter getCounter(Enum<?> arg0) {
        return null;
      }

      @Override
      public float getProgress() {
        return 0;
      }

    });
    return context;
  }

  private String getHashKeyValue(DynamoDBItemWritable value, String type) {
    return getKeyValue(value, HASH_KEY, type);
  }

  private String getRangeKeyValue(DynamoDBItemWritable value, String type) {
    return getKeyValue(value, RANGE_KEY, type);
  }

  private String getKeyValue(DynamoDBItemWritable value, String keyName, String type) {
    if (type.equals("S")) {
      return value.getItem().get(keyName).getS();
    } else {
      return value.getItem().get(keyName).getN();
    }
  }

  private ScanResult getHashKeyItems(String[] hashKeys) {
    return getHashKeyItems(hashKeys, "S");
  }

  private ScanResult getHashKeyItems(String[] hashKeys, String type) {
    List<Map<String, AttributeValue>> items = new ArrayList<>();
    for (String key : hashKeys) {
      Map<String, AttributeValue> item = new HashMap<>();
      if (type.equals("S")) {
        item.put("hashKey", new AttributeValue(key));
      } else {
        item.put("hashKey", new AttributeValue().withN(key));
      }
      items.add(item);
    }
    return new ScanResult().withScannedCount(items.size()).withItems(items).withConsumedCapacity
        (new ConsumedCapacity().withCapacityUnits(1d));
  }

  private ScanResult getHashNumberRangeKeyItems(String[] hashKeys, String hashType) {
    List<Map<String, AttributeValue>> items = new ArrayList<>();
    for (String key : hashKeys) {
      for (Integer i = 0; i < NUM_RANGE_KEYS_PER_HASH_KEY; i++) {
        Map<String, AttributeValue> item = new HashMap<>();
        if (hashType.equals("S")) {
          item.put("hashKey", new AttributeValue(key));
        } else {
          item.put("hashKey", new AttributeValue().withN(key));
        }
        item.put("rangeKey", new AttributeValue().withN("0" + i.toString()));
        items.add(item);
      }
    }
    return new ScanResult().withScannedCount(items.size()).withItems(items).withConsumedCapacity
        (new ConsumedCapacity().withCapacityUnits(1d));
  }

  private TableDescription getTableDescription(String hashType, String rangeType) {
    List<KeySchemaElement> keySchema = new ArrayList<>();
    List<AttributeDefinition> definitions = new ArrayList<>();

    keySchema.add(new KeySchemaElement().withAttributeName("hashKey").withKeyType(KeyType.HASH));
    definitions.add(new AttributeDefinition().withAttributeName("hashKey").withAttributeType
        (hashType));

    if (rangeType != null) {
      keySchema.add(new KeySchemaElement().withAttributeName("rangeKey").withKeyType(KeyType
          .RANGE));
      definitions.add(new AttributeDefinition().withAttributeName("rangeKey").withAttributeType
          (rangeType));
    }

    TableDescription description = new TableDescription().withKeySchema(keySchema)
        .withAttributeDefinitions(definitions).withProvisionedThroughput(new
            ProvisionedThroughputDescription().withReadCapacityUnits(1000L)
            .withWriteCapacityUnits(1000L));
    return description;
  }

  private JobConf getTestConf() {
    JobConf conf = new JobConf();
    conf.set(DynamoDBConstants.DEFAULT_ACCESS_KEY_CONF, "abc");
    conf.set(DynamoDBConstants.DEFAULT_SECRET_KEY_CONF, "abcd");
    return conf;
  }
}
