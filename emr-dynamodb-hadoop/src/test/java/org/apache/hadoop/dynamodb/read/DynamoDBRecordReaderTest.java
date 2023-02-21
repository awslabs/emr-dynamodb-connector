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
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputDescription;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

public class DynamoDBRecordReaderTest {

  private static final String HASH_KEY = "hashKey";
  private static final String RANGE_KEY = "rangeKey";
  private static final int NUM_RANGE_KEYS_PER_HASH_KEY = 3;
  private static final String[] HASH_KEYS = {"6d9KhslLlNWKoPUpOrYX",
      "gi0aeHusUZPjgpNJtXNDLzkipmeft", "9w3ZDZyiFheGE", "cfk9gcCgFA6M5g"};

  @Test
  public void testPaginatedReads() {
    DynamoDBSplit split = new DynamoDBSegmentsSplit(null, 0, 0, Arrays.asList(0), 4, 0, new
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
      public RetryResult<ScanResponse> scanTable(String tableName, DynamoDBQueryFilter
          dynamoDBQueryFilter, Integer segment, Integer totalSegments, Map<String,
          AttributeValue> exclusiveStartKey, long limit, Reporter reporter) {

        List<Map<String, AttributeValue>> items = getItems();
        if (i == 0) {
          Map<String, AttributeValue> lastEvaluatedKey = new HashMap<>();
          lastEvaluatedKey.put("test", AttributeValue.fromS("test"));
          i++;
          return new RetryResult<>(ScanResponse.builder()
              .scannedCount(items.size())
              .consumedCapacity(ConsumedCapacity.builder()
                  .capacityUnits(1d)
                  .build())
              .items(items)
              .lastEvaluatedKey(lastEvaluatedKey)
              .build(), 0);
        } else {
          assertEquals("test", exclusiveStartKey.get("test").s());
          return new RetryResult<>(ScanResponse.builder()
              .scannedCount(items.size())
              .consumedCapacity(ConsumedCapacity.builder()
                  .capacityUnits(1d)
                  .build())
              .items(items)
              .build(), 0);
        }
      }

      private List<Map<String, AttributeValue>> getItems() {
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        for (String key : HASH_KEYS) {
          Map<String, AttributeValue> item = new HashMap<>();
          item.put("hashKey", AttributeValue.fromS(key));
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
    DynamoDBSplit split = new DynamoDBSegmentsSplit(null, 0, 0, Arrays.asList(0), 4, 0, new
        DynamoDBQueryFilter());

    DynamoDBRecordReaderContext context = buildContext();
    context.setSplit(split);
    context.setClient(new DynamoDBClient() {

      @Override
      public TableDescription describeTable(String tableName) {
        return getTableDescription("S", "N");
      }

      @Override
      public RetryResult<ScanResponse> scanTable(String tableName, DynamoDBQueryFilter
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
    DynamoDBSplit split = new DynamoDBSegmentsSplit(null, 0, 0, Arrays.asList(0), 4, 0, new
        DynamoDBQueryFilter());

    DynamoDBRecordReaderContext context = buildContext();
    context.setSplit(split);
    context.setClient(new DynamoDBClient() {

      @Override
      public TableDescription describeTable(String tableName) {
        return getTableDescription("S", "N");
      }

      @Override
      public RetryResult<ScanResponse> scanTable(String tableName, DynamoDBQueryFilter
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
    DynamoDBSplit split = new DynamoDBSegmentsSplit(null, 0, 0, Arrays.asList(0), 4, 0, new
        DynamoDBQueryFilter());

    DynamoDBRecordReaderContext context = buildContext();
    context.setSplit(split);
    context.setClient(new DynamoDBClient() {
      @Override
      public TableDescription describeTable(String tableName) {
        return getTableDescription("S", null);
      }

      @Override
      public RetryResult<ScanResponse> scanTable(String tableName, DynamoDBQueryFilter
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
    DynamoDBSplit split = new DynamoDBSegmentsSplit(null, 0, 0, Arrays.asList(0), 4, 0, new
        DynamoDBQueryFilter());
    DynamoDBRecordReaderContext context = buildContext();
    context.setSplit(split);
    context.setClient(new DynamoDBClient() {
      @Override
      public TableDescription describeTable(String tableName) {
        return getTableDescription("S", null);
      }

      @Override
      public RetryResult<ScanResponse> scanTable(String tableName, DynamoDBQueryFilter
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
      return value.getItem().get(keyName).s();
    } else {
      return value.getItem().get(keyName).n();
    }
  }

  private ScanResponse getHashKeyItems(String[] hashKeys) {
    return getHashKeyItems(hashKeys, "S");
  }

  private ScanResponse getHashKeyItems(String[] hashKeys, String type) {
    List<Map<String, AttributeValue>> items = new ArrayList<>();
    for (String key : hashKeys) {
      Map<String, AttributeValue> item = new HashMap<>();
      if (type.equals("S")) {
        item.put("hashKey", AttributeValue.fromS(key));
      } else {
        item.put("hashKey", AttributeValue.fromN(key));
      }
      items.add(item);
    }
    return ScanResponse.builder()
        .scannedCount(items.size())
        .items(items)
        .consumedCapacity(ConsumedCapacity.builder()
            .capacityUnits(1d)
            .build())
        .build();
  }

  private ScanResponse getHashNumberRangeKeyItems(String[] hashKeys, String hashType) {
    List<Map<String, AttributeValue>> items = new ArrayList<>();
    for (String key : hashKeys) {
      for (Integer i = 0; i < NUM_RANGE_KEYS_PER_HASH_KEY; i++) {
        Map<String, AttributeValue> item = new HashMap<>();
        if (hashType.equals("S")) {
          item.put("hashKey", AttributeValue.fromS(key));
        } else {
          item.put("hashKey", AttributeValue.fromN(key));
        }
        item.put("rangeKey", AttributeValue.fromN("0" + i.toString()));
        items.add(item);
      }
    }
    return ScanResponse.builder()
        .scannedCount(items.size())
        .items(items)
        .consumedCapacity(ConsumedCapacity.builder()
            .capacityUnits(1d)
            .build())
        .build();
  }

  private TableDescription getTableDescription(String hashType, String rangeType) {
    List<KeySchemaElement> keySchema = new ArrayList<>();
    List<AttributeDefinition> definitions = new ArrayList<>();

    keySchema.add(KeySchemaElement.builder()
        .attributeName("hashKey")
        .keyType(KeyType.HASH)
        .build());
    definitions.add(AttributeDefinition.builder()
        .attributeName("hashKey")
        .attributeType(hashType)
        .build());

    if (rangeType != null) {
      keySchema.add(KeySchemaElement.builder()
          .attributeName("rangeKey")
          .keyType(KeyType.RANGE)
          .build());
      definitions.add(AttributeDefinition.builder()
          .attributeName("rangeKey")
          .attributeType(rangeType)
          .build());
    }

    TableDescription description = TableDescription.builder()
        .keySchema(keySchema)
        .attributeDefinitions(definitions)
        .provisionedThroughput(ProvisionedThroughputDescription.builder()
            .readCapacityUnits(1000L)
            .writeCapacityUnits(1000L)
            .build())
        .build();
    return description;
  }

  private JobConf getTestConf() {
    JobConf conf = new JobConf();
    conf.set(DynamoDBConstants.DEFAULT_ACCESS_KEY_CONF, "abc");
    conf.set(DynamoDBConstants.DEFAULT_SECRET_KEY_CONF, "abcd");
    return conf;
  }
}
