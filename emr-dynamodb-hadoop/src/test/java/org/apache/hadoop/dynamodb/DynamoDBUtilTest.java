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

package org.apache.hadoop.dynamodb;

import static org.apache.hadoop.dynamodb.DynamoDBConstants.DEFAULT_MAX_ITEMS_PER_BATCH;
import static org.apache.hadoop.dynamodb.DynamoDBUtil.getBoundedBatchLimit;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.internal.util.EC2MetadataUtils;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

@RunWith(PowerMockRunner.class)
@PrepareForTest({EC2MetadataUtils.class})
public class DynamoDBUtilTest {

  final String TEST_ENDPOINT = "http://emr.test-dynamodb.endpoint";
  final String TEST_REGION = "test-region";
  final String TEST_REGION_ID = "test-region-id";

  Configuration conf = new Configuration();
  Region region;

  private static final List<String> TEST_NAMES = Lists.newArrayList("id", "payload", "number", "collection");
  private static final String TEST_STRING = "AfFLIHsycSvZoEhPPKHUrtwewDAlcD";
  private static final String TEST_NUMBER = "3592.0001";
  private static final List<String> TEST_NUMBER_ARRAY = Lists.newArrayList("2.14748364", "1.23452487", "1.73904643");
  private static final List<AttributeValue> TEST_LIST = Lists.newArrayList(AttributeValue.fromS(TEST_STRING),
    AttributeValue.fromN(TEST_NUMBER));
  private static final List<String> TEST_MAP_KEYS = Lists.newArrayList("mapString", "mapNumber");

  @Before
  public void setUp() {
    PowerMockito.mockStatic(EC2MetadataUtils.class);
    conf.clear();
  }

  @Test
  public void testArrayItemSize() throws UnsupportedEncodingException {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put(TEST_NAMES.get(0), AttributeValue.fromS(TEST_STRING));
    item.put(TEST_NAMES.get(1), AttributeValue.fromS(TEST_STRING));
    item.put(TEST_NAMES.get(2), AttributeValue.fromN(TEST_NUMBER));
    item.put(TEST_NAMES.get(3), AttributeValue.fromNs(TEST_NUMBER_ARRAY));

    List<String> allStrings = Lists.newArrayList(TEST_STRING, TEST_STRING, TEST_NUMBER);
    allStrings.addAll(TEST_NAMES);
    allStrings.addAll(TEST_NUMBER_ARRAY);

    assertEquals(getExpectedItemSize(allStrings), DynamoDBUtil.getItemSizeBytes(item));
  }
  
  @Test
  public void testListItemSize() throws UnsupportedEncodingException {
    Map<String, AttributeValue> item = new HashMap<>();
    int expectedSize = 0;
    item.put(TEST_NAMES.get(0), AttributeValue.fromS(TEST_STRING));
    item.put(TEST_NAMES.get(1), AttributeValue.fromS(TEST_STRING));
    item.put(TEST_NAMES.get(2), AttributeValue.fromN(TEST_NUMBER));
    item.put(TEST_NAMES.get(3), AttributeValue.fromL(TEST_LIST));

    List<String> allStrings = Lists.newArrayList(TEST_STRING, TEST_STRING, TEST_STRING, TEST_NUMBER, TEST_NUMBER);
    allStrings.addAll(TEST_NAMES);

    assertEquals(getExpectedItemSize(allStrings), DynamoDBUtil.getItemSizeBytes(item));
  }
  
  @Test
  public void testMapItemSize() throws UnsupportedEncodingException {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put(TEST_NAMES.get(0), AttributeValue.fromS(TEST_STRING));
    item.put(TEST_NAMES.get(1), AttributeValue.fromS(TEST_STRING));
    item.put(TEST_NAMES.get(2), AttributeValue.fromN(TEST_NUMBER));

    Map<String, AttributeValue> attrMap = new HashMap<>();
    attrMap.put(TEST_MAP_KEYS.get(0), AttributeValue.fromS(TEST_STRING));
    attrMap.put(TEST_MAP_KEYS.get(1), AttributeValue.fromN(TEST_NUMBER));
    item.put(TEST_NAMES.get(3), AttributeValue.fromM(attrMap));

    List<String> allStrings = Lists.newArrayList(TEST_STRING, TEST_STRING, TEST_STRING, TEST_NUMBER, TEST_NUMBER);
    allStrings.addAll(TEST_NAMES);
    allStrings.addAll(TEST_MAP_KEYS);

    assertEquals(getExpectedItemSize(allStrings), DynamoDBUtil.getItemSizeBytes(item));
  }

  @Test
  public void testEmptyItemSize() throws UnsupportedEncodingException {
    Map<String, AttributeValue> item = new HashMap<>();

    assertEquals(0, DynamoDBUtil.getItemSizeBytes(item));
  }

  @Test
  public void testNullItemSize() throws UnsupportedEncodingException {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put(null, AttributeValue.fromS(TEST_STRING));
    item.put(TEST_NAMES.get(0), null);

    List<String> allStrings = Lists.newArrayList(TEST_STRING, TEST_NAMES.get(0));

    assertEquals(getExpectedItemSize(allStrings), DynamoDBUtil.getItemSizeBytes(item));
  }

  @Test
  public void testNumberItemSize() throws UnsupportedEncodingException {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put(TEST_NAMES.get(0), AttributeValue.fromN(TEST_NUMBER));
    item.put(TEST_NAMES.get(1), AttributeValue.fromS(TEST_STRING));
    item.put(TEST_NAMES.get(2), AttributeValue.fromN(TEST_NUMBER));
    item.put(TEST_NAMES.get(3), AttributeValue.fromNs(TEST_NUMBER_ARRAY));

    List<String> allStrings = Lists.newArrayList(TEST_STRING, TEST_NUMBER, TEST_NUMBER);
    allStrings.addAll(TEST_NAMES);
    allStrings.addAll(TEST_NUMBER_ARRAY);

    assertEquals(getExpectedItemSize(allStrings), DynamoDBUtil.getItemSizeBytes(item));
  }

  @Test
  public void getsEndpointFromConf() {
    conf.set(DynamoDBConstants.ENDPOINT, TEST_ENDPOINT);
    assertEquals(TEST_ENDPOINT, DynamoDBUtil.getDynamoDBEndpoint(conf, null));
  }

  @Test
  public void getsRegionFromConf() {
    conf.set(DynamoDBConstants.REGION, TEST_REGION);
    assertEquals(TEST_REGION, DynamoDBUtil.getDynamoDBRegion(conf, null));
  }

  @Test
  public void getsRegionIdFromConf() {
    conf.set(DynamoDBConstants.REGION_ID, TEST_REGION_ID);
    assertEquals(TEST_REGION_ID, DynamoDBUtil.getDynamoDBRegion(conf, null));
  }

  @Test
  public void getsRegionFromEc2Instance() {
    final String EC2_INSTANCE_REGION = "ec2-instance-region";
    when(EC2MetadataUtils.getEC2InstanceRegion()).thenReturn(EC2_INSTANCE_REGION);
    assertEquals(EC2_INSTANCE_REGION, DynamoDBUtil.getDynamoDBRegion(conf, null));
    PowerMockito.verifyStatic();
    EC2MetadataUtils.getEC2InstanceRegion();
  }

  @Test
  public void getsRegionFromDefaultAwsRegion() {
    when(EC2MetadataUtils.getEC2InstanceRegion())
        .thenThrow(SdkException.builder()
            .message("Unable to get region from EC2 instance data")
            .build());

    assertEquals(DynamoDBConstants.DEFAULT_AWS_REGION, DynamoDBUtil.getDynamoDBRegion(conf, null));
    PowerMockito.verifyStatic();
  }


  @Test
  public void testGetBoundedBatchLimit() {
    assertEquals(1, getBoundedBatchLimit(conf, 0));
    assertEquals(1, getBoundedBatchLimit(conf, 1));
    assertEquals(DEFAULT_MAX_ITEMS_PER_BATCH,
        getBoundedBatchLimit(conf, DEFAULT_MAX_ITEMS_PER_BATCH + 1));
    assertEquals(DEFAULT_MAX_ITEMS_PER_BATCH,
        getBoundedBatchLimit(conf, DEFAULT_MAX_ITEMS_PER_BATCH));
  }

  private int getExpectedItemSize(List<String> strings) {
    int size = 0;
    for (String str : strings) {
      size += str.length();
    }
    return size;
  }
}
