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

import com.amazonaws.AmazonClientException;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.regions.ServiceAbbreviations;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.util.EC2MetadataUtils;

import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(PowerMockRunner.class)
@PrepareForTest({RegionUtils.class, EC2MetadataUtils.class})
public class DynamoDBUtilTest {

  final String TEST_ENDPOINT = "http://emr.test-dynamodb.endpoint";
  final String TEST_REGION = "test-region";

  Configuration conf = new Configuration();
  Region region;

  private static final List<String> TEST_NAMES = Lists.newArrayList("id", "payload", "number", "collection");
  private static final String TEST_STRING = "AfFLIHsycSvZoEhPPKHUrtwewDAlcD";
  private static final String TEST_NUMBER = "3592.0001";
  private static final List<String> TEST_NUMBER_ARRAY = Lists.newArrayList("2.14748364", "1.23452487", "1.73904643");
  private static final List<AttributeValue> TEST_LIST = Lists.newArrayList(new AttributeValue(TEST_STRING),
    new AttributeValue().withN(TEST_NUMBER));
  private static final List<String> TEST_MAP_KEYS = Lists.newArrayList("mapString", "mapNumber");

  @Before
  public void setUp() {
    PowerMockito.spy(RegionUtils.class);
    PowerMockito.mockStatic(EC2MetadataUtils.class);
    region = mock(Region.class);
    conf.clear();
  }

  @Test
  public void testArrayItemSize() throws UnsupportedEncodingException {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put(TEST_NAMES.get(0), new AttributeValue(TEST_STRING));
    item.put(TEST_NAMES.get(1), new AttributeValue(TEST_STRING));
    item.put(TEST_NAMES.get(2), new AttributeValue().withN(TEST_NUMBER));
    item.put(TEST_NAMES.get(3), new AttributeValue().withNS(TEST_NUMBER_ARRAY));

    List<String> allStrings = Lists.newArrayList(TEST_STRING, TEST_STRING, TEST_NUMBER);
    allStrings.addAll(TEST_NAMES);
    allStrings.addAll(TEST_NUMBER_ARRAY);

    assertEquals(getExpectedItemSize(allStrings), DynamoDBUtil.getItemSizeBytes(item));
  }
  
  @Test
  public void testListItemSize() throws UnsupportedEncodingException {
    Map<String, AttributeValue> item = new HashMap<>();
    int expectedSize = 0;
    item.put(TEST_NAMES.get(0), new AttributeValue(TEST_STRING));
    item.put(TEST_NAMES.get(1), new AttributeValue(TEST_STRING));
    item.put(TEST_NAMES.get(2), new AttributeValue().withN(TEST_NUMBER));
    item.put(TEST_NAMES.get(3), new AttributeValue().withL(TEST_LIST));

    List<String> allStrings = Lists.newArrayList(TEST_STRING, TEST_STRING, TEST_STRING, TEST_NUMBER, TEST_NUMBER);
    allStrings.addAll(TEST_NAMES);

    assertEquals(getExpectedItemSize(allStrings), DynamoDBUtil.getItemSizeBytes(item));
  }
  
  @Test
  public void testMapItemSize() throws UnsupportedEncodingException {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put(TEST_NAMES.get(0), new AttributeValue(TEST_STRING));
    item.put(TEST_NAMES.get(1), new AttributeValue(TEST_STRING));
    item.put(TEST_NAMES.get(2), new AttributeValue().withN(TEST_NUMBER));

    Map<String, AttributeValue> attrMap = new HashMap<>();
    attrMap.put(TEST_MAP_KEYS.get(0), new AttributeValue(TEST_STRING));
    attrMap.put(TEST_MAP_KEYS.get(1), new AttributeValue().withN(TEST_NUMBER));
    item.put(TEST_NAMES.get(3), new AttributeValue().withM(attrMap));

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
    item.put(null, new AttributeValue(TEST_STRING));
    item.put(TEST_NAMES.get(0), null);

    List<String> allStrings = Lists.newArrayList(TEST_STRING, TEST_NAMES.get(0));

    assertEquals(getExpectedItemSize(allStrings), DynamoDBUtil.getItemSizeBytes(item));
  }

  @Test
  public void testNumberItemSize() throws UnsupportedEncodingException {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put(TEST_NAMES.get(0), new AttributeValue().withN(TEST_NUMBER));
    item.put(TEST_NAMES.get(1), new AttributeValue(TEST_STRING));
    item.put(TEST_NAMES.get(2), new AttributeValue().withN(TEST_NUMBER));
    item.put(TEST_NAMES.get(3), new AttributeValue().withNS(TEST_NUMBER_ARRAY));

    List<String> allStrings = Lists.newArrayList(TEST_STRING, TEST_NUMBER, TEST_NUMBER);
    allStrings.addAll(TEST_NAMES);
    allStrings.addAll(TEST_NUMBER_ARRAY);

    assertEquals(getExpectedItemSize(allStrings), DynamoDBUtil.getItemSizeBytes(item));
  }

  @Test
  public void getsEndpointFromConf() {
    conf.set(DynamoDBConstants.ENDPOINT, TEST_ENDPOINT);
    assertEquals(TEST_ENDPOINT, DynamoDBUtil.getDynamoDBEndpoint(conf, null));
    verify(region, never()).getServiceEndpoint(ServiceAbbreviations.Dynamodb);
  }

  @Test
  public void getsEndpointFromRegion() {
    when(RegionUtils.getRegion(TEST_REGION)).thenReturn(region);
    when(region.getServiceEndpoint(ServiceAbbreviations.Dynamodb)).thenReturn(TEST_ENDPOINT);
    conf.set(DynamoDBConstants.REGION, TEST_REGION);
    assertEquals(TEST_ENDPOINT, DynamoDBUtil.getDynamoDBEndpoint(conf, null));
    verify(region).getServiceEndpoint(ServiceAbbreviations.Dynamodb);
  }

  @Test
  public void getsEndpointFromEc2InstanceRegion() {
    when(EC2MetadataUtils.getEC2InstanceRegion()).thenReturn("ec2-instance-region");
    when(RegionUtils.getRegion("ec2-instance-region")).thenReturn(region);
    when(region.getServiceEndpoint(ServiceAbbreviations.Dynamodb)).thenReturn(TEST_ENDPOINT);
    assertEquals(TEST_ENDPOINT, DynamoDBUtil.getDynamoDBEndpoint(conf, null));
    PowerMockito.verifyStatic();
    EC2MetadataUtils.getEC2InstanceRegion();
    verify(region, never()).getServiceEndpoint(TEST_REGION);
  }

  @Test
  public void getsEndpointFromDefaultAwsRegion() {
    PowerMockito.mockStatic(RegionUtils.class);
    when(EC2MetadataUtils.getEC2InstanceRegion()).thenThrow(new AmazonClientException("Unable to " +
        "get region from EC2 instance data"));
    when(RegionUtils.getRegion(DynamoDBConstants.DEFAULT_AWS_REGION)).thenReturn(region);
    when(region.getServiceEndpoint(ServiceAbbreviations.Dynamodb)).thenReturn(TEST_ENDPOINT);
    assertEquals(TEST_ENDPOINT, DynamoDBUtil.getDynamoDBEndpoint(conf, null));
    PowerMockito.verifyStatic();
    RegionUtils.getRegion(DynamoDBConstants.DEFAULT_AWS_REGION);
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
