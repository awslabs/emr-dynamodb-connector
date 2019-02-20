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
    item.put("id", new AttributeValue("AfFLIHsycSvZoEhPPKHUrtwewDAlcD"));
    item.put("payload", new AttributeValue("AfFLIHsycSvZoEhPPKHUrtwewDAlcD"));
    item.put("number", new AttributeValue().withN("3592.0001"));
    List<String> numberArray = Lists.newArrayList("2.1474836479", "1.2345248749", "1.7390464369");
    item.put("numberArray", new AttributeValue().withNS(numberArray));

    assertEquals(131, DynamoDBUtil.getItemSizeBytes(item));
  }
  
  @Test
  public void testListItemSize() throws UnsupportedEncodingException {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put("id", new AttributeValue("AfFLIHsycSvZoEhPPKHUrtwewDAlcD"));
    item.put("payload", new AttributeValue("AfFLIHsycSvZoEhPPKHUrtwewDAlcD"));
    item.put("number", new AttributeValue().withN("3592.0001"));
    List<AttributeValue> attrList = new ArrayList<>();
    attrList.add(new AttributeValue().withN("20123"));
    attrList.add(new AttributeValue("QERASdfklkajsdfasdf"));
    item.put("testList", new AttributeValue().withL(attrList));

    assertEquals(116, DynamoDBUtil.getItemSizeBytes(item));
  }
  
  @Test
  public void testMapItemSize() throws UnsupportedEncodingException {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put("id", new AttributeValue("AfFLIHsycSvZoEhPPKHUrtwewDAlcD"));
    item.put("payload", new AttributeValue("AfFLIHsycSvZoEhPPKHUrtwewDAlcD"));
    item.put("number", new AttributeValue().withN("3592.0001"));
    Map<String, AttributeValue> attrMap = new HashMap<>();
    attrMap.put("mapNumber", new AttributeValue().withN("20123"));
    attrMap.put("mapString", new AttributeValue("QERASdfklkajsdfasdf"));
    item.put("testMap", new AttributeValue().withM(attrMap));

    assertEquals(133, DynamoDBUtil.getItemSizeBytes(item));
  }

  @Test
  public void testEmptyItemSize() throws UnsupportedEncodingException {
    Map<String, AttributeValue> item = new HashMap<>();

    assertEquals(0, DynamoDBUtil.getItemSizeBytes(item));
  }

  @Test
  public void testNumberItemSize() throws UnsupportedEncodingException {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put("id", new AttributeValue().withN("1234"));
    item.put("payload", new AttributeValue("AfFLIHsycSvZoEhPPKHUrtwewDAlcD"));
    item.put("number", new AttributeValue().withN("3592.0001"));
    List<String> numberArray = Lists.newArrayList("2.1474836479", "1.2345248749", "1.7390464369");
    item.put("numberArray", new AttributeValue().withNS(numberArray));

    assertEquals(105, DynamoDBUtil.getItemSizeBytes(item));
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
}
