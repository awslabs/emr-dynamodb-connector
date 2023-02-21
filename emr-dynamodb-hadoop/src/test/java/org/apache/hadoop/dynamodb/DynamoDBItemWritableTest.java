/**
 * Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file
 * except in compliance with the License. A copy of the License is located at
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "LICENSE.TXT" file accompanying this file. This file is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under the License.
 */

package org.apache.hadoop.dynamodb;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class DynamoDBItemWritableTest {

  private DynamoDBItemWritable item;

  @Before
  public void setup() {
    item = new DynamoDBItemWritable();
  }

  @Test
  public void testSerialization() throws IOException {
    setTestData();

    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(outStream);
    item.write(out);
    outStream.close();

    final byte[] data = outStream.toByteArray();

    item.setItem(null);
    assertNull(item.getItem());

    item.readFields(new DataInputStream(new ByteArrayInputStream(data)));
    checkReturnedItem();
  }

  @Test
  public void testSerializationBackwardsCompatibility() throws IOException {
    setTestData();

    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(outStream);
    out.writeUTF(item.writeStream()); // the old method of serialization
    outStream.close();

    item.setItem(null);
    assertNull(item.getItem());

    item.readFields(new DataInputStream(new ByteArrayInputStream(outStream.toByteArray())));
    checkReturnedItem();

    item.setItem(new HashMap<String, AttributeValue>());
    outStream = new ByteArrayOutputStream();
    out = new DataOutputStream(outStream);
    out.writeUTF(item.writeStream()); // the old method of serialization
    outStream.close();

    item.setItem(null);
    item.readFields(new DataInputStream(new ByteArrayInputStream(outStream.toByteArray())));
    assertEquals(item.getItem(), new HashMap<String, AttributeValue>());
  }

  private void checkReturnedItem() {
    assertNotNull(item.getItem());
    Map<String, AttributeValue> returnedData = item.getItem();
    assertEquals(5, returnedData.size());
    assertEquals("test", returnedData.get("s").s());
    assertEquals("1234", returnedData.get("n").n());
    assertNull(returnedData.get("n").s());
    assertEquals(0, returnedData.get("ss").ss().size());
    assertEquals(3, returnedData.get("ns").ns().size());
    assertEquals(2, returnedData.get("l").l().size());

    List<String> ns = returnedData.get("ns").ns();
    assertEquals("1.0", ns.get(0));
    assertEquals("1.10", ns.get(1));
    assertEquals("2.0", ns.get(2));

    List<AttributeValue> l = returnedData.get("l").l();
    assertEquals("1.0", l.get(0).s());
    assertEquals("0", l.get(1).s());
  }

  @Test
  public void testBinarySerialization() {
    Random rnd = new Random();
    Gson gson = DynamoDBUtil.getGson();
    Type type = new TypeToken<Map<String, AttributeValue>>() {
    }.getType();

    int loop = 1000;
    int totalByteArrays = 50;
    int byteArrayLength = 1024;

    List<ByteBuffer> byteBuffers = new ArrayList<>();
    for (int i = 0; i < totalByteArrays; i++) {
      byte[] bytes = new byte[byteArrayLength];
      rnd.nextBytes(bytes);
      byteBuffers.add(ByteBuffer.wrap(bytes));
    }

    for (int i = 0; i < loop; i++) {
      Map<String, AttributeValue> map = new HashMap<>();
      map.put("hash", AttributeValue.fromB(SdkBytes.fromByteBuffer(byteBuffers.get(rnd.nextInt(totalByteArrays)))));
      map.put("range", AttributeValue.fromB(SdkBytes.fromByteBuffer(byteBuffers.get(rnd.nextInt(totalByteArrays)))));
      map.put("list", AttributeValue.fromBs(Arrays.asList(
          SdkBytes.fromByteBuffer(byteBuffers.get(rnd.nextInt(totalByteArrays))),
          SdkBytes.fromByteBuffer(byteBuffers.get(rnd.nextInt(totalByteArrays))))));

      Map<String, AttributeValue> dynamoDBItem = gson.fromJson(gson.toJson(map, type), type);
      compare(map, dynamoDBItem);
    }
  }

  @Test
  public void testMalformedJsonDeserialization() {
    String malformedJson = "attr1" + DynamoDBItemWritable.END_OF_TEXT + "{\"s\":\"seattle\"}" +
        DynamoDBItemWritable.START_OF_TEXT + "attr2" + DynamoDBItemWritable.END_OF_TEXT +
        "{\"nS\":[\"123\",\"456\",\"789\"]}";

    item.readFieldsStream(malformedJson);
    Map<String, AttributeValue> attrValueMap = item.getItem();

    assertEquals("seattle", attrValueMap.get("attr1").s());
    assertEquals(new HashSet<>(Arrays.asList("123", "456", "789")), new HashSet<>(attrValueMap
        .get("attr2").ns()));
  }

  private void compare(Map<String, AttributeValue> map, Map<String, AttributeValue> map2) {
    AttributeValue lHash = map.get("hash");
    AttributeValue lRange = map.get("range");

    AttributeValue rHash = map2.get("hash");
    AttributeValue rRange = map2.get("range");

    AttributeValue lList = map.get("list");
    AttributeValue rList = map.get("list");

    assertArrayEquals(lHash.b().asByteArray(), rHash.b().asByteArray());
    assertArrayEquals(lRange.b().asByteArray(), rRange.b().asByteArray());
    assertArrayEquals(lList.bs().get(0).asByteArray(), rList.bs().get(0).asByteArray());
    assertArrayEquals(lList.bs().get(1).asByteArray(), rList.bs().get(1).asByteArray());
  }

  private void setTestData() {
    List<String> ss = new ArrayList<>();
    List<String> ns = new ArrayList<>();
    ns.add("1.0");
    ns.add("1.10");
    ns.add("2.0");
    List<AttributeValue> l = new ArrayList<>();
    l.add(AttributeValue.fromS("1.0"));
    l.add(AttributeValue.fromS("0"));

    Map<String, AttributeValue> sampleData = new HashMap<>();
    sampleData.put("s", AttributeValue.fromS("test"));
    sampleData.put("n", AttributeValue.fromN("1234"));
    sampleData.put("ss", AttributeValue.fromSs(ss));
    sampleData.put("ns", AttributeValue.fromNs(ns));
    sampleData.put("l", AttributeValue.fromL(l));

    item.setItem(sampleData);
  }

  @Test
  public void testParametrizedConstructor() {
    Map<String, AttributeValue> map = new HashMap<>();
    DynamoDBItemWritable item = new DynamoDBItemWritable(map);
    assertEquals(item.getItem(), map);
  }
}
