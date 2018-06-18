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

package org.apache.hadoop.dynamodb.test;

import static org.junit.Assert.assertEquals;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import org.apache.commons.lang.RandomStringUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class DynamoDBTestUtils {

  private static final String[] attributeTypes = new String[]{"S", "SS", "N", "NS", "L", "B", "BS"};
  private static final int SEED = 87394;
  private static final int MAX_BYTE_ARRAY_LENGTH = 2048;
  private static final int STRING_LENGTH = 1024;
  private static final int LIST_ATTRIBUTES_COUNT = 250;

  private static final Random rnd = new Random(SEED);

  public static String getRandomNumber() {
    int number = rnd.nextInt();

    return String.valueOf(number);
  }

  public static List<String> getRandomNumbers() {
    int count = rnd.nextInt(LIST_ATTRIBUTES_COUNT) + 1;
    List<String> numbers = new ArrayList<>(count);

    for (int i = 0; i < count; i++) {
      numbers.add(getRandomNumber());
    }

    return numbers;
  }

  public static String getRandomString() {
    int length = rnd.nextInt(STRING_LENGTH) + 1;

    return RandomStringUtils.randomAlphanumeric(length);
  }

  public static List<String> getRandomStrings() {
    int count = rnd.nextInt(LIST_ATTRIBUTES_COUNT) + 1;
    List<String> strings = new ArrayList<>(count);

    for (int i = 0; i < count; i++) {
      strings.add(getRandomString());
    }

    return strings;
  }

  public static ByteBuffer getRandomByteBuffer() {
    int bufferSize = rnd.nextInt(MAX_BYTE_ARRAY_LENGTH) + 1;
    byte[] buffer = new byte[bufferSize];

    rnd.nextBytes(buffer);

    return ByteBuffer.wrap(buffer);
  }

  public static List<ByteBuffer> getRandomByteBuffers() {
    int noOfBuffers = rnd.nextInt(LIST_ATTRIBUTES_COUNT) + 1;
    List<ByteBuffer> buffers = new ArrayList<>(noOfBuffers);

    for (int i = 0; i < noOfBuffers; i++) {
      buffers.add(getRandomByteBuffer());
    }

    return buffers;
  }

  public static Map<String, AttributeValue> aRandomMap = getRandomItem();

  public static Map<String, AttributeValue> getRandomItem() {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put("S", new AttributeValue().withS(getRandomString()));
    item.put("SS", new AttributeValue().withSS(getRandomStrings()));
    item.put("N", new AttributeValue().withN(getRandomNumber()));
    item.put("L", new AttributeValue().withL(new AttributeValue().withM(aRandomMap)));
    item.put("NS", new AttributeValue().withNS(getRandomNumbers()));
    item.put("B", new AttributeValue().withB(getRandomByteBuffer()));
    item.put("BS", new AttributeValue().withBS(getRandomByteBuffers()));
    return item;
  }

  public static void checkItems(Map<String, AttributeValue> expectedItem, Map<String,
      AttributeValue> actualItem) {
    for (String attributeType : attributeTypes) {
      boolean equals = expectedItem.get(attributeType).equals(actualItem.get(attributeType));
      String message = "Element with key [" + attributeType + "]:";
      assertEquals(message, expectedItem.get(attributeType), actualItem.get(attributeType));
    }
  }
}
