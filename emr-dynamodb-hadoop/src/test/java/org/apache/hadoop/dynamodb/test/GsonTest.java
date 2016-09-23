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

package org.apache.hadoop.dynamodb.test;

import static org.junit.Assert.assertFalse;

import com.google.gson.Gson;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import org.apache.hadoop.dynamodb.DynamoDBItemWritable;
import org.apache.hadoop.dynamodb.DynamoDBUtil;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class GsonTest {
  private static final int TASK_COUNT = 100;

  @Test
  public void testSingleThread() {
    Gson gson = DynamoDBUtil.getGson();
    Map<String, AttributeValue> item = DynamoDBTestUtils.getRandomItem();
    String json = gson.toJson(item, DynamoDBItemWritable.type);
    Map<String, AttributeValue> deserializedItem = gson.fromJson(json, DynamoDBItemWritable.type);
    DynamoDBTestUtils.checkItems(item, deserializedItem);
  }

  @Test
  public void testSpecialCase1() {
    Map<String, AttributeValue> item = DynamoDBTestUtils.getRandomItem();
    item.put("S", new AttributeValue().withS("This is a \n \0 \1 \2 line test"));

    Gson gson = DynamoDBUtil.getGson();
    String json = gson.toJson(item, DynamoDBItemWritable.type);
    Map<String, AttributeValue> deserializedItem = gson.fromJson(json, DynamoDBItemWritable.type);
    DynamoDBTestUtils.checkItems(item, deserializedItem);
  }

  @Test
  public void testMultiThread() throws InterruptedException {
    final CountDownLatch startLatch = new CountDownLatch(1);
    final CountDownLatch finishedLatch = new CountDownLatch(10);
    final AtomicBoolean failed = new AtomicBoolean(false);

    ExecutorService executor = Executors.newFixedThreadPool(10);

    for (int taskCount = 0; taskCount < TASK_COUNT; taskCount++) {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            startLatch.await();
            Gson gson = DynamoDBUtil.getGson();
            Map<String, AttributeValue> item = DynamoDBTestUtils.getRandomItem();
            String json = gson.toJson(item, DynamoDBItemWritable.type);
            Map<String, AttributeValue> deserializedItem = gson.fromJson(json,
                DynamoDBItemWritable.type);
            DynamoDBTestUtils.checkItems(item, deserializedItem);
          } catch (Throwable t) {
            failed.set(true);
          } finally {
            finishedLatch.countDown();
          }
        }
      });
    }

    startLatch.countDown();
    finishedLatch.await();
    assertFalse(failed.get());
  }
}
