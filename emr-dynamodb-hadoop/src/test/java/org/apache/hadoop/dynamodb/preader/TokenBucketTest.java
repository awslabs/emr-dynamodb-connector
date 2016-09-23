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

package org.apache.hadoop.dynamodb.preader;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.dynamodb.util.MockTimeSource;
import org.junit.Test;

public class TokenBucketTest {

  private static final double EPSILON = 0.001;
  private static final double MIN = 1.0;

  @Test
  public void testRefill() {
    final double RATE = 10.0;
    final double CAPACITY = RATE * 3;

    MockTimeSource time = new MockTimeSource();
    TokenBucket bucket = new TokenBucket(RATE, CAPACITY, time);
    double tokens;

    // Should not be successful
    tokens = bucket.acquire(CAPACITY * 10, Double.MAX_VALUE);
    assertEquals(0, tokens, EPSILON);

    // Should empty the bucket
    tokens = bucket.acquire(CAPACITY, Double.MAX_VALUE);
    assertEquals(CAPACITY, tokens, EPSILON);

    // Should have nothing left
    tokens = bucket.acquire(MIN, Double.MAX_VALUE);
    assertEquals(0, tokens, EPSILON);

    // Lets advance by half a second, should be half full
    time.advanceByMillis(500);
    tokens = bucket.acquire(MIN, Double.MAX_VALUE);
    assertEquals(RATE / 2, tokens, EPSILON);

    // Lets advance by more than what it takes to overflow the bucket,
    // should still only have CAPACITY
    time.advanceByMillis((long) (CAPACITY / RATE) * 1000 * 3); // 3 times the millis required to
    // refill
    tokens = bucket.acquire(MIN, Double.MAX_VALUE);
    assertEquals(CAPACITY, tokens, EPSILON);
  }

}
