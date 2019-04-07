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
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.dynamodb.DynamoDBConstants;
import org.apache.hadoop.dynamodb.util.MockTimeSource;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.FromDataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class RateControllerTest {

  @DataPoints("rates")
  public static final double rates[] = {1.0, 10.0, 50.0, 100.0, 1000.0, 10000.0};
  @DataPoints("sizes")
  public static final double sizes[] = {0, 50, 100, 200, 4000, 4100, 9000, 20000, 40000, 60000};
  @DataPoints("windows")
  public static final int windows[] = {1, 5, 10, 100};
  private MockTimeSource time;

  @Before
  public void setup() {
    time = new MockTimeSource();
  }

  @Theory
  public void testHappyCase(@FromDataPoints("rates") double rate, @FromDataPoints("sizes") double
      itemSize, @FromDataPoints("windows") int windowSize) {

    RateController rateCtr = new RateController(time, rate, windowSize, itemSize);
    double rcu = 0.0;

    // Consume until we get a zero response
    RateController.RequestLimit lim;
    while ((lim = rateCtr.getNextRequestLimit()) != RateController.RequestLimit.ZERO) {
      rcu += lim.readCapacityUnits;
    }

    // Should not have consumed more IO than rate * bucket window
    double bucketCapacity = Math.max(windowSize * rateCtr.getTargetRate(), RateController
        .MIN_RCU_PER_REQ);
    assertTrue("readCapacityUnits=" + rcu + ", cap=" + bucketCapacity, rcu <= bucketCapacity);

    // Should have consumed at least cap - min_per_request
    assertTrue(rcu > bucketCapacity - RateController.MIN_RCU_PER_REQ);
  }

  @Theory
  // Here we want to test that adjustments (due to large items) are correctly handled
  public void testRateWithLargeItems(@FromDataPoints("rates") double rate, @FromDataPoints
      ("sizes") double itemSize, @FromDataPoints("windows") int windowSize) {

    final int DURATION_SEC = 600; // 10 min
    final int ADVANCE_BY_MILLIS = 50;

    time.setNanoTime(0);
    RateController rateCtr = new RateController(time, rate, windowSize, itemSize);
    int items = 0;
    double consumed = 0.0;

    // Consume until we get a zero response
    while (time.getTimeSinceMs(0) <= DURATION_SEC * 1000) {
      RateController.RequestLimit lim;
      while ((lim = rateCtr.getNextRequestLimit()) != RateController.RequestLimit.ZERO) {
        items += lim.items;

        double c = Math.ceil((lim.items * itemSize) / (DynamoDBConstants
            .BYTES_PER_READ_CAPACITY_UNIT * DynamoDBConstants
            .READ_EVENTUALLY_TO_STRONGLY_CONSISTENT_FACTOR));
        c = Math.max(c, 1.0);
        rateCtr.adjust(lim.readCapacityUnits, c, lim.items);

        consumed += c;
      }

      time.advanceByMillis(ADVANCE_BY_MILLIS);
    }

    // Verify that what was actually consumed is close to our target (i.exception., adjustment works)
    double calcRcu = (rate * (DURATION_SEC + windowSize));

    // Epsilon should be enough units to cover a large get or 10% fuss
    double delta = Math.abs(calcRcu - consumed);
    double epsilon = itemSize / DynamoDBConstants.BYTES_PER_READ_CAPACITY_UNIT;
    epsilon = Math.max(epsilon, calcRcu * 0.1);

    assertTrue("delta=" + delta + " > epsilon=" + epsilon + ", calcRcu=" + calcRcu + ", readCapacityUnits=" +
        consumed, delta < epsilon);

    // Now make sure that the item sizes fit within the RCUs
    double totalItemSize = items * itemSize;
    double totalRcuSize = consumed * DynamoDBConstants.BYTES_PER_READ_CAPACITY_UNIT * 2; // ec reads
    assertTrue("total item size (" + totalItemSize + ") > totalRcuSize (" + totalRcuSize + ")",
        totalItemSize <= totalRcuSize);
  }

  @Test
  public void emptyBatchAdjustment() {
    time.setNanoTime(0);
    RateController ctrl = new RateController(time, 10, 1, 1.0);
    assertEquals(1.0, ctrl.getAvgItemSize(), 0.01);

    // Empty batch is allowed
    ctrl.adjust(25, 0, 0);
    assertEquals(1.0, ctrl.getAvgItemSize(), 0.01);

    // The item size is updated when item count is non-zero
    ctrl.adjust(25, 10, 1);
    assertTrue(ctrl.getAvgItemSize() > 1.0);
  }

  @Test
  public void limitedAverageItemSize() {
    time.setNanoTime(0);
    RateController ctrl = new RateController(time, 10, 1, 10.0);
    assertEquals(10.0, ctrl.getAvgItemSize(), 0.01);

    // Item size upper bound
    ctrl.adjust(25, 10000, 1);
    assertEquals(400 * 1024, ctrl.getAvgItemSize(), 0.01);

    // Item size lower bound
    for (int i = 0; i < 100; i++) {
      ctrl.adjust(25, 1, 999999999);
    }
    assertEquals(1, ctrl.getAvgItemSize(), 0.01);
  }
}
