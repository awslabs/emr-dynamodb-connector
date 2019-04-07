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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.dynamodb.DynamoDBConstants;
import org.apache.hadoop.dynamodb.util.AbstractTimeSource;

public class RateController {

  static final double MIN_RCU_PER_REQ = 1;
  private static final double MAX_RCU_PER_REQ = 25;
  private static final Log log = LogFactory.getLog(RateController.class);
  private static final double ITEM_SIZE_SMOOTH_FACTOR = 0.7;

  // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html#limits-items
  private static final double MIN_ITEM_SIZE = 1.0;
  private static final double MAX_ITEM_SIZE = 400 * 1024;

  private final double targetRate;
  private final TokenBucket bucket;
  private double avgItemSizeBytes;

  public RateController(AbstractTimeSource time, double targetRate, int windowSize, double
      avgItemSizeBytes) {
    this.targetRate = targetRate;
    this.avgItemSizeBytes = Math.min(Math.max(avgItemSizeBytes, MIN_ITEM_SIZE), MAX_ITEM_SIZE);;

    double capacity = Math.max(targetRate * windowSize, MIN_RCU_PER_REQ);
    this.bucket = new TokenBucket(targetRate, capacity, time);

    log.info("Rate controller initialized. target rate=" + targetRate + ", bucket capacity="
        + capacity);
  }

  RequestLimit getNextRequestLimit() {
    double rcu = bucket.acquire(MIN_RCU_PER_REQ, MAX_RCU_PER_REQ);
    if (rcu < MIN_RCU_PER_REQ) {
      return RequestLimit.ZERO;
    }

    double items = getBytesFromRcu(rcu) / avgItemSizeBytes;

    // Round down, but always to at least one item
    items = Math.max(1, Math.floor(items));

    return new RequestLimit((int) items, rcu);
  }

  void adjust(double permittedReadUnits, double consumedReadUnits, int items) {
    // Update average item size
    double oldAvg = avgItemSizeBytes;
    if (items > 0) {
      avgItemSizeBytes = (avgItemSizeBytes * ITEM_SIZE_SMOOTH_FACTOR)
          + estimateAvgItemSize(consumedReadUnits, items) * (1.0 - ITEM_SIZE_SMOOTH_FACTOR);
      avgItemSizeBytes = Math.min(Math.max(avgItemSizeBytes, MIN_ITEM_SIZE), MAX_ITEM_SIZE);
    }

    log.debug("report: permitted=" + permittedReadUnits + ", consumed=" + consumedReadUnits + ", "
        + "items=" + items + ", avg from= " + oldAvg + " to " + avgItemSizeBytes);

    // Adjust token bucket, if we ended up consuming more/less than
    // permitted
    double adjustment = permittedReadUnits - consumedReadUnits;
    if (Math.abs(adjustment) > 0.1) {
      double tokens = bucket.forceUpdate(adjustment);
      log.debug("Adjusting token bucket, adjustment= " + adjustment + ", permittedReadUnits="
          + permittedReadUnits + ", consumedReadUnits=" + consumedReadUnits + ", avgItem="
          + avgItemSizeBytes + ", tokens=" + tokens);
    }
  }

  double getTargetRate() {
    return targetRate;
  }

  double getAvgItemSize() {
    return avgItemSizeBytes;
  }

  private double estimateAvgItemSize(double consumedReadUnits, int items) {
    return getBytesFromRcu(consumedReadUnits) / items;
  }

  private double getBytesFromRcu(double rcu) {
    return DynamoDBConstants.BYTES_PER_READ_CAPACITY_UNIT * DynamoDBConstants
        .READ_EVENTUALLY_TO_STRONGLY_CONSISTENT_FACTOR * rcu;
  }

  static class RequestLimit {

    static final RequestLimit ZERO = new RequestLimit(0, 0.0);
    public final int items;
    final double readCapacityUnits;

    RequestLimit(int items, double readCapacityUnits) {
      this.items = items;
      this.readCapacityUnits = readCapacityUnits;
    }
  }

}
