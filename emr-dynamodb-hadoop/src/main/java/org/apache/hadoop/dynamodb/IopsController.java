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

import static org.apache.hadoop.dynamodb.DynamoDBConstants.DEFAULT_AVERAGE_ITEM_SIZE_IN_BYTES;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;

import java.util.Random;

public class IopsController {

  private final IopsCalculator iopsCalculator;
  private final Duration iopsUpdateDuration;
  private final DynamoDBOperationType operationType;
  private DateTime lastUpdateTime;
  private double targetIops;
  private double targetItemsPerSecond;

  public IopsController(IopsCalculator iopsCalculator, double averageItemSizeInBytes,
      DynamoDBOperationType operationType) {
    this.iopsCalculator = iopsCalculator;
    this.operationType = operationType;

    iopsUpdateDuration = getUpdateDuration();
    lastUpdateTime = getCurrentTime();
    targetIops = iopsCalculator.calculateTargetIops();

    if (averageItemSizeInBytes < 1) {
      averageItemSizeInBytes = DEFAULT_AVERAGE_ITEM_SIZE_IN_BYTES;
    }
    double bytesPerIO = (DynamoDBOperationType.READ == operationType) ? DynamoDBConstants
        .BYTES_PER_READ_CAPACITY_UNIT : DynamoDBConstants.BYTES_PER_WRITE_CAPACITY_UNIT;
    double itemsPerIO = bytesPerIO / averageItemSizeInBytes;
    targetItemsPerSecond = calculateTargetItemsPerSecond(operationType, targetIops, itemsPerIO);
  }

  public static double calculateTargetItemsPerSecond(DynamoDBOperationType operationType, double
      targetIops, double itemsPerIO) {
    // Minimum required IOPS for write is 1.0 even for small items
    if (DynamoDBOperationType.WRITE.equals(operationType) && itemsPerIO > 1.0) {
      itemsPerIO = 1.0;
    }

    double result = targetIops * itemsPerIO;
    if (result < 1) {
      result = 1;
    }
    return result;
  }

  public long getTargetItemsPerSecond() {
    return (long) targetItemsPerSecond;
  }

  public void update(long itemsPerSecond, double iopsConsumed) {
    DateTime currentTime = getCurrentTime();
    if (currentTime.isAfter(lastUpdateTime.plus(iopsUpdateDuration))) {
      lastUpdateTime = currentTime;
      targetIops = iopsCalculator.calculateTargetIops();
    }

    if (itemsPerSecond != 0 && iopsConsumed != 0) {
      double itemsPerIO = itemsPerSecond / iopsConsumed;
      targetItemsPerSecond = calculateTargetItemsPerSecond(operationType, targetIops, itemsPerIO);
    }
  }

  /**
   * This method generates a random duration between 5 and 10 minutes. This is the duration used
   * to get the updated capacity unit information from the table.
   */
  private Duration getUpdateDuration() {
    Random random = new Random(System.currentTimeMillis());
    long randomDuration = random.nextInt(5 * 60 * 1000);
    return Duration.standardMinutes(5).plus(randomDuration);
  }

  private DateTime getCurrentTime() {
    return new DateTime(DateTimeZone.UTC);
  }

}
