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

package org.apache.hadoop.dynamodb.read;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.dynamodb.DynamoDBClient;
import org.apache.hadoop.dynamodb.DynamoDBConstants;
import org.apache.hadoop.dynamodb.IopsCalculator;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputDescription;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

public class ReadIopsCalculator implements IopsCalculator {

  private static final Log log = LogFactory.getLog(ReadIopsCalculator.class);

  private final DynamoDBClient dynamoDBClient;
  private final JobClient jobClient;
  private final JobConf jobConf;
  private final String tableName;

  private final double throughputPercent;
  private final int totalSegments;
  private final int localSegments;

  public ReadIopsCalculator(JobClient jobClient, DynamoDBClient dynamoDBClient, String tableName,
      int totalSegments, int localSegments) {
    this.jobConf = (JobConf) jobClient.getConf();
    this.jobClient = jobClient;

    this.dynamoDBClient = dynamoDBClient;
    this.tableName = tableName;
    this.totalSegments = totalSegments;
    this.localSegments = localSegments;

    this.throughputPercent = Double.parseDouble(jobConf.get(DynamoDBConstants
        .THROUGHPUT_READ_PERCENT, DynamoDBConstants.DEFAULT_THROUGHPUT_PERCENTAGE));

    log.info("Table name: " + tableName);
    log.info("Throughput percent: " + throughputPercent);
  }

  public long calculateTargetIops() {
    double configuredThroughput;
    // Always fetch throughput from DDB if auto-scaling is enabled or not specified
    if (Boolean.parseBoolean(jobConf.get(DynamoDBConstants.READ_THROUGHPUT_AUTOSCALING))
        || jobConf.get(DynamoDBConstants.READ_THROUGHPUT) == null) {
      configuredThroughput = getThroughput();
    } else {
      configuredThroughput = Double.parseDouble(jobConf.get(DynamoDBConstants.READ_THROUGHPUT));
    }
    double calculatedThroughput = Math.floor(configuredThroughput * throughputPercent);

    long throughputPerTask = Math.max((long) (calculatedThroughput / totalSegments
        * localSegments), 1);

    log.info("Throughput per task for table " + tableName + " : " + throughputPerTask);
    return throughputPerTask;
  }

  protected double getThroughput() {
    TableDescription tableDescription = dynamoDBClient.describeTable(tableName);
    if (tableDescription.billingModeSummary() == null
        || tableDescription.billingModeSummary().billingMode() == BillingMode.PROVISIONED) {
      ProvisionedThroughputDescription provisionedThroughput = tableDescription
          .provisionedThroughput();
      return provisionedThroughput.readCapacityUnits();
    }
    return DynamoDBConstants.DEFAULT_CAPACITY_FOR_ON_DEMAND;
  }
}
