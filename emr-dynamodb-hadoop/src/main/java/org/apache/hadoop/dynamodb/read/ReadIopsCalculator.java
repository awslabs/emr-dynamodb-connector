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

import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputDescription;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.dynamodb.DynamoDBClient;
import org.apache.hadoop.dynamodb.DynamoDBConstants;
import org.apache.hadoop.dynamodb.IopsCalculator;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

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
    double configuredThroughput = Math.floor(Double.parseDouble(
        jobConf.get(DynamoDBConstants.READ_THROUGHPUT, String.valueOf(getThroughput()))) * throughputPercent);
    long throughputPerTask = Math.max((long) (configuredThroughput / totalSegments
        * localSegments), 1);

    log.info("Throughput per task for table " + tableName + " : " + throughputPerTask);
    return throughputPerTask;
  }

  private double getThroughput() {
    TableDescription tableDescription = dynamoDBClient.describeTable(tableName);
    if (tableDescription.getBillingModeSummary() == null ||
        tableDescription.getBillingModeSummary().getBillingMode()
            .equalsIgnoreCase(DynamoDBConstants.BILLING_MODE_PROVISIONED)) {
      ProvisionedThroughputDescription provisionedThroughput = tableDescription
          .getProvisionedThroughput();
      return provisionedThroughput.getReadCapacityUnits();
    }
    return DynamoDBConstants.DEFAULT_CAPACITY_FOR_ON_DEMAND;
  }
}
