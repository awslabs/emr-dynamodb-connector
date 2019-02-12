/**
 * Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file
 * except in compliance with the License. A copy of the License is located at
 *     http://aws.amazon.com/apache2.0/
 * or in the "LICENSE.TXT" file accompanying this file. This file is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under the License.
 */

package org.apache.hadoop.dynamodb.write;

import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputDescription;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.google.common.base.Strings;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.dynamodb.DynamoDBClient;
import org.apache.hadoop.dynamodb.DynamoDBConstants;
import org.apache.hadoop.dynamodb.DynamoDBUtil;
import org.apache.hadoop.dynamodb.IopsCalculator;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;

public class WriteIopsCalculator implements IopsCalculator {

  private static final Log log = LogFactory.getLog(WriteIopsCalculator.class);

  private final DynamoDBClient dynamoDBClient;
  private final JobClient jobClient;
  private final JobConf jobConf;
  private final String tableName;

  private final int maxParallelTasks;
  private final double throughputPercent;

  public WriteIopsCalculator(JobClient jobClient, DynamoDBClient dynamoDBClient, String tableName) {
    this.jobConf = (JobConf) jobClient.getConf();
    this.jobClient = jobClient;

    this.dynamoDBClient = dynamoDBClient;
    this.tableName = tableName;
    this.throughputPercent = Double.parseDouble(jobConf
        .get(DynamoDBConstants.THROUGHPUT_WRITE_PERCENT,
            DynamoDBConstants.DEFAULT_THROUGHPUT_PERCENTAGE));

    log.info("Table name: " + tableName);
    log.info("Throughput percent: " + throughputPercent);

    String taskId = jobConf.get("mapreduce.task.attempt.id");
    log.info("Task Id: " + taskId);
    if (Strings.isNullOrEmpty(taskId)) {
      // Running in local mode
      maxParallelTasks = 1;
    } else {
      int totalMapTasks = jobConf.getNumMapTasks();
      log.info("Total map tasks: " + totalMapTasks);

      maxParallelTasks = Math.min(calculateMaxMapTasks(totalMapTasks), totalMapTasks);
      log.info("Max parallel map tasks: " + maxParallelTasks);
    }
  }

  public long calculateTargetIops() {
    double configuredThroughput = Math.floor(Double.parseDouble(
        jobConf.get(DynamoDBConstants.WRITE_THROUGHPUT, String.valueOf(getThroughput())))
        * throughputPercent);
    long throughputPerTask = Math.max((long) (configuredThroughput / maxParallelTasks), 1);

    log.info("Throughput per task for table " + tableName + " : " + throughputPerTask);
    return throughputPerTask;
  }

  int calculateMaxMapTasks(int totalMapTasks) {
    try {
      return DynamoDBUtil.calcMaxMapTasks(jobClient);
    } catch (IOException e) {
      log.warn("Exception calculating max map tasks", e);
    }
    return totalMapTasks;
  }

  private double getThroughput() {
    TableDescription tableDescription = dynamoDBClient.describeTable(tableName);
    if (tableDescription.getBillingModeSummary() == null || tableDescription.getBillingModeSummary()
        .getBillingMode().equalsIgnoreCase(DynamoDBConstants.BILLING_MODE_PROVISIONED)) {
      ProvisionedThroughputDescription provisionedThroughput =
          tableDescription.getProvisionedThroughput();
      return provisionedThroughput.getWriteCapacityUnits();
    }
    return DynamoDBConstants.DEFAULT_CAPACITY_FOR_ON_DEMAND;
  }

}
