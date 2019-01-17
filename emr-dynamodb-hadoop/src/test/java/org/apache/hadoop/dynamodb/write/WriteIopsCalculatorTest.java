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

package org.apache.hadoop.dynamodb.write;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import com.amazonaws.services.dynamodbv2.model.BillingModeSummary;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputDescription;
import com.amazonaws.services.dynamodbv2.model.TableDescription;

import org.apache.hadoop.dynamodb.DynamoDBClient;
import org.apache.hadoop.dynamodb.DynamoDBConstants;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class WriteIopsCalculatorTest {
  private static final String TABLE_NAME = "Test";
  private static final int TOTAL_MAP_TASKS = 10;
  private static final int MAX_CONCURRENT_MAP_TASKS = 20;
  private static final double THROUGHPUT_WRITE_PERCENT = 0.8;
  private static final long WRITE_CAPACITY_UNITS = 1000;

  @Mock
  private DynamoDBClient dynamoDBClient;
  @Mock
  private JobClient jobClient;

  private WriteIopsCalculator writeIopsCalculator;

  @Before
  public void setup() {
    when(dynamoDBClient.describeTable(TABLE_NAME)).thenReturn(new TableDescription()
        .withBillingModeSummary(
            new BillingModeSummary().withBillingMode(DynamoDBConstants.BILLING_MODE_PROVISIONED))
        .withProvisionedThroughput(
            new ProvisionedThroughputDescription().withWriteCapacityUnits(WRITE_CAPACITY_UNITS)));

    JobConf jobConf = new JobConf();
    jobConf.setNumMapTasks(TOTAL_MAP_TASKS);
    jobConf.set("mapreduce.task.attempt.id", "attempt_m_1");
    jobConf.set(DynamoDBConstants.THROUGHPUT_WRITE_PERCENT, String.valueOf
        (THROUGHPUT_WRITE_PERCENT));
    when(jobClient.getConf()).thenReturn(jobConf);

    writeIopsCalculator = new WriteIopsCalculator(jobClient, dynamoDBClient, TABLE_NAME) {
      @Override
      int calculateMaxMapTasks(int totalMapTasks) {
        return MAX_CONCURRENT_MAP_TASKS;
      }
    };
  }

  @Test
  public void testCalculateTargetIops() {
    long writeIops = writeIopsCalculator.calculateTargetIops();
    long expectedWriteIops = (long) (WRITE_CAPACITY_UNITS * THROUGHPUT_WRITE_PERCENT / Math.min
        (MAX_CONCURRENT_MAP_TASKS, TOTAL_MAP_TASKS));
    assertEquals(expectedWriteIops, writeIops);
  }
}
