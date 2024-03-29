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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.dynamodb.DynamoDBClient;
import org.apache.hadoop.dynamodb.DynamoDBConstants;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.BillingModeSummary;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputDescription;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

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

  @Test
  public void testCalculateTargetIops() {
    JobConf jobConf = new JobConf();
    writeIopsCalculator = getWriteIopsCalculator(jobConf);

    long writeIops = writeIopsCalculator.calculateTargetIops();
    long expectedWriteIops = (long) (WRITE_CAPACITY_UNITS * THROUGHPUT_WRITE_PERCENT / Math.min
        (MAX_CONCURRENT_MAP_TASKS, TOTAL_MAP_TASKS));
    assertEquals(expectedWriteIops, writeIops);
  }

  @Test
  public void testCalculateIopsAutoscalingEnabled() {
    JobConf jobConf = new JobConf();
    jobConf.set(DynamoDBConstants.WRITE_THROUGHPUT, "500");
    writeIopsCalculator = getWriteIopsCalculator(jobConf);

    WriteIopsCalculator spyIopsCalculator = Mockito.spy(writeIopsCalculator);
    doReturn((double) 1000).when(spyIopsCalculator).getThroughput();
    long writeIops = spyIopsCalculator.calculateTargetIops();
    long expectedWriteIops = (long) (500 * THROUGHPUT_WRITE_PERCENT / Math.min
        (MAX_CONCURRENT_MAP_TASKS, TOTAL_MAP_TASKS));
    assertEquals(expectedWriteIops, writeIops);
    // Autoscaling not enabled, throughput shouldn't be fetched when user specified
    verify(spyIopsCalculator, times(0)).getThroughput();

    jobConf.set(DynamoDBConstants.WRITE_THROUGHPUT_AUTOSCALING, "true");
    long writeIops2 = spyIopsCalculator.calculateTargetIops();
    long expectedWriteIops2 = (long) (1000 * THROUGHPUT_WRITE_PERCENT / Math.min
        (MAX_CONCURRENT_MAP_TASKS, TOTAL_MAP_TASKS));
    assertEquals(expectedWriteIops2, writeIops2);
    // Autoscaling enabled, throughput should be fetched regardless of if user specified
    verify(spyIopsCalculator, times(1)).getThroughput();
  }

  private WriteIopsCalculator getWriteIopsCalculator(JobConf jobConf) {
    when(dynamoDBClient.describeTable(TABLE_NAME)).thenReturn(TableDescription.builder()
        .billingModeSummary(BillingModeSummary.builder()
                .billingMode(BillingMode.PROVISIONED)
                .build())
        .provisionedThroughput(ProvisionedThroughputDescription.builder()
                 .writeCapacityUnits(WRITE_CAPACITY_UNITS)
                 .build())
        .build());

    jobConf.setNumMapTasks(TOTAL_MAP_TASKS);
    jobConf.set("mapreduce.task.attempt.id", "attempt_m_1");
    jobConf.set(DynamoDBConstants.THROUGHPUT_WRITE_PERCENT,
        String.valueOf(THROUGHPUT_WRITE_PERCENT));
    when(jobClient.getConf()).thenReturn(jobConf);

    return new WriteIopsCalculator(jobClient, dynamoDBClient, TABLE_NAME) {
      @Override
      int calculateMaxMapTasks(int totalMapTasks) {
        return MAX_CONCURRENT_MAP_TASKS;
      }
    };
  }
}
