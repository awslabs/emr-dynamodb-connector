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
public class ReadIopsCalculatorTest {
  private static final String TABLE_NAME = "Test";
  private static final long READ_CAPACITY_UNITS = 2000;
  private static final double THROUGHPUT_READ_PERCENT = 0.8;
  private static final int LOCAL_SEGMENTS = 2;
  private static final int TOTAL_SEGMETNS = 9;

  @Mock
  private DynamoDBClient dynamoDBClient;
  @Mock
  private JobClient jobClient;

  private ReadIopsCalculator readIopsCalculator;

  @Test
  public void testCalculateTargetIops() {
    JobConf jobConf = new JobConf();
    readIopsCalculator = getReadIopsCalculator(jobConf);

    long readIops = readIopsCalculator.calculateTargetIops();
    long expectedReadIops = (long) (READ_CAPACITY_UNITS * THROUGHPUT_READ_PERCENT *
        LOCAL_SEGMENTS / TOTAL_SEGMETNS);
    assertEquals(expectedReadIops, readIops);
  }

  @Test
  public void testCalculateIopsAutoscalingEnabled() {
    JobConf jobConf = new JobConf();
    jobConf.set(DynamoDBConstants.READ_THROUGHPUT, "500");
    readIopsCalculator = getReadIopsCalculator(jobConf);

    ReadIopsCalculator spyIopsCalculator = Mockito.spy(readIopsCalculator);
    doReturn((double) 1000).when(spyIopsCalculator).getThroughput();
    long readIops = spyIopsCalculator.calculateTargetIops();
    long expectedReadIops = (long) (500 * THROUGHPUT_READ_PERCENT *
        LOCAL_SEGMENTS / TOTAL_SEGMETNS);
    assertEquals(expectedReadIops, readIops);
    // Autoscaling not enabled, throughput shouldn't be fetched when user specified
    verify(spyIopsCalculator, times(0)).getThroughput();

    jobConf.set(DynamoDBConstants.READ_THROUGHPUT_AUTOSCALING, "true");
    long readIops2 = spyIopsCalculator.calculateTargetIops();
    long expectedReadIops2 = (long) (1000 * THROUGHPUT_READ_PERCENT *
        LOCAL_SEGMENTS / TOTAL_SEGMETNS);
    assertEquals(expectedReadIops2, readIops2);
    // Autoscaling enabled, throughput should be fetched regardless of if user specified
    verify(spyIopsCalculator, times(1)).getThroughput();
  }

  private ReadIopsCalculator getReadIopsCalculator(JobConf jobConf) {
    when(dynamoDBClient.describeTable(TABLE_NAME)).thenReturn(TableDescription.builder()
        .billingModeSummary(BillingModeSummary.builder()
            .billingMode(BillingMode.PROVISIONED)
            .build())
        .provisionedThroughput(ProvisionedThroughputDescription.builder()
            .readCapacityUnits(READ_CAPACITY_UNITS)
            .build())
        .build());

    jobConf.set(DynamoDBConstants.THROUGHPUT_READ_PERCENT, String.valueOf(THROUGHPUT_READ_PERCENT));
    doReturn(jobConf).when(jobClient).getConf();

    return new ReadIopsCalculator(jobClient, dynamoDBClient, TABLE_NAME,
        TOTAL_SEGMETNS, LOCAL_SEGMENTS);
  }
}
