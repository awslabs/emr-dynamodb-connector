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
import static org.mockito.Mockito.when;
import org.apache.hadoop.dynamodb.DynamoDBClient;
import org.apache.hadoop.dynamodb.DynamoDBConstants;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
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

  @Before
  public void setup() {
    when(dynamoDBClient.describeTable(TABLE_NAME)).thenReturn(TableDescription.builder()
        .billingModeSummary(BillingModeSummary.builder()
            .billingMode(DynamoDBConstants.BILLING_MODE_PROVISIONED)
            .build())
        .provisionedThroughput(ProvisionedThroughputDescription.builder()
            .readCapacityUnits(READ_CAPACITY_UNITS)
            .build())
        .build());

    JobConf jobConf = new JobConf();
    jobConf.set(DynamoDBConstants.THROUGHPUT_READ_PERCENT, String.valueOf(THROUGHPUT_READ_PERCENT));
    when(jobClient.getConf()).thenReturn(jobConf);

    readIopsCalculator = new ReadIopsCalculator(jobClient, dynamoDBClient, TABLE_NAME,
        TOTAL_SEGMETNS, LOCAL_SEGMENTS);
  }

  @Test
  public void testCalculateTargetIops() {
    long readIops = readIopsCalculator.calculateTargetIops();
    long expectedReadIops = (long) (READ_CAPACITY_UNITS * THROUGHPUT_READ_PERCENT *
        LOCAL_SEGMENTS / TOTAL_SEGMETNS);
    assertEquals(expectedReadIops, readIops);
  }

}
