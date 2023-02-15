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

import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.joda.time.Duration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.concurrent.Callable;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.http.SdkHttpResponse;

@RunWith(MockitoJUnitRunner.class)
public class DynamoDBFibonacciRetryerTest {

  @Mock
  private Callable<Object> call;

  @Test
  public void testSucceedCall() throws Exception {
    DynamoDBFibonacciRetryer retryer = new DynamoDBFibonacciRetryer(Duration.standardSeconds(10));
    retryer.runWithRetry(call, null, null);
    verify(call).call();
  }

  @Test(expected = RuntimeException.class)
  public void testRetryThrottleException() throws Exception {
    AwsServiceException ase = AwsServiceException.builder()
        .message("Test")
        .awsErrorDetails(AwsErrorDetails.builder()
            .errorCode("ProvisionedThroughputExceededException")
            .sdkHttpResponse(SdkHttpResponse.builder()
                .statusCode(400)
                .build())
            .build())
        .build();
    when(call.call()).thenThrow(ase);
    DynamoDBFibonacciRetryer retryer = new DynamoDBFibonacciRetryer(Duration.standardSeconds(10));

    try {
      retryer.runWithRetry(call, null, null);
    } finally {
      verify(call, atLeast(2)).call();
      verify(call, atMost(15)).call();
    }
  }

  @Test(expected = RuntimeException.class)
  public void testRetryableASEException() throws Exception {
    AwsServiceException ase = AwsServiceException.builder()
        .message("Test")
        .awsErrorDetails(AwsErrorDetails.builder()
            .errorCode("ArbitRetryableException")
            .sdkHttpResponse(SdkHttpResponse.builder()
                .statusCode(500)
                .build())
            .build())
        .build();
    when(call.call()).thenThrow(ase);
    DynamoDBFibonacciRetryer retryer = new DynamoDBFibonacciRetryer(Duration.standardSeconds(10));

    try {
      retryer.runWithRetry(call, null, null);
    } finally {
      verify(call, atLeastOnce()).call();
      verify(call, atMost(15)).call();
    }
  }

  @Test(expected = RuntimeException.class)
  public void testRetryableASEException2() throws Exception {
    AwsServiceException ase = AwsServiceException.builder()
        .message("Test")
        .awsErrorDetails(AwsErrorDetails.builder()
            .errorCode("ArbitRetryableException")
            .sdkHttpResponse(SdkHttpResponse.builder()
                .statusCode(503)
                .build())
            .build())
        .build();
    when(call.call()).thenThrow(ase);
    DynamoDBFibonacciRetryer retryer = new DynamoDBFibonacciRetryer(Duration.standardSeconds(10));

    try {
      retryer.runWithRetry(call, null, null);
    } finally {
      verify(call, atLeast(2)).call();
      verify(call, atMost(15)).call();
    }
  }

  @Test(expected = RuntimeException.class)
  public void testNonRetryableASEException() throws Exception {
    AwsServiceException ase = AwsServiceException.builder()
        .message("Test")
        .awsErrorDetails(AwsErrorDetails.builder()
            .errorCode("ArbitNonRetryableException")
            .sdkHttpResponse(SdkHttpResponse.builder()
                .statusCode(400)
                .build())
            .build())
        .build();
    when(call.call()).thenThrow(ase);
    DynamoDBFibonacciRetryer retryer = new DynamoDBFibonacciRetryer(Duration.standardSeconds(10));

    try {
      retryer.runWithRetry(call, null, null);
    } finally {
      verify(call).call();
    }
  }

  @Test(expected = RuntimeException.class)
  public void testRetryACEException() throws Exception {
    SdkException ace = SdkException.builder().message("Test").build();
    when(call.call()).thenThrow(ace);
    DynamoDBFibonacciRetryer retryer = new DynamoDBFibonacciRetryer(Duration.standardSeconds(10));

    try {
      retryer.runWithRetry(call, null, null);
    } finally {
      verify(call, atLeast(2)).call();
      verify(call, atMost(15)).call();
    }
  }

  @Test(expected = RuntimeException.class)
  public void testNonRetryIOException() throws Exception {
    IOException ioe = new IOException("Test");
    when(call.call()).thenThrow(ioe);
    DynamoDBFibonacciRetryer retryer = new DynamoDBFibonacciRetryer(Duration.standardSeconds(10));

    try {
      retryer.runWithRetry(call, null, null);
    } finally {
      verify(call).call();
    }
  }
}
