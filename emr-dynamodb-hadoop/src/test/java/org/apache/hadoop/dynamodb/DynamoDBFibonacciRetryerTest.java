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

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;

import org.joda.time.Duration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.concurrent.Callable;

@RunWith(MockitoJUnitRunner.class)
public class DynamoDBFibonacciRetryerTest {

  @Mock
  private Callable<Object> call;

  @Test
  public void testSuceedCall() throws Exception {
    DynamoDBFibonacciRetryer retryer = new DynamoDBFibonacciRetryer(Duration.standardSeconds(10));
    retryer.runWithRetry(call, null, null);
    verify(call).call();
  }

  @Test(expected = RuntimeException.class)
  public void testRetryThrottleException() throws Exception {
    AmazonServiceException ase = new AmazonServiceException("Test");
    ase.setErrorCode("ProvisionedThroughputExceededException");
    ase.setStatusCode(400);
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
    AmazonServiceException ase = new AmazonServiceException("Test");
    ase.setErrorCode("ArbitRetryableException");
    ase.setStatusCode(500);
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
    AmazonServiceException ase = new AmazonServiceException("Test");
    ase.setErrorCode("ArbitRetryableException");
    ase.setStatusCode(503);
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
    AmazonServiceException ase = new AmazonServiceException("Test");
    ase.setErrorCode("ArbitNonRetryableException");
    ase.setStatusCode(400);
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
    AmazonClientException ace = new AmazonClientException("Test");
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
