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

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.Reporter;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;

import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * FIXME This class is not thread safe.
 */
public class DynamoDBFibonacciRetryer {

  private static final Log log = LogFactory.getLog(DynamoDBFibonacciRetryer.class);
  private static final Set<Integer> internalErrorStatusCodes = new HashSet<>();
  private static final Set<String> throttleErrorCodes = new HashSet<>();

  static {
    internalErrorStatusCodes.add(500);
    internalErrorStatusCodes.add(503);

    throttleErrorCodes.add("ProvisionedThroughputExceededException");
    throttleErrorCodes.add("ThrottlingException");
  }

  private final Duration retryPeriod;
  private final Random random = new Random(System.currentTimeMillis());
  private volatile boolean isShutdown;
  private int fib1 = 0;
  private int fib2 = 1;
  private int retryCount;

  public DynamoDBFibonacciRetryer(Duration retryPeriod) {
    this.retryPeriod = retryPeriod;
  }

  /*
   * This method retries with fibonacci backoff with maximum retry for 1 hour
   * period.
   */
  public <T> RetryResult<T> runWithRetry(Callable<T> callable, Reporter reporter,
      PrintCounter retryCounter) {
    fib1 = 0;
    fib2 = 1;
    retryCount = 0;
    DateTime currentTime = new DateTime(DateTimeZone.UTC);
    DateTime retryEndTime = currentTime.plus(retryPeriod);

    while (true) {
      if (isShutdown) {
        log.info("Is shut down, giving up and returning null");
        return null;
      }

      try {
        T returnObj = callable.call();
        return new RetryResult<>(returnObj, retryCount);
      } catch (Exception e) {
        handleException(retryEndTime, e, reporter, retryCounter);
      }
    }
  }

  public synchronized void shutdown() {
    if (isShutdown) {
      throw new IllegalStateException("Cannot call shutdown() more than once");
    }
    isShutdown = true;
  }

  private void handleException(DateTime retryEndTime, Exception exception, Reporter reporter,
      PrintCounter retryCounter) {
    DateTime currentTime = new DateTime(DateTimeZone.UTC);
    long maxDelay = retryEndTime.getMillis() - currentTime.getMillis();

    if (verifyRetriableException(exception) && maxDelay > 0) {
      if (exception instanceof AmazonServiceException) {
        AmazonServiceException ase = (AmazonServiceException) exception;
        if (throttleErrorCodes.contains(ase.getErrorCode())) {
          // Retry exception
        } else if (internalErrorStatusCodes.contains(ase.getStatusCode())) {
          // Retry exception
        } else {
          throw new RuntimeException(exception);
        }
      }
      incrementRetryCounter(reporter, retryCounter);
      retryCount++;
      log.warn("Retry: " + retryCount + " Exception: " + exception);
      delayOp(maxDelay);
    } else {
      if (isShutdown) {
        log.warn("Retries exceeded and caught, but is shutdown so not throwing", exception);
      } else {
        log.error("Retries exceeded or non-retryable exception, throwing: " + exception);
        throw new RuntimeException(exception);
      }
    }
  }

  private boolean verifyRetriableException(Exception exception) {
    return exception instanceof AmazonServiceException
        || exception instanceof AmazonClientException
        || exception instanceof SocketException
        || exception instanceof SocketTimeoutException;
  }

  private void incrementRetryCounter(Reporter reporter, PrintCounter retryCounter) {
    if (reporter != null) {
      if (retryCounter != null) {
        reporter.incrCounter(retryCounter.getGroup(), retryCounter.getName(), 1);
      } else {
        reporter.progress();
      }
    }
  }

  private void delayOp(long maxDelay) {
    increment();
    try {
      long computedDelay = (fib2 * 50) + random.nextInt(fib1 * 100);
      long delay = Math.min(computedDelay, maxDelay + random.nextInt(100));
      Thread.sleep(delay);
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted while retrying", e);
    }
  }

  private void increment() {
    int sum = fib1 + fib2;
    fib1 = fib2;
    fib2 = sum;
  }

  public static class RetryResult<T> {

    public final T result;
    public final int retries;

    public RetryResult(T result, int retries) {
      this.result = result;
      this.retries = retries;
    }
  }
}
