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

package org.apache.hadoop.dynamodb.preader;

import org.apache.hadoop.dynamodb.util.AbstractTimeSource;

class TokenBucket {

  private final double rate; // refill rate in milliseconds
  private final double capacity;
  private final AbstractTimeSource time;

  private double tokens;
  private long lastRefill; // in nanoseconds

  TokenBucket(double refillRateInSeconds, double capacity, AbstractTimeSource time) {
    this.rate = refillRateInSeconds / 1000.0;
    this.capacity = capacity;
    this.time = time;
    this.tokens = capacity;
    this.lastRefill = time.getNanoTime();
  }

  /**
   * Refills and attempts to acquire tokens between min and max parameters.
   *
   * @return Amount of tokens acquired between `min` and `max` if tokens are available, 0 if less
   * than `min` are available.
   */
  synchronized double acquire(double minTokens, double maxTokens) {
    refill();
    double avail = Math.floor(Math.min(maxTokens, tokens));
    if (avail >= minTokens) {
      tokens -= avail;
      return avail;
    }
    return 0.0f;
  }

  /**
   * Update the number of tokens in the bucket based. Caps at the bucket capacity, but allows
   * taking the bucket into deficit.
   *
   * @param delta Amount of tokens to add or remove.
   */
  public synchronized double forceUpdate(double delta) {
    tokens = Math.min(tokens + delta, capacity);
    return tokens;
  }

  /**
   * Refill the bucket based on current time. Caps at the bucket capacity.
   */
  private void refill() {
    long nowNano = time.getNanoTime();
    long deltaMs = time.getTimeDeltaMs(lastRefill, nowNano);
    if (deltaMs < 0) {
      return;
    }

    tokens = Math.min(tokens + deltaMs * rate, capacity);
    lastRefill = nowNano;

  }
}
