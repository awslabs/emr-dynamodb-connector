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

import java.util.List;

/**
 * Result of a scan or query request.
 */
public class PageResults<V> {

  public final List<V> items;
  public final V lastEvaluatedKey;
  public final double consumedRcu;
  public final int retries;
  public final Exception exception;

  private volatile int pos;

  public PageResults(List<V> items, V lastEvaluatedKey, double consumedRcu, int retries) {
    if (items == null) {
      throw new IllegalArgumentException("Items must not be null");
    }
    this.items = items;
    this.lastEvaluatedKey = lastEvaluatedKey;
    this.consumedRcu = consumedRcu;
    this.retries = retries;
    this.exception = null;
  }

  public PageResults(List<V> items, V lastEvaluatedKey) {
    this(items, lastEvaluatedKey, 0.0, 0);
  }

  public PageResults(Exception exception) {
    if (exception == null) {
      throw new IllegalArgumentException("exception must not be null");
    }
    this.items = null;
    this.lastEvaluatedKey = null;
    this.consumedRcu = 0;
    this.retries = 0;
    this.exception = exception;
  }

  public V next() {
    if (hasMore()) {
      return items.get(pos++);
    }
    return null;
  }

  public boolean hasMore() {
    return pos < items.size();
  }

  public boolean isFailed() {
    return exception != null;
  }
}
