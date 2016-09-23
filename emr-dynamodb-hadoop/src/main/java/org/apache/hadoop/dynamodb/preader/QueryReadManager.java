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

public class QueryReadManager extends AbstractReadManager {

  public QueryReadManager(RateController rateController, AbstractTimeSource time,
      DynamoDBRecordReaderContext context) {
    super(rateController, time, context);
  }

  @Override
  protected void initializeReadRequests() {
    int totalSegments = context.getSplit().getTotalSegments();
    if (totalSegments != 1) {
      String errorMsg = "Expect 1 segment for query (received " + context.getSplit()
          .getTotalSegments() + ")";
      log.error(errorMsg);
      throw new RuntimeException(errorMsg);
    }
    segmentsRemaining.set(totalSegments);

    enqueueReadRequestToTail(new QueryRecordReadRequest(this, context, null /* lastEvaluatedKey
    */));
  }
}
