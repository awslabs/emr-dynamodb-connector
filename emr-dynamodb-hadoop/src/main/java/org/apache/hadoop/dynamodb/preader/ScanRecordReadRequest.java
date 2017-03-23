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

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

import org.apache.hadoop.dynamodb.DynamoDBFibonacciRetryer.RetryResult;
import org.apache.hadoop.dynamodb.preader.RateController.RequestLimit;

import java.util.Map;

public class ScanRecordReadRequest extends AbstractRecordReadRequest {

  public ScanRecordReadRequest(AbstractReadManager readMgr, DynamoDBRecordReaderContext context,
      int segment, Map<String, AttributeValue> lastEvaluatedKey) {
    super(readMgr, context, segment, lastEvaluatedKey);
  }

  @Override
  protected AbstractRecordReadRequest buildNextReadRequest(PageResults<Map<String,
      AttributeValue>> pageResults) {
    return new ScanRecordReadRequest(readMgr, context, segment, pageResults.lastEvaluatedKey);
  }

  @Override
  protected PageResults<Map<String, AttributeValue>> fetchPage(RequestLimit lim) {
    // Read from DynamoDB
    RetryResult<ScanResult> retryResult = context.getClient().scanTable(tableName, null, segment,
        context.getSplit().getTotalSegments(), lastEvaluatedKey, lim.items, context.getReporter());

    ScanResult result = retryResult.result;
    int retries = retryResult.retries;

    double consumedCapacityUnits = 0.0;
    if (result.getConsumedCapacity() != null) {
      consumedCapacityUnits = result.getConsumedCapacity().getCapacityUnits();
    }
    return new PageResults<>(result.getItems(), result.getLastEvaluatedKey(), consumedCapacityUnits,
        retries);
  }
}
