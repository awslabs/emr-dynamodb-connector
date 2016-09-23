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
import com.amazonaws.services.dynamodbv2.model.QueryResult;

import org.apache.hadoop.dynamodb.DynamoDBFibonacciRetryer.RetryResult;
import org.apache.hadoop.dynamodb.preader.RateController.RequestLimit;

import java.util.Map;

public class QueryRecordReadRequest extends AbstractRecordReadRequest {

  public QueryRecordReadRequest(AbstractReadManager readMgr, DynamoDBRecordReaderContext context,
      Map<String, AttributeValue> lastEvaluatedKey) {
    super(readMgr, context, 0 /* segment */, lastEvaluatedKey);
  }

  @Override
  protected AbstractRecordReadRequest buildNextReadRequest(PageResults<Map<String,
      AttributeValue>> pageResults) {
    return new QueryRecordReadRequest(readMgr, context, pageResults.lastEvaluatedKey);
  }

  @Override
  protected PageResults<Map<String, AttributeValue>> fetchPage(RequestLimit lim) {
    // Read from DynamoDB
    RetryResult<QueryResult> retryResult = context.getClient().queryTable(tableName, context
        .getSplit().getFilterPushdown(), lastEvaluatedKey, lim.items, context.getReporter());

    QueryResult result = retryResult.result;
    int retries = retryResult.retries;

    return new PageResults<>(result.getItems(), result.getLastEvaluatedKey(), result
        .getConsumedCapacity().getCapacityUnits(), retries);
  }
}
