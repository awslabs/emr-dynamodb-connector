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

import org.apache.hadoop.dynamodb.DynamoDBConstants;
import org.apache.hadoop.dynamodb.preader.RateController.RequestLimit;

import java.util.Map;

public abstract class AbstractRecordReadRequest {

  protected final AbstractReadManager readMgr;
  protected final DynamoDBRecordReaderContext context;

  protected final Map<String, AttributeValue> lastEvaluatedKey;
  protected final int segment;
  protected final String tableName;

  public AbstractRecordReadRequest(AbstractReadManager readMgr, DynamoDBRecordReaderContext
      context, int segment, Map<String, AttributeValue> lastEvaluatedKey) {
    this.readMgr = readMgr;
    this.context = context;
    this.tableName = context.getConf().get(DynamoDBConstants.INPUT_TABLE_NAME);
    this.segment = segment;
    this.lastEvaluatedKey = lastEvaluatedKey;
  }

  public void read(RequestLimit lim) {
    signalProgress();
    PageResults<Map<String, AttributeValue>> pageResults = readNextPage(lim);
    addPageToMultiplexer(pageResults);
    reportMetrics(lim, pageResults);
    enqueueNextPageOrCompleteSegment(pageResults);
  }

  private void reportMetrics(RequestLimit lim,
      PageResults<Map<String, AttributeValue>> pageResults) {
    if (!pageResults.isFailed()) {
      readMgr.report(lim.readCapacityUnits, pageResults.consumedRcu, pageResults.items.size(),
          pageResults.retries);
    }
  }

  private void signalProgress() {
    context.getReporter().progress();
  }

  protected abstract AbstractRecordReadRequest buildNextReadRequest(
      PageResults<Map<String, AttributeValue>> pageResults);

  protected abstract PageResults<Map<String, AttributeValue>> fetchPage(RequestLimit lim);

  private PageResults<Map<String, AttributeValue>> readNextPage(RequestLimit lim) {
    try {
      return fetchPage(lim);
    } catch (Exception e) {
      return new PageResults<>(e);
    }
  }

  private void addPageToMultiplexer(PageResults<Map<String, AttributeValue>> pageResults) {
    if (!context.getPageResultMultiplexer().addPageResults(pageResults)) {
      throw new RuntimeException("Interrupted while adding to the page mux. Aborting.");
    }
  }

  private void enqueueNextPageOrCompleteSegment(
      PageResults<Map<String, AttributeValue>> pageResults) {
    if (pageResults.lastEvaluatedKey != null) {
      // Schedule the next page read for this segment
      readMgr.enqueueReadRequestToTail(buildNextReadRequest(pageResults));
    } else {
      // Or mark it as complete
      readMgr.markSegmentComplete(segment);
    }
  }
}
