package org.apache.hadoop.dynamodb.preader;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.when;

import org.apache.hadoop.dynamodb.DynamoDBClient;
import org.apache.hadoop.dynamodb.DynamoDBConstants;
import org.apache.hadoop.dynamodb.DynamoDBFibonacciRetryer.RetryResult;
import org.apache.hadoop.dynamodb.preader.RateController.RequestLimit;
import org.apache.hadoop.dynamodb.split.DynamoDBSegmentsSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.Map;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

@RunWith(MockitoJUnitRunner.class)
public final class ScanRecordReadRequestTest {

  @Mock
  DynamoDBRecordReaderContext context;
  @Mock
  DynamoDBClient client;
  @Mock
  Reporter reporter;

  @Test
  public void fetchPageReturnsZeroConsumedCapacityWhenResultsConsumedCapacityIsNull() {
    RetryResult<ScanResponse> stubbedResult = new RetryResult<>(
        ScanResponse.builder()
            .consumedCapacity((ConsumedCapacity) null)
            .items(Collections.emptyList())
            .build(),
        0);

    JobConf jobConf = new JobConf();
    jobConf.set(DynamoDBConstants.INPUT_TABLE_NAME, "test-table");

    when(context.getClient()).thenReturn(client);
    when(context.getConf()).thenReturn(jobConf);
    when(context.getSplit()).thenReturn(new DynamoDBSegmentsSplit());
    when(context.getReporter()).thenReturn(reporter);
    
    when(client.scanTable(
        anyString(),
        isNull(),
        anyInt(),
        anyInt(),
        isNull(),
        anyLong(),
        any(Reporter.class))
    ).thenReturn(stubbedResult);

    ScanReadManager readManager = Mockito.mock(ScanReadManager.class);
    ScanRecordReadRequest readRequest = new ScanRecordReadRequest(readManager, context, 0, null);
    PageResults<Map<String, AttributeValue>> pageResults =
        readRequest.fetchPage(new RequestLimit(0, 0));
    assertEquals(0.0, pageResults.consumedRcu, 0.0);
  }
}
