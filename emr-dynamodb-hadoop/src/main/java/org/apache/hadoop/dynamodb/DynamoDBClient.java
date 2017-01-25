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
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.*;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.*;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.primitives.Ints;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.dynamodb.filter.DynamoDBQueryFilter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;
import org.joda.time.Duration;

import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

public class DynamoDBClient {

  private static final Log log = LogFactory.getLog(DynamoDBClient.class);

  private static final int DEFAULT_RETRY_DURATION = 10;
  private static final int MAX_ALLOWABLE_BYTE_SIZE = 512 * 1024;
  private static final long MAX_BACKOFF_IN_MILLISECONDS = 1000 * 3;
  private static final int MAX_WRITE_BATCH_SIZE = 25;
  private static final CredentialPairName DYNAMODB_CREDENTIAL_PAIR_NAME =
      new CredentialPairName(
          DynamoDBConstants.DYNAMODB_ACCESS_KEY_CONF,
          DynamoDBConstants.DYNAMODB_SECRET_KEY_CONF
      );
  private static final CredentialPairName DEFAULT_CREDENTIAL_PAIR_NAME =
      new CredentialPairName(
          DynamoDBConstants.DEFAULT_ACCESS_KEY_CONF,
          DynamoDBConstants.DEFAULT_SECRET_KEY_CONF
      );
  private final Map<String, List<WriteRequest>> writeBatchMap = new HashMap<>();
  private final AmazonDynamoDBClient dynamoDB;
  private int writeBatchMapSizeBytes;
  private int batchWriteRetries;

  // For unit testing only
  public DynamoDBClient() {
    dynamoDB = null;
  }

  public DynamoDBClient(Configuration conf) {
    this(conf, null);
  }

  public DynamoDBClient(Configuration conf, String region) {
    dynamoDB = getDynamoDBClient(conf);
    dynamoDB.setEndpoint(DynamoDBUtil.getDynamoDBEndpoint(conf, region));
  }

  public TableDescription describeTable(String tableName) {
    final DescribeTableRequest describeTablesRequest = new DescribeTableRequest()
        .withTableName(tableName);
    try {
      DynamoDBFibonacciRetryer.RetryResult<DescribeTableResult> describeResult = getRetryDriver().runWithRetry(
          new Callable<DescribeTableResult>() {
            @Override
            public DescribeTableResult call() {
              DescribeTableResult result = dynamoDB.describeTable(describeTablesRequest);
              log.info("Describe table output: " + result);
              return result;
            }
          }, null, null);
      return describeResult.result.getTable();
    } catch (Exception e) {
      throw new RuntimeException("Could not lookup table " + tableName + " in DynamoDB.", e);
    }
  }

  public DynamoDBFibonacciRetryer.RetryResult<ScanResult> scanTable(
      String tableName, DynamoDBQueryFilter dynamoDBQueryFilter, Integer segment, Integer
      totalSegments, Map<String, AttributeValue> exclusiveStartKey, long limit, Reporter reporter) {
    final ScanRequest scanRequest = new ScanRequest(tableName)
        .withExclusiveStartKey(exclusiveStartKey)
        .withLimit(Ints.checkedCast(limit))
        .withSegment(segment)
        .withTotalSegments(totalSegments)
        .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);

    if (dynamoDBQueryFilter != null) {
      Map<String, Condition> scanFilter = dynamoDBQueryFilter.getScanFilter();
      if (!scanFilter.isEmpty()) {
        scanRequest.setScanFilter(scanFilter);
      }
    }

    DynamoDBFibonacciRetryer.RetryResult<ScanResult> retryResult = getRetryDriver().runWithRetry(new Callable<ScanResult>() {
      @Override
      public ScanResult call() {
        log.debug("Executing DynamoDB scan: " + scanRequest);
        return dynamoDB.scan(scanRequest);
      }
    }, reporter, PrintCounter.DynamoDBReadThrottle);
    return retryResult;
  }

  public DynamoDBFibonacciRetryer.RetryResult<QueryResult> queryTable(
      String tableName, DynamoDBQueryFilter dynamoDBQueryFilter, Map<String, AttributeValue>
      exclusiveStartKey, long limit, Reporter reporter) {
    final QueryRequest queryRequest = new QueryRequest()
        .withTableName(tableName)
        .withExclusiveStartKey(exclusiveStartKey)
        .withKeyConditions(dynamoDBQueryFilter.getKeyConditions())
        .withLimit(Ints.checkedCast(limit))
        .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);

    DynamoDBFibonacciRetryer.RetryResult<QueryResult> retryResult = getRetryDriver().runWithRetry(
        new Callable<QueryResult>() {
          @Override
          public QueryResult call() {
            log.debug("Executing DynamoDB query: " + queryRequest);
            return dynamoDB.query(queryRequest);
          }
        }, reporter, PrintCounter.DynamoDBReadThrottle);
    return retryResult;
  }

  public BatchWriteItemResult putBatch(String tableName, Map<String, AttributeValue> item,
                                       long maxBatchSize, Reporter reporter)
      throws UnsupportedEncodingException {
    int itemSizeBytes = DynamoDBUtil.getItemSizeBytes(item);
    if (itemSizeBytes > MAX_ALLOWABLE_BYTE_SIZE) {
      throw new RuntimeException("Cannot pass items with size greater than "
          + MAX_ALLOWABLE_BYTE_SIZE + " bytes");
    }

    long batchLimit = Math.min(maxBatchSize, MAX_WRITE_BATCH_SIZE);
    if (batchLimit < 1) {
      batchLimit = 1;
    }

    BatchWriteItemResult result = null;
    if (writeBatchMap.containsKey(tableName)) {
      if (writeBatchMap.get(tableName).size() >= batchLimit
          || (writeBatchMapSizeBytes + itemSizeBytes) > MAX_ALLOWABLE_BYTE_SIZE) {
        result = writeBatch(reporter, itemSizeBytes);
      }
    }

    // writeBatchMap could be cleared from writeBatch()
    List<WriteRequest> writeBatchList;
    if (!writeBatchMap.containsKey(tableName)) {
      writeBatchList = new ArrayList<>(MAX_WRITE_BATCH_SIZE);
      writeBatchMap.put(tableName, writeBatchList);
    } else {
      writeBatchList = writeBatchMap.get(tableName);
    }
    writeBatchList.add(new WriteRequest().withPutRequest(new PutRequest().withItem(item)));
    writeBatchMapSizeBytes += itemSizeBytes;

    return result;
  }

  public void close() {
    while (!writeBatchMap.isEmpty()) {
      writeBatch(Reporter.NULL, 0);
    }

    if (dynamoDB != null) {
      dynamoDB.shutdown();
    }
  }

  /**
   * @param roomNeeded number of bytes that writeBatch MUST make room for
   */
  private BatchWriteItemResult writeBatch(Reporter reporter, final int roomNeeded) {
    final BatchWriteItemRequest batchWriteItemRequest = new BatchWriteItemRequest()
        .withRequestItems(writeBatchMap)
        .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL);

    DynamoDBFibonacciRetryer.RetryResult<BatchWriteItemResult> retryResult = getRetryDriver().runWithRetry(
        new Callable<BatchWriteItemResult>() {
          @Override
          public BatchWriteItemResult call() throws
              UnsupportedEncodingException,
              InterruptedException {
            pauseExponentially(batchWriteRetries);
            BatchWriteItemResult result = dynamoDB.batchWriteItem(batchWriteItemRequest);

            Map<String, List<WriteRequest>> unprocessedItems = result.getUnprocessedItems();
            if (unprocessedItems == null || unprocessedItems.isEmpty()) {
              batchWriteRetries = 0;
            } else {
              batchWriteRetries++;

              int unprocessedItemCount = 0;
              for (List<WriteRequest> unprocessedWriteRequests : unprocessedItems.values()) {
                unprocessedItemCount += unprocessedWriteRequests.size();

                int batchSizeBytes = 0;
                for (WriteRequest request : unprocessedWriteRequests) {
                  batchSizeBytes += DynamoDBUtil.getItemSizeBytes(
                      request.getPutRequest().getItem());
                }

                if (unprocessedWriteRequests.size() >= MAX_WRITE_BATCH_SIZE
                    || (MAX_ALLOWABLE_BYTE_SIZE - batchSizeBytes) < roomNeeded) {
                  throw new AmazonClientException("Full list of write requests not processed");
                }
              }

              double consumed = 0.0;
              for (ConsumedCapacity consumedCapacity : result.getConsumedCapacity()) {
                consumed = consumedCapacity.getCapacityUnits();
              }

              int batchSize = 0;
              for (List<WriteRequest> writeRequests :
                  batchWriteItemRequest.getRequestItems().values()) {
                batchSize += writeRequests.size();
              }

              log.debug(
                  "BatchWriteItem attempted " + batchSize + " items, consumed " + consumed + " "
                      + "wcu, left unprocessed " + unprocessedItemCount + " items," + " "
                      + "now at " + "" + batchWriteRetries + " retries");
            }
            return result;
          }
        }, reporter, PrintCounter.DynamoDBWriteThrottle);

    writeBatchMap.clear();
    writeBatchMapSizeBytes = 0;

    // If some items failed to go through, add them back to the writeBatchMap
    Map<String, List<WriteRequest>> unprocessedItems = retryResult.result.getUnprocessedItems();
    for (Entry<String, List<WriteRequest>> entry : unprocessedItems.entrySet()) {
      String key = entry.getKey();
      List<WriteRequest> requests = entry.getValue();
      for (WriteRequest request : requests) {
        writeBatchMapSizeBytes += DynamoDBUtil.getItemSizeBytes(request.getPutRequest().getItem());
      }
      writeBatchMap.put(key, requests);
    }
    return retryResult.result;
  }

  private DynamoDBFibonacciRetryer getRetryDriver() {
    return new DynamoDBFibonacciRetryer(Duration.standardMinutes(DEFAULT_RETRY_DURATION));
  }

  private void pauseExponentially(int retries) throws InterruptedException {
    if (retries == 0) {
      return;
    }
    long scaleFactor = 500 + new Random().nextInt(100);
    long delay = (long) (Math.pow(2, retries) * scaleFactor) / 4;
    delay = Math.min(delay, MAX_BACKOFF_IN_MILLISECONDS);
    log.info("Pausing " + delay + " ms at retry " + retries);
    Thread.sleep(delay);
  }

  private AmazonDynamoDBClient getDynamoDBClient(Configuration conf) {
    ClientConfiguration clientConfig = new ClientConfiguration().withMaxErrorRetry(1);
    applyProxyConfiguration(clientConfig, conf);
    return new AmazonDynamoDBClient(getAWSCredentialsProvider(conf), clientConfig);
  }

  @VisibleForTesting
  void applyProxyConfiguration(ClientConfiguration clientConfig, Configuration conf) {
    final String proxyHost = conf.get(DynamoDBConstants.PROXY_HOST);
    final int proxyPort = conf.getInt(DynamoDBConstants.PROXY_PORT, 0);
    final String proxyUsername = conf.get(DynamoDBConstants.PROXY_USERNAME);
    final String proxyPassword = conf.get(DynamoDBConstants.PROXY_PASSWORD);
    boolean proxyHostAndPortPresent = false;
    if (!Strings.isNullOrEmpty(proxyHost) && proxyPort > 0) {
      clientConfig.setProxyHost(proxyHost);
      clientConfig.setProxyPort(proxyPort);
      proxyHostAndPortPresent = true;
    } else if (Strings.isNullOrEmpty(proxyHost) ^ proxyPort <= 0) {
      throw new RuntimeException("Only one of proxy host and port are set, when both are required");
    }
    if (!Strings.isNullOrEmpty(proxyUsername) && !Strings.isNullOrEmpty(proxyPassword)) {
      if (!proxyHostAndPortPresent) {
        throw new RuntimeException("Proxy host and port must be supplied if proxy username and "
            + "password are present");
      } else {
        clientConfig.setProxyUsername(proxyUsername);
        clientConfig.setProxyPassword(proxyPassword);
      }
    } else if (Strings.isNullOrEmpty(proxyUsername) ^ Strings.isNullOrEmpty(proxyPassword)) {
      throw new RuntimeException("Only one of proxy username and password are set, when both are "
          + "required");
    }
  }

  protected AWSCredentialsProvider getAWSCredentialsProvider(Configuration conf) {
    List<AWSCredentialsProvider> providersList = new ArrayList<>();

    // try to load custom credential provider, fail if a provider is specified but cannot be
    // initialized
    String providerClass = conf.get(DynamoDBConstants.CUSTOM_CREDENTIALS_PROVIDER_CONF);
    if (!Strings.isNullOrEmpty(providerClass)) {
      try {
        providersList.add(
            (AWSCredentialsProvider) ReflectionUtils.newInstance(Class.forName(providerClass), conf)
        );
      } catch (ClassNotFoundException e) {
        throw new RuntimeException("Custom AWSCredentialsProvider not found: " + providerClass, e);
      }
    }

    // try to fetch credentials from core-site
    String accessKey = conf.get(DYNAMODB_CREDENTIAL_PAIR_NAME.getAccessKeyName());
    String secretKey;
    if (Strings.isNullOrEmpty(accessKey)) {
      accessKey = conf.get(DEFAULT_CREDENTIAL_PAIR_NAME.getAccessKeyName());
      secretKey = conf.get(DEFAULT_CREDENTIAL_PAIR_NAME.getSecretKeyName());
    } else {
      secretKey = conf.get(DYNAMODB_CREDENTIAL_PAIR_NAME.getSecretKeyName());
    }

    if (Strings.isNullOrEmpty(accessKey) || Strings.isNullOrEmpty(secretKey)) {
      providersList.add(new InstanceProfileCredentialsProvider());
    } else {
      final AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
      providersList.add(new AWSCredentialsProvider() {
        @Override
        public AWSCredentials getCredentials() {
          return credentials;
        }

        @Override
        public void refresh() {
        }
      });
    }

    AWSCredentialsProvider[] providerArray = providersList.toArray(
        new AWSCredentialsProvider[providersList.size()]
    );

    AWSCredentialsProviderChain providerChain = new AWSCredentialsProviderChain(providerArray);
    providerChain.setReuseLastProvider(true);
    return providerChain;
  }

}
