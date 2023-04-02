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

import static org.apache.hadoop.dynamodb.DynamoDBConstants.DEFAULT_MAX_BATCH_SIZE;
import static org.apache.hadoop.dynamodb.DynamoDBConstants.DEFAULT_MAX_ITEMS_PER_BATCH;
import static org.apache.hadoop.dynamodb.DynamoDBConstants.DEFAULT_MAX_ITEM_SIZE;
import static org.apache.hadoop.dynamodb.DynamoDBConstants.MAX_BATCH_SIZE;
import static org.apache.hadoop.dynamodb.DynamoDBConstants.MAX_ITEMS_PER_BATCH;
import static org.apache.hadoop.dynamodb.DynamoDBConstants.MAX_ITEM_SIZE;
import static org.apache.hadoop.dynamodb.DynamoDBUtil.getDynamoDBEndpoint;
import static org.apache.hadoop.dynamodb.DynamoDBUtil.getDynamoDBRegion;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.primitives.Ints;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.dynamodb.DynamoDBFibonacciRetryer.RetryResult;
import org.apache.hadoop.dynamodb.filter.DynamoDBIndexInfo;
import org.apache.hadoop.dynamodb.filter.DynamoDBQueryFilter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;
import org.joda.time.Duration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.Capacity;
import software.amazon.awssdk.services.dynamodb.model.Condition;
import software.amazon.awssdk.services.dynamodb.model.ConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

public class DynamoDBClient {

  private static final Log log = LogFactory.getLog(DynamoDBClient.class);

  private static final int DEFAULT_RETRY_DURATION = 10;
  private static final long MAX_BACKOFF_IN_MILLISECONDS = 1000 * 3;
  private static final CredentialPairName DYNAMODB_CREDENTIAL_PAIR_NAME =
      new CredentialPairName(
          DynamoDBConstants.DYNAMODB_ACCESS_KEY_CONF,
          DynamoDBConstants.DYNAMODB_SECRET_KEY_CONF
      );
  private static final CredentialPairName DYNAMODB_SESSION_CREDENTIAL_PAIR_NAME =
      new CredentialPairName(
          DYNAMODB_CREDENTIAL_PAIR_NAME.getAccessKeyName(),
          DYNAMODB_CREDENTIAL_PAIR_NAME.getSecretKeyName(),
          DynamoDBConstants.DYNAMODB_SESSION_TOKEN_CONF
      );
  private static final CredentialPairName DEFAULT_CREDENTIAL_PAIR_NAME =
      new CredentialPairName(
          DynamoDBConstants.DEFAULT_ACCESS_KEY_CONF,
          DynamoDBConstants.DEFAULT_SECRET_KEY_CONF
      );
  private final Map<String, List<WriteRequest>> writeBatchMap = new HashMap<>();
  private final DynamoDbClient dynamoDB;
  private int writeBatchMapSizeBytes;
  private int batchWriteRetries;
  private final Configuration config;
  private final long maxBatchSize;
  private final long maxItemByteSize;

  // For unit testing only
  public DynamoDBClient() {
    this((DynamoDbClient) null, null);
  }

  public DynamoDBClient(DynamoDbClient amazonDynamoDBClient, Configuration conf) {
    dynamoDB = amazonDynamoDBClient;
    config = conf;
    maxBatchSize = DEFAULT_MAX_BATCH_SIZE;
    maxItemByteSize = DEFAULT_MAX_ITEM_SIZE;
  }

  public DynamoDBClient(Configuration conf) {
    this(conf, null);
  }

  public DynamoDBClient(Configuration conf, String region) {
    Preconditions.checkNotNull(conf, "conf cannot be null.");
    config = conf;
    dynamoDB = getDynamoDBClient(conf, region);
    maxBatchSize = config.getLong(MAX_BATCH_SIZE, DEFAULT_MAX_BATCH_SIZE);
    maxItemByteSize = config.getLong(MAX_ITEM_SIZE, DEFAULT_MAX_ITEM_SIZE);
  }

  public final Map<String, List<WriteRequest>> getWriteBatchMap() {
    return this.writeBatchMap;
  }

  public TableDescription describeTable(String tableName) {
    final DescribeTableRequest describeTablesRequest = DescribeTableRequest.builder()
        .tableName(tableName)
        .build();
    try {
      RetryResult<DescribeTableResponse> describeResult = getRetryDriver().runWithRetry(
          () -> {
            DescribeTableResponse response = dynamoDB.describeTable(describeTablesRequest);
            log.info("Describe table output: " + response);
            return response;
          }, null, null);
      return describeResult.result.table();
    } catch (Exception e) {
      throw new RuntimeException("Could not lookup table " + tableName + " in DynamoDB.", e);
    }
  }

  public RetryResult<ScanResponse> scanTable(
      String tableName, DynamoDBQueryFilter dynamoDBQueryFilter, Integer segment, Integer
      totalSegments, Map<String, AttributeValue> exclusiveStartKey, long limit, Reporter reporter) {
    final ScanRequest.Builder scanRequestBuilder = ScanRequest.builder().tableName(tableName)
        .exclusiveStartKey(exclusiveStartKey)
        .limit(Ints.checkedCast(limit))
        .segment(segment)
        .totalSegments(totalSegments)
        .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL);

    if (dynamoDBQueryFilter != null) {
      Map<String, Condition> scanFilter = dynamoDBQueryFilter.getScanFilter();
      if (!scanFilter.isEmpty()) {
        scanRequestBuilder.scanFilter(scanFilter);
      }
    }

    final ScanRequest scanRequest = scanRequestBuilder.build();

    RetryResult<ScanResponse> retryResult = getRetryDriver().runWithRetry(() -> {
      log.debug("Executing DynamoDB scan: " + scanRequest);
      return dynamoDB.scan(scanRequest);
    }, reporter, PrintCounter.DynamoDBReadThrottle);
    return retryResult;
  }

  public RetryResult<QueryResponse> queryTable(
      String tableName, DynamoDBQueryFilter dynamoDBQueryFilter, Map<String, AttributeValue>
      exclusiveStartKey, long limit, Reporter reporter) {
    final QueryRequest.Builder queryRequestBuilder = QueryRequest.builder()
        .tableName(tableName)
        .exclusiveStartKey(exclusiveStartKey)
        .keyConditions(dynamoDBQueryFilter.getKeyConditions())
        .limit(Ints.checkedCast(limit))
        .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL);

    DynamoDBIndexInfo index = dynamoDBQueryFilter.getIndex();
    if (index != null) {
      log.debug("Using DynamoDB index: " + index.getIndexName());
      queryRequestBuilder.indexName(index.getIndexName());
    }

    final QueryRequest queryRequest = queryRequestBuilder.build();

    RetryResult<QueryResponse> retryResult = getRetryDriver().runWithRetry(
        () -> {
          log.debug("Executing DynamoDB query: " + queryRequest);
          return dynamoDB.query(queryRequest);
        }, reporter, PrintCounter.DynamoDBReadThrottle);
    return retryResult;
  }

  public BatchWriteItemResponse putBatch(String tableName, Map<String, AttributeValue> item,
                                         long maxItemsPerBatch, Reporter reporter,
                                         boolean deletionMode)
      throws UnsupportedEncodingException {

    int itemSizeBytes = DynamoDBUtil.getItemSizeBytes(item);
    if (itemSizeBytes > maxItemByteSize) {
      throw new RuntimeException("Cannot pass items with size greater than " + maxItemByteSize
          + ". Item with size of " + itemSizeBytes + " was given.");
    }
    maxItemsPerBatch = DynamoDBUtil.getBoundedBatchLimit(config, maxItemsPerBatch);
    BatchWriteItemResponse response = null;
    if (writeBatchMap.containsKey(tableName)) {

      boolean writeRequestsForTableAtLimit =
          writeBatchMap.get(tableName).size() >= maxItemsPerBatch;

      boolean totalSizeOfWriteBatchesOverLimit =
          writeBatchMapSizeBytes + itemSizeBytes > maxBatchSize;

      if (writeRequestsForTableAtLimit || totalSizeOfWriteBatchesOverLimit) {
        response = writeBatch(reporter, itemSizeBytes);
      }
    }
    // writeBatchMap could be cleared from writeBatch()
    List<WriteRequest> writeBatchList;
    if (!writeBatchMap.containsKey(tableName)) {
      writeBatchList = new ArrayList<>((int) maxItemsPerBatch);
      writeBatchMap.put(tableName, writeBatchList);
    } else {
      writeBatchList = writeBatchMap.get(tableName);
    }

    log.info("BatchWriteItem deletionMode " + deletionMode);

    if (deletionMode) {
      writeBatchList.add(WriteRequest.builder()
          .deleteRequest(DeleteRequest.builder()
              .key(getKeys(item))
              .build())
          .build());
    } else {
      writeBatchList.add(WriteRequest.builder()
          .putRequest(PutRequest.builder()
              .item(item)
              .build())
          .build());
    }

    writeBatchMapSizeBytes += itemSizeBytes;

    return response;
  }

  public void close() {
    while (!writeBatchMap.isEmpty()) {
      writeBatch(Reporter.NULL, 0);
    }

    if (dynamoDB != null) {
      dynamoDB.close();
    }
  }

  private Map<String, AttributeValue> getKeys(final Map<String, AttributeValue> item) {
    final String tableKeyNames = config.get(DynamoDBConstants.DYNAMODB_TABLE_KEY_NAMES);

    if (tableKeyNames == null || tableKeyNames.isEmpty()) {
      return item;
    }

    final Set<String> keySet = new HashSet<>(
        Arrays.asList(tableKeyNames.split(DynamoDBConstants.DYNAMODB_TABLE_KEY_NAMES_SEPARATOR)));

    final Map<String, AttributeValue> keys = item.entrySet().stream()
        .filter(entry -> keySet.contains(entry.getKey()))
        .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

    if (keys.isEmpty()) {
      throw new IllegalArgumentException(String.format(
          "Given item does not contain any key for the table: %s", tableKeyNames));
    }

    return keys;
  }

  private static Map<String, AttributeValue> getItemFromRequest(WriteRequest request) {
    if (request.putRequest() != null) {
      return request.putRequest().item();
    }
    return request.deleteRequest().key();
  }

  /**
   * @param roomNeeded number of bytes that writeBatch MUST make room for
   */
  private BatchWriteItemResponse writeBatch(Reporter reporter, final int roomNeeded) {
    final BatchWriteItemRequest batchWriteItemRequest = BatchWriteItemRequest.builder()
        .requestItems(writeBatchMap)
        .returnConsumedCapacity(ReturnConsumedCapacity.INDEXES)
        .build();

    RetryResult<BatchWriteItemResponse> retryResult = getRetryDriver().runWithRetry(
        new Callable<BatchWriteItemResponse>() {
          @Override
          public BatchWriteItemResponse call() throws
              UnsupportedEncodingException,
              InterruptedException {
            pauseExponentially(batchWriteRetries);
            BatchWriteItemResponse result = dynamoDB.batchWriteItem(batchWriteItemRequest);

            Map<String, List<WriteRequest>> unprocessedItems = result.unprocessedItems();
            if (unprocessedItems == null || unprocessedItems.isEmpty()) {
              batchWriteRetries = 0;
            } else {
              batchWriteRetries++;

              int unprocessedItemCount = 0;
              for (List<WriteRequest> unprocessedWriteRequests : unprocessedItems.values()) {
                unprocessedItemCount += unprocessedWriteRequests.size();

                int batchSizeBytes = 0;
                for (WriteRequest request : unprocessedWriteRequests) {
                  batchSizeBytes += DynamoDBUtil.getItemSizeBytes(getItemFromRequest(request));
                }

                long maxItemsPerBatch =
                    config.getLong(MAX_ITEMS_PER_BATCH, DEFAULT_MAX_ITEMS_PER_BATCH);
                long maxBatchSize = config.getLong(MAX_BATCH_SIZE, DEFAULT_MAX_BATCH_SIZE);

                if (unprocessedWriteRequests.size() >= maxItemsPerBatch
                    || (maxBatchSize - batchSizeBytes) < roomNeeded) {
                  throw SdkException.builder()
                      .message("Full list of write requests not processed")
                      .build();
                }
              }

              double consumed = 0.0;
              for (ConsumedCapacity consumedCapacity : result.consumedCapacity()) {
                consumed = consumedCapacity.table().capacityUnits();
                if (consumedCapacity.localSecondaryIndexes() != null) {
                  for (Capacity lsiConsumedCapacity :
                      consumedCapacity.localSecondaryIndexes().values()) {
                    consumed += lsiConsumedCapacity.capacityUnits();
                  }
                }
              }

              int batchSize = 0;
              for (List<WriteRequest> writeRequests :
                  batchWriteItemRequest.requestItems().values()) {
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
    Map<String, List<WriteRequest>> unprocessedItems = retryResult.result.unprocessedItems();
    for (Entry<String, List<WriteRequest>> entry : unprocessedItems.entrySet()) {
      String key = entry.getKey();
      List<WriteRequest> requests = entry.getValue();
      for (WriteRequest request : requests) {
        writeBatchMapSizeBytes += DynamoDBUtil.getItemSizeBytes(getItemFromRequest(request));
      }
      writeBatchMap.put(key, new ArrayList<>(requests));
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

  private DynamoDbClient getDynamoDBClient(Configuration conf, String region) {
    final DynamoDbClientBuilder dynamoDbClientBuilder = DynamoDbClient.builder();

    dynamoDbClientBuilder.region(Region.of(getDynamoDBRegion(conf, region)));

    String customEndpoint = getDynamoDBEndpoint(conf, region);
    if (!Strings.isNullOrEmpty(customEndpoint)) {
      dynamoDbClientBuilder.endpointOverride(URI.create(customEndpoint));
    }

    return dynamoDbClientBuilder.httpClient(ApacheHttpClient.builder()
            .proxyConfiguration(applyProxyConfiguration(conf))
            .build())
        .credentialsProvider(getAwsCredentialsProvider(conf))
        .overrideConfiguration(ClientOverrideConfiguration.builder()
            .retryPolicy(builder -> builder.numRetries(1))
            .build())
        .build();
  }

  @VisibleForTesting
  ProxyConfiguration applyProxyConfiguration(Configuration conf) {
    ProxyConfiguration.Builder builder = ProxyConfiguration.builder();

    final String proxyHost = conf.get(DynamoDBConstants.PROXY_HOST);
    final int proxyPort = conf.getInt(DynamoDBConstants.PROXY_PORT, 0);
    final String proxyUsername = conf.get(DynamoDBConstants.PROXY_USERNAME);
    final String proxyPassword = conf.get(DynamoDBConstants.PROXY_PASSWORD);
    boolean proxyHostAndPortPresent = false;
    if (!Strings.isNullOrEmpty(proxyHost) && proxyPort > 0) {
      builder.endpoint(buildProxyEndpoint(proxyHost, proxyPort));
      proxyHostAndPortPresent = true;
    } else if (Strings.isNullOrEmpty(proxyHost) ^ proxyPort <= 0) {
      throw new RuntimeException("Only one of proxy host and port are set, when both are required");
    }
    if (!Strings.isNullOrEmpty(proxyUsername) && !Strings.isNullOrEmpty(proxyPassword)) {
      if (!proxyHostAndPortPresent) {
        throw new RuntimeException("Proxy host and port must be supplied if proxy username and "
            + "password are present");
      } else {
        builder.username(proxyUsername)
            .password(proxyPassword);
      }
    } else if (Strings.isNullOrEmpty(proxyUsername) ^ Strings.isNullOrEmpty(proxyPassword)) {
      throw new RuntimeException("Only one of proxy username and password are set, when both are "
          + "required");
    }

    return builder.build();
  }

  protected AwsCredentialsProvider getAwsCredentialsProvider(Configuration conf) {
    List<AwsCredentialsProvider> providersList = new ArrayList<>();

    // try to load custom credential provider, fail if a provider is specified but cannot be
    // initialized
    String providerClass = conf.get(DynamoDBConstants.CUSTOM_CREDENTIALS_PROVIDER_CONF);
    if (!Strings.isNullOrEmpty(providerClass)) {
      try {
        providersList.add(
            (AwsCredentialsProvider) ReflectionUtils.newInstance(Class.forName(providerClass), conf)
        );
      } catch (ClassNotFoundException e) {
        throw new RuntimeException("Custom AWSCredentialsProvider not found: " + providerClass, e);
      }
    }

    // try to fetch credentials from core-site
    String accessKey = conf.get(DYNAMODB_SESSION_CREDENTIAL_PAIR_NAME.getAccessKeyName());
    String secretKey;
    String sessionKey;
    if (Strings.isNullOrEmpty(accessKey)) {
      accessKey = conf.get(DEFAULT_CREDENTIAL_PAIR_NAME.getAccessKeyName());
      secretKey = conf.get(DEFAULT_CREDENTIAL_PAIR_NAME.getSecretKeyName());
      sessionKey = null;
    } else {
      secretKey = conf.get(DYNAMODB_SESSION_CREDENTIAL_PAIR_NAME.getSecretKeyName());
      sessionKey = conf.get(DYNAMODB_SESSION_CREDENTIAL_PAIR_NAME.getSessionKeyName());
    }

    if (Strings.isNullOrEmpty(accessKey) || Strings.isNullOrEmpty(secretKey)) {
      providersList.add(InstanceProfileCredentialsProvider.create());
    } else if (!Strings.isNullOrEmpty(sessionKey)) {
      final AwsCredentials credentials =
          AwsSessionCredentials.create(accessKey, secretKey, sessionKey);
      providersList.add(() -> credentials);
    } else {
      final AwsCredentials credentials = AwsBasicCredentials.create(accessKey, secretKey);
      providersList.add(() -> credentials);
    }

    AwsCredentialsProvider[] providerArray = providersList.toArray(
        new AwsCredentialsProvider[providersList.size()]
    );

    AwsCredentialsProviderChain providerChain = AwsCredentialsProviderChain.builder()
        .credentialsProviders(providerArray)
        .reuseLastProviderEnabled(true)
        .build();
    return providerChain;
  }

  private URI buildProxyEndpoint(String proxyHost, int proxyPort) {
    // Default proxy protocol is HTTP, aligning with AWS Java SDK 1.x
    // https://github.com/aws/aws-sdk-java/blob/master/aws-java-sdk-core/src/main/java/com/amazonaws/ClientConfiguration.java#L171
    final String HTTP_PROTOCOL = "http://";
    return URI.create(HTTP_PROTOCOL + proxyHost + ":" + proxyPort);
  }
}
