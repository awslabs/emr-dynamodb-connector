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

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.model.BillingMode;

/**
 * Contains constants used for the Hadoop to DynamoDB connection. Note that many of these string
 * literals are specifically chosen to allow for backward compatibility with Hive and should not be
 * changed.
 */
public interface DynamoDBConstants {

  // Credentials
  String DYNAMODB_ACCESS_KEY_CONF = "dynamodb.awsAccessKeyId";
  String DYNAMODB_SECRET_KEY_CONF = "dynamodb.awsSecretAccessKey";
  String DYNAMODB_SESSION_TOKEN_CONF = "dynamodb.awsSessionToken";
  String DEFAULT_ACCESS_KEY_CONF = "fs.s3.awsAccessKeyId";
  String DEFAULT_SECRET_KEY_CONF = "fs.s3.awsSecretAccessKey";
  String CUSTOM_CREDENTIALS_PROVIDER_CONF = "dynamodb.customAWSCredentialsProvider";

  // Table constants
  String DYNAMODB_COLUMN_MAPPING = "dynamodb.column.mapping";
  String DYNAMODB_TYPE_MAPPING = "dynamodb.type.mapping";
  String DYNAMODB_NULL_SERIALIZATION = "dynamodb.null.serialization";

  // JobConf constants
  String DYNAMODB_FILTER_PUSHDOWN = "dynamodb.filter.pushdown";

  String ENDPOINT = "dynamodb.endpoint";

  String REGION_ID = "dynamodb.regionid";
  String REGION = "dynamodb.region";
  String PROXY_HOST = "dynamodb.proxy.hostname";
  String PROXY_PORT = "dynamodb.proxy.port";
  String PROXY_USERNAME = "dynamodb.proxy.username";
  String PROXY_PASSWORD = "dynamodb.proxy.password";

  // The TABLE_NAME constant is here for backwards compatibility with Hive
  String TABLE_NAME = "dynamodb.table.name";
  String OUTPUT_TABLE_NAME = "dynamodb.output.tableName";
  String INPUT_TABLE_NAME = "dynamodb.input.tableName";

  String THROUGHPUT_WRITE_PERCENT = "dynamodb.throughput.write.percent";
  String THROUGHPUT_READ_PERCENT = "dynamodb.throughput.read.percent";
  String READ_THROUGHPUT = "dynamodb.throughput.read";
  String WRITE_THROUGHPUT = "dynamodb.throughput.write";
  String AVG_ITEM_SIZE = "dynamodb.item.average.size";
  String ITEM_COUNT = "dynamodb.item.count";
  String TABLE_SIZE_BYTES = "dynamodb.table.size-bytes";
  String MAX_MAP_TASKS = "dynamodb.max.map.tasks";
  String DEFAULT_THROUGHPUT_PERCENTAGE = "0.5";
  String BILLING_MODE_PROVISIONED = BillingMode.PROVISIONED.toString();

  String DYNAMODB_MAX_ITEM_SIZE = "dynamodb.max.item.size";
  String MAX_ITEM_SIZE = DYNAMODB_MAX_ITEM_SIZE;
  String MAX_BATCH_SIZE = "dynamodb.max.batch.size";
  String MAX_ITEMS_PER_BATCH = "dynamodb.max.batch.items";

  String INDEX_NAME = "dynamodb.index.name";
  String ROW_KEY_NAME = "dynamodb.row.key.name";
  String ROW_KEY_MIN_VALUE = "dynamodb.row.key.min.value";
  String ROW_KEY_MAX_VALUE = "dynamodb.row.key.max.value";
  String ROW_SAMPLE_PERCENT = "dynamodb.row.sample_percent";
  String SORT_KEY_NAME = "dynamodb.sort.key.name";
  String SORT_KEY_MIN_VALUE = "dynamodb.sort.key.min.value";
  String SORT_KEY_MAX_VALUE = "dynamodb.sort.key.max.value";
  String ATTRIBUTES_TO_GET = "dynamodb.attributes.to.get";

  String DELETION_MODE = "dynamodb.deletion.mode";
  boolean DEFAULT_DELETION_MODE = false;

  // http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html
  long DEFAULT_MAX_ITEM_SIZE = 400 * 1024;
  long DEFAULT_MAX_BATCH_SIZE = 16 * 1024 * 1024;
  long DEFAULT_MAX_ITEMS_PER_BATCH = 25;

  double READ_EVENTUALLY_TO_STRONGLY_CONSISTENT_FACTOR = 2;

  String SCAN_SEGMENTS = "dynamodb.scan.segments";
  int MAX_SCAN_SEGMENTS = 1000000;
  int MIN_SCAN_SEGMENTS = 1;
  double BYTES_PER_READ_CAPACITY_UNIT = 4096;
  double BYTES_PER_WRITE_CAPACITY_UNIT = 1024;

  long MAX_BYTES_PER_SEGMENT = 1024L * 1024L * 1024L;
  double MIN_IO_PER_SEGMENT = 100.0;

  int PSCAN_SEGMENT_BATCH_SIZE = 50;
  int PSCAN_MULTIPLEXER_CAPACITY = 600;
  int RATE_CONTROLLER_WINDOW_SIZE_SEC = 5;

  String EXPORT_FORMAT_VERSION = "dynamodb.export.format.version";
  String DEFAULT_AWS_REGION = Regions.US_EAST_1.getName();

  int DEFAULT_AVERAGE_ITEM_SIZE_IN_BYTES = 100;
  Long DEFAULT_CAPACITY_FOR_ON_DEMAND = 40000L;

  Double DEFAULT_ROW_SAMPLE_PERCENT = 0.001;
  Long DEFAULT_ROW_KEY_MIN_VALUE = 1L;
  Long DEFAULT_ROW_KEY_MAX_VALUE = 10000L;
}
