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

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;

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
  String CUSTOM_CLIENT_BUILDER_TRANSFORMER = "dynamodb.customClientBuilderTransformer";

  // Table constants
  String DYNAMODB_COLUMN_MAPPING = "dynamodb.column.mapping";
  String DYNAMODB_TYPE_MAPPING = "dynamodb.type.mapping";
  String DYNAMODB_NULL_SERIALIZATION = "dynamodb.null.serialization";
  String DYNAMODB_TABLE_KEY_NAMES = "dynamodb.table.keyNames";

  String DYNAMODB_TABLE_KEY_NAMES_SEPARATOR = ",";

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
  String READ_THROUGHPUT_AUTOSCALING = "dynamodb.throughput.read.autoscaling";
  String WRITE_THROUGHPUT_AUTOSCALING = "dynamodb.throughput.write.autoscaling";
  String AVG_ITEM_SIZE = "dynamodb.item.average.size";
  String ITEM_COUNT = "dynamodb.item.count";
  String TABLE_SIZE_BYTES = "dynamodb.table.size-bytes";
  String MAX_MAP_TASKS = "dynamodb.max.map.tasks";
  String DEFAULT_THROUGHPUT_PERCENTAGE = "0.5";
  String DEFAULT_THROUGHPUT_AUTOSCALING = "true";
  String BILLING_MODE_PROVISIONED = BillingMode.PROVISIONED.toString();

  String DYNAMODB_MAX_ITEM_SIZE = "dynamodb.max.item.size";
  String MAX_ITEM_SIZE = DYNAMODB_MAX_ITEM_SIZE;
  String MAX_BATCH_SIZE = "dynamodb.max.batch.size";
  String MAX_ITEMS_PER_BATCH = "dynamodb.max.batch.items";

  String DELETION_MODE = "dynamodb.deletion.mode";
  boolean DEFAULT_DELETION_MODE = false;

  // The default size of segment split
  String SEGMENT_SPLIT_SIZE = "dynamodb.split.size";
  long DEFAULT_SEGMENT_SPLIT_SIZE = 1;

  // Whether current resource manager is Yarn or not
  String YARN_RESOURCE_MANAGER_ENABLED = "dynamodb.yarn.enabled";
  boolean DEFAULT_YARN_RESOURCE_MANAGER_ENABLED = true;

  // http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html
  long DEFAULT_MAX_ITEM_SIZE = 400 * 1024;
  long DEFAULT_MAX_BATCH_SIZE = 16 * 1024 * 1024;
  long DEFAULT_MAX_ITEMS_PER_BATCH = 25;

  double READ_EVENTUALLY_TO_STRONGLY_CONSISTENT_FACTOR = 2;

  String SCAN_SEGMENTS = "dynamodb.scan.segments";
  String EXCLUDED_SCAN_SEGMENTS = "dynamodb.scan.segments.exclude";
  int MAX_SCAN_SEGMENTS = 1000000;
  int MIN_SCAN_SEGMENTS = 1;
  double BYTES_PER_READ_CAPACITY_UNIT = 4096;
  double BYTES_PER_WRITE_CAPACITY_UNIT = 1024;

  long MAX_BYTES_PER_SEGMENT = 100 * 1024L * 1024L; // At most 100 MB per segment
  double MIN_IO_PER_SEGMENT = 100.0;

  int PSCAN_SEGMENT_BATCH_SIZE = 50;
  int PSCAN_MULTIPLEXER_CAPACITY = 600;
  int RATE_CONTROLLER_WINDOW_SIZE_SEC = 5;

  String EXPORT_FORMAT_VERSION = "dynamodb.export.format.version";
  String DEFAULT_AWS_REGION = Region.US_EAST_1.toString();

  int DEFAULT_AVERAGE_ITEM_SIZE_IN_BYTES = 100;
  Long DEFAULT_CAPACITY_FOR_ON_DEMAND = 40000L;
}
