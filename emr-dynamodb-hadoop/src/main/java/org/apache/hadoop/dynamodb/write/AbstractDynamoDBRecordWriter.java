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

package org.apache.hadoop.dynamodb.write;

import static org.apache.hadoop.dynamodb.DynamoDBConstants.DEFAULT_AVERAGE_ITEM_SIZE_IN_BYTES;
import static org.apache.hadoop.dynamodb.DynamoDBUtil.createJobClient;

import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.ConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.dynamodb.DynamoDBClient;
import org.apache.hadoop.dynamodb.DynamoDBConstants;
import org.apache.hadoop.dynamodb.DynamoDBItemWritable;
import org.apache.hadoop.dynamodb.DynamoDBOperationType;
import org.apache.hadoop.dynamodb.DynamoDBUtil;
import org.apache.hadoop.dynamodb.IopsCalculator;
import org.apache.hadoop.dynamodb.IopsController;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.List;

/**
 * AbstractDynamoDBRecordWriter does all the backend work for reading in key-value pairs from the
 * reducer and writing them to DynamoDB. To use the DynamoDBRecordWriter class, you need to subclass
 * this and overwrite the convertValueToDynamoDBItemWritable with a conversion from the input Value
 * to a DynamoDBItemWritable. As an alternative, you can also use DefaultDynamoDBRecordWriter, and
 * do the conversion to a DynamoDBItemWritable inside of the reducer.
 *
 * @param <K> The type of Key received from the reducer
 * @param <V> The type of Value received from the reducer
 */
public abstract class AbstractDynamoDBRecordWriter<K, V> implements RecordWriter<K, V> {

  private static final Log log = LogFactory.getLog(AbstractDynamoDBRecordWriter.class);
  private static final long PRINT_COUNT_INCREMENT = 1000;

  private final DynamoDBClient client;
  private final Progressable progressable;
  private final String tableName;
  private IopsController iopsController;
  private long permissibleWritesPerSecond;
  private Reporter reporter;

  private int batchSize = 0;
  private long intervalBeginTime = 0;
  private long nextPrintCount = PRINT_COUNT_INCREMENT;
  private long totalItemsWritten = 0;
  private double totalIOPSConsumed = 0;
  private long writesPerSecond = 0;
  private boolean deletionMode;

  public AbstractDynamoDBRecordWriter(JobConf jobConf, Progressable progressable) {
    this.progressable = progressable;

    client = new DynamoDBClient(jobConf);
    tableName = jobConf.get(DynamoDBConstants.OUTPUT_TABLE_NAME);
    if (tableName == null) {
      throw new ResourceNotFoundException("No output table name was specified.");
    }


    deletionMode = jobConf.getBoolean(DynamoDBConstants.DELETION_MODE, DynamoDBConstants.DEFAULT_DELETION_MODE);

    IopsCalculator iopsCalculator = new WriteIopsCalculator(createJobClient(jobConf), client,
        tableName);
    iopsController = new IopsController(iopsCalculator, DEFAULT_AVERAGE_ITEM_SIZE_IN_BYTES,
        DynamoDBOperationType.WRITE);
    permissibleWritesPerSecond = iopsController.getTargetItemsPerSecond();
    log.info("Number of allocated item writes per second: " + permissibleWritesPerSecond);

    // Hive may not have a valid Reporter and pass in null progressable
    // TODO Check whether this would happen when excluding Hive
    if (progressable instanceof Reporter) {
      reporter = (Reporter) progressable;
    }
  }

  @Override
  public void write(K key, V value) throws IOException {
    if (value == null) {
      throw new RuntimeException("Null record encountered. At least the key columns must be "
          + "specified.");
    }

    verifyInterval();
    if (progressable != null) {
      progressable.progress();
    }

    DynamoDBItemWritable item = convertValueToDynamoDBItem(key, value);
    BatchWriteItemResult result = client.putBatch(tableName, item.getItem(),
        permissibleWritesPerSecond - writesPerSecond, reporter, deletionMode);

    batchSize++;
    totalItemsWritten++;

    if (result != null) {
      if (result.getConsumedCapacity() != null) {
        for (ConsumedCapacity consumedCapacity : result.getConsumedCapacity()) {
          double consumedUnits = consumedCapacity.getCapacityUnits();
          totalIOPSConsumed += consumedUnits;
        }
      }

      int unprocessedItems = 0;
      for (List<WriteRequest> requests : result.getUnprocessedItems().values()) {
        unprocessedItems += requests.size();
      }
      writesPerSecond += batchSize - unprocessedItems;
      batchSize = unprocessedItems;
    }
  }

  @Override
  public void close(Reporter reporter) throws IOException {
    client.close();
    log.info(totalItemsWritten + " total items written");
  }

  /**
   * Convert some generic value into a type that can be input into DynamoDB
   *
   * @param value The value to convert
   * @return Some object in a format that is compatible with DynamoDB.
   */
  protected abstract DynamoDBItemWritable convertValueToDynamoDBItem(K key, V value);

  private void verifyInterval() {
    if (writesPerSecond >= permissibleWritesPerSecond) {
      if (writesPerSecond > 0) {
        iopsController.update(writesPerSecond, totalIOPSConsumed);
      }
      permissibleWritesPerSecond = iopsController.getTargetItemsPerSecond();

      if (totalItemsWritten > nextPrintCount) {
        log.info("Total items written: " + totalItemsWritten);
        log.info("New writes per second: " + permissibleWritesPerSecond);
        nextPrintCount += PRINT_COUNT_INCREMENT;
      }

      DynamoDBUtil.verifyInterval(intervalBeginTime, 1000L);
      intervalBeginTime = new DateTime(DateTimeZone.UTC).getMillis();
      totalIOPSConsumed = 0;
      writesPerSecond = 0;
    }
  }
}
