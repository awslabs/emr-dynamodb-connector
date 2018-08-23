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

package org.apache.hadoop.dynamodb.read;

import static org.apache.hadoop.dynamodb.DynamoDBUtil.createJobClient;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.dynamodb.DynamoDBClient;
import org.apache.hadoop.dynamodb.DynamoDBConstants;
import org.apache.hadoop.dynamodb.DynamoDBItemWritable;
import org.apache.hadoop.dynamodb.IopsCalculator;
import org.apache.hadoop.dynamodb.preader.AbstractReadManager;
import org.apache.hadoop.dynamodb.preader.DynamoDBRecordReaderContext;
import org.apache.hadoop.dynamodb.preader.PageResultMultiplexer;
import org.apache.hadoop.dynamodb.preader.QueryReadManager;
import org.apache.hadoop.dynamodb.preader.RateController;
import org.apache.hadoop.dynamodb.preader.ScanReadManager;
import org.apache.hadoop.dynamodb.split.DynamoDBSplit;
import org.apache.hadoop.dynamodb.util.TimeSource;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Map;

/**
 * AbstractDynamoDBRecordReader does all the backend work for splitting up the data in DynamoDB,
 * reading it in, and outputting them into key-value pairs for the mapper. To use the
 * DynamoDBRecordReader class, you need to subclass this and overwrite the
 * convertDynamoDBItemToValue with a conversion from the DynamoDB item to the desired value type. As
 * an alternative, you can also use DefaultDynamoDBRecordReader, and do the conversion to a
 * DynamoDBItemWritable inside of the reducer.
 *
 * @param <K> The type of Key that will be passed to the Mapper
 * @param <V> The type of Value that will be passed to the Mapper
 */
public abstract class AbstractDynamoDBRecordReader<K, V> implements RecordReader<K, V> {

  private static final Log log = LogFactory.getLog(AbstractDynamoDBRecordReader.class);

  protected final DynamoDBClient client;
  protected final String tableName;

  protected final DynamoDBSplit split;
  protected final long approxTotalItemCount;
  protected final Reporter reporter;
  private final PageResultMultiplexer<Map<String, AttributeValue>> pageMux;
  private final AbstractReadManager readMgr;
  private final DynamoDBRecordReaderContext context;
  protected volatile long readItemCount;

  public AbstractDynamoDBRecordReader(DynamoDBRecordReaderContext context) {
    this.context = context;
    this.client = context.getClient();
    this.tableName = context.getConf().get(DynamoDBConstants.INPUT_TABLE_NAME);
    this.split = context.getSplit();
    this.readItemCount = 0;
    this.approxTotalItemCount = split.getApproxItemCount();
    this.reporter = context.getReporter();

    int numSegments = split.getSegments().size();
    if (numSegments != 1 && isQuery()) {
      throw new IllegalArgumentException("Query should always result in one segment");
    }

    this.pageMux = new PageResultMultiplexer<>(DynamoDBConstants.PSCAN_SEGMENT_BATCH_SIZE,
        DynamoDBConstants.PSCAN_MULTIPLEXER_CAPACITY);
    this.context.setPageResultMultiplexer(this.pageMux);

    this.readMgr = initReadManager();

    printInitInfo();
  }

  @Override
  public long getPos() throws IOException {
    return 0;
  }

  @Override
  public float getProgress() throws IOException {
    if (approxTotalItemCount == 0) {
      return 0.0f;
    } else {
      float progress = (float) readItemCount / (float) approxTotalItemCount;
      return Math.min(progress, 0.9f);
    }
  }

  @Override
  public boolean next(K key, V value) throws IOException {
    reporter.progress();

    Map<String, AttributeValue> item = pageMux.next();
    if (item != null) {
      convertDynamoDBItemToValue(item, value);
      return true;
    }

    return false;
  }

  @Override
  public void close() throws IOException {
    log.info("Closing down record reader");

    readMgr.shutdown();
    client.close();

    // Note that there is no guarantee that the read workers have pushed all
    // their data to the mux - we're making the assumption that this is fine
    // since shutdown should not be called until after all items have been
    // read from the mux. If it's called earlier, it's because someone wants
    // to cancel the run.
    pageMux.setDraining(true);
  }

  protected void convertDynamoDBItemToValue(Map<String, AttributeValue> item, V value) {
    DynamoDBItemWritable ddbItem = new DynamoDBItemWritable(item);
    convertDynamoDBItemToValue(ddbItem, value);
  }

  /**
   * Convert a DynamoDB item to some value. Note that the nature of this method requires V to have
   * some form of setter since the converted value must be stored in toValue.
   *
   * @param item    The item retrieved from DynamoDB
   * @param toValue Where the converted item should be stored
   */
  protected abstract void convertDynamoDBItemToValue(DynamoDBItemWritable item, V toValue);

  private AbstractReadManager initReadManager() {
    // Calculate target rate. Currently this is only done at task startup
    // time, but we could have this refresh every x minutes so that changes
    // in table provisioning could be reflected.
    IopsCalculator iopsCalculator = new ReadIopsCalculator(createJobClient(context.getConf()),
        client, tableName, split.getTotalSegments(), split.getSegments().size());
    double targetRate = iopsCalculator.calculateTargetIops();

    // Set up the read manager, which will read from input segments and into
    // the output page multiplexer
    TimeSource time = new TimeSource();
    RateController rateController = new RateController(time, targetRate, DynamoDBConstants
        .RATE_CONTROLLER_WINDOW_SIZE_SEC, context.getAverageItemSize());

    if (isQuery()) {
      return new QueryReadManager(rateController, time, context);
    }
    return new ScanReadManager(rateController, time, context);
  }

  private boolean isQuery() {
    return split.getFilterPushdown().getKeyConditions().size() > 0;
  }

  private void printInitInfo() {
    log.info("Total segments: " + split.getTotalSegments());
    log.info("Segment count of this mapper: " + split.getSegments().size());
    log.info("Segments of this mapper: " + split.getSegments());
    log.info("Approximate item count of this mapper: " + approxTotalItemCount);
  }
}
