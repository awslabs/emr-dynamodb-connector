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

package org.apache.hadoop.dynamodb.split;

import static java.lang.String.format;
import static org.apache.hadoop.dynamodb.DynamoDBConstants.DEFAULT_ROW_KEY_MAX_VALUE;
import static org.apache.hadoop.dynamodb.DynamoDBConstants.DEFAULT_ROW_KEY_MIN_VALUE;
import static org.apache.hadoop.dynamodb.DynamoDBConstants.ROW_KEY_MAX_VALUE;
import static org.apache.hadoop.dynamodb.DynamoDBConstants.ROW_KEY_MIN_VALUE;
import static org.apache.hadoop.dynamodb.DynamoDBConstants.SORT_KEY_MAX_VALUE;
import static org.apache.hadoop.dynamodb.DynamoDBConstants.SORT_KEY_MIN_VALUE;

import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.dynamodb.DynamoDBConstants;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

public class MultipleRowKeyDynamoDBSplitGenerator extends DynamoDBSplitGenerator {
  private static final Log log = LogFactory.getLog(MultipleRowKeyDynamoDBSplitGenerator.class);

  private final String indexName;
  private final String rowKeyName;
  private final String sortKeyName;
  private final long sortKeyMinInclusive;
  private final long sortKeyMaxExclusive;
  private final long[] rowKeyValues;

  public MultipleRowKeyDynamoDBSplitGenerator(JobConf jobConf) {
    indexName = jobConf.get(DynamoDBConstants.INDEX_NAME);
    rowKeyName = jobConf.get(DynamoDBConstants.ROW_KEY_NAME);
    sortKeyName = jobConf.get(DynamoDBConstants.SORT_KEY_NAME);
    sortKeyMinInclusive = jobConf.getLong(SORT_KEY_MIN_VALUE, -1L);
    sortKeyMaxExclusive = jobConf.getLong(SORT_KEY_MAX_VALUE, -1L);
    if (sortKeyMinInclusive == -1) {
      throw new IllegalArgumentException("Required value not configured: " + SORT_KEY_MIN_VALUE);
    }
    if (sortKeyMaxExclusive == -1) {
      throw new IllegalArgumentException("Required value not configured: " + SORT_KEY_MAX_VALUE);
    }

    double samplingPercent = jobConf.getDouble(DynamoDBConstants.ROW_SAMPLE_PERCENT,
        DynamoDBConstants.DEFAULT_ROW_SAMPLE_PERCENT);
    long rowKeyMinValue = jobConf.getLong(ROW_KEY_MIN_VALUE, DEFAULT_ROW_KEY_MIN_VALUE);
    long rowKeyMaxValue = jobConf.getLong(ROW_KEY_MAX_VALUE, DEFAULT_ROW_KEY_MAX_VALUE);
    int numKeys =
        ((Double) Math.floor(samplingPercent * (rowKeyMaxValue - rowKeyMinValue + 1))).intValue();
    if (numKeys < 1) {
      throw new IllegalArgumentException(format(
          "%s value of %s to low to generate discrete keys from table key range of %s to %s",
          DynamoDBConstants.ROW_SAMPLE_PERCENT, samplingPercent, rowKeyMinValue, rowKeyMaxValue));
    }

    log.info(format("indexName: %s", indexName));
    log.info(format("rowKeyName: %s", rowKeyName));
    log.info(format("sortKeyName: %s", sortKeyName));
    log.info(format("sortKeyMinInclusive: %s", sortKeyMinInclusive));
    log.info(format("sortKeyMaxExclusive: %s", sortKeyMaxExclusive));
    log.info(format("samplingPercent: %s", samplingPercent));
    log.info(format("rowKeyMinValue: %s", rowKeyMinValue));
    log.info(format("rowKeyMaxValue: %s", rowKeyMaxValue));
    log.info(format("Number of row keys to select: %s", numKeys));
    log.info(format("Output path: %s", jobConf.get("mapreduce.output.fileoutputformat.outputdir")));

    // simple approach to select the first N rowKeys
    rowKeyValues = new long[numKeys];
    for (int i = 0; i < numKeys; i++) {
      rowKeyValues[i] = i + rowKeyMinValue;
    }

    if (rowKeyValues[rowKeyValues.length - 1] > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(format(
          "The %s of %s, combined with a %s of %s produces "
              + "a key that exceeds the max int value of %s",
          ROW_KEY_MAX_VALUE, rowKeyMaxValue, DynamoDBConstants.ROW_SAMPLE_PERCENT,
          samplingPercent, Integer.MAX_VALUE));
    }
  }

  @Override
  public InputSplit[] generateSplits(int maxClusterMapTasks, int numSegments, JobConf conf) {
    return super.generateSplits(maxClusterMapTasks, rowKeyValues.length, conf);
  }

  @Override
  protected void assignSegmentsToSplits(List<List<Integer>> segmentsPerSplit, int numSegments,
      int numMappers) {
    // Round-robin which split gets which segment id
    int mapper = 0;
    for (long rowKeyValue : rowKeyValues) {
      segmentsPerSplit.get(mapper).add((int)rowKeyValue);
      mapper = (mapper + 1) % numMappers;
    }
  }

  @Override
  protected DynamoDBSplit createDynamoDBSplit(Path path, long approxItemCount, int splitId,
      List<Integer> segments, int totalSegments) {

    return new DynamoDBRowKeySplit(path, approxItemCount, splitId, segments, totalSegments,
        indexName, rowKeyName, sortKeyName, sortKeyMinInclusive, sortKeyMaxExclusive);
  }
}
