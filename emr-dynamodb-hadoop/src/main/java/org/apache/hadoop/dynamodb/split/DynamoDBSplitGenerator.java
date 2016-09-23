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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.dynamodb.DynamoDBConstants;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import java.util.ArrayList;
import java.util.List;

public class DynamoDBSplitGenerator {

  private static final Log log = LogFactory.getLog(DynamoDBSplitGenerator.class);

  public InputSplit[] generateSplits(int maxClusterMapTasks, int numSegments, JobConf conf) {
    log.info("Generating " + numSegments + " segments for " + maxClusterMapTasks + " max mappers");

    int numMappers = Math.min(maxClusterMapTasks, numSegments);
    List<List<Integer>> segmentsPerSplit = new ArrayList<List<Integer>>(numMappers);
    for (int i = 0; i < numMappers; i++) {
      segmentsPerSplit.add(new ArrayList<Integer>());
    }

    // Round-robin which split gets which segment id
    int mapper = 0;
    for (int i = 0; i < numSegments; i++) {
      segmentsPerSplit.get(mapper).add(i);
      mapper = (mapper + 1) % numMappers;
    }

    long approxItemCountPerSplit = conf.getLong(DynamoDBConstants.ITEM_COUNT, 0) / ((long)
        numMappers);
    InputSplit[] splits = new InputSplit[numMappers];
    for (int i = 0; i < numMappers; i++) {
      log.info("Assigning " + segmentsPerSplit.get(i).size() + " segments to mapper " + i + ": "
          + segmentsPerSplit.get(i));
      splits[i] = createDynamoDBSplit(getInputPath(conf), approxItemCountPerSplit, i,
          segmentsPerSplit.get(i), numSegments);
    }

    return splits;
  }

  protected Path getInputPath(JobConf conf) {
    return null;
  }

  protected DynamoDBSplit createDynamoDBSplit(Path path, long approxItemCount, int splitId,
      List<Integer> segments, int totalSegments) {
    return new DynamoDBSegmentsSplit(path, approxItemCount, splitId, segments, totalSegments, null);
  }

}
