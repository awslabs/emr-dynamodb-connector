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

package org.apache.hadoop.hive.dynamodb.split;

import org.apache.hadoop.dynamodb.split.DynamoDBSplit;
import org.apache.hadoop.dynamodb.split.DynamoDBSplitGenerator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;

import java.util.List;

public class HiveDynamoDBSplitGenerator extends DynamoDBSplitGenerator {

  @Override
  protected Path getInputPath(JobConf conf) {
    Path path = null;

    Path[] paths = FileInputFormat.getInputPaths(conf);
    if ((paths != null) && (paths.length > 0)) {
      path = paths[0];
    }

    return path;
  }

  @Override
  protected DynamoDBSplit createDynamoDBSplit(Path path, long approxItemCount, int splitId,
      List<Integer> segments, int totalSegments) {
    return new HiveDynamoDBSegmentsSplit(path, approxItemCount, splitId, segments, totalSegments,
        null);
  }

}
