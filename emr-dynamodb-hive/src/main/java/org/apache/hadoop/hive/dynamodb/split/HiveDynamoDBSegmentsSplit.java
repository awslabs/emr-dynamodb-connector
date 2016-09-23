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

import org.apache.hadoop.dynamodb.filter.DynamoDBQueryFilter;
import org.apache.hadoop.dynamodb.split.DynamoDBSplit;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This InputSplit implementation is meant to be used with Hive, where {@code path} is non-null, and
 * takes a form of, e.g., hdfs://10.239.164.224:9000/mnt/hive_0110/warehouse/dynamodbtable that maps
 * to a DynamoDB table.
 *
 * {@link org.apache.hadoop.dynamodb.split.DynamoDBSegmentsSplit}. We cannot use
 * DynamoDBSegmentsSplit with Hive because {@link org.apache.hadoop.hive.ql.io.HiveInputFormat} in
 * Aws157Hive (as of version 0.11.0) requires the injected InputSplit object be a FileSplit.
 * Otherwise, it falls back to using new Path(""), which always throws an IllegalArgumentException:
 * Can not create a Path from an empty string.
 */
public class HiveDynamoDBSegmentsSplit extends FileSplit implements DynamoDBSplit {

  private long approxItemCount;
  private int splitId;
  private List<Integer> segments;
  private int totalSegments;
  private DynamoDBQueryFilter filterPushdown;

  public HiveDynamoDBSegmentsSplit() {
    super(null, 0, 0, (String[]) null);
    this.segments = new ArrayList<>();
  }

  public HiveDynamoDBSegmentsSplit(Path path, long approxItemCount, int splitId, List<Integer>
      segments, int totalSegments, DynamoDBQueryFilter filterPushdown) {
    super(path, 0, 0, (String[]) null);
    this.approxItemCount = approxItemCount;
    this.segments = segments;
    this.splitId = splitId;
    this.totalSegments = totalSegments;
    this.filterPushdown = filterPushdown;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);

    approxItemCount = in.readLong();
    splitId = in.readInt();
    int numSegments = in.readInt();
    segments = new ArrayList<>(numSegments);
    for (int i = 0; i < numSegments; i++) {
      segments.add(in.readInt());
    }
    totalSegments = in.readInt();
    filterPushdown = new DynamoDBQueryFilter();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);

    out.writeLong(approxItemCount);
    out.writeInt(splitId);
    out.writeInt(segments.size());
    for (Integer segment : segments) {
      out.writeInt(segment);
    }
    out.writeInt(totalSegments);
  }

  @Override
  public long getLength() {
    return 0;
  }

  @Override
  public String[] getLocations() throws IOException {
    return new String[0];
  }

  public long getApproxItemCount() {
    return approxItemCount;
  }

  public int getSplitId() {
    return splitId;
  }

  public List<Integer> getSegments() {
    return Collections.unmodifiableList(segments);
  }

  public int getTotalSegments() {
    return totalSegments;
  }

  public DynamoDBQueryFilter getFilterPushdown() {
    return filterPushdown;
  }

  public void setDynamoDBFilterPushdown(DynamoDBQueryFilter filterPushdown) {
    this.filterPushdown = filterPushdown;
  }
}
