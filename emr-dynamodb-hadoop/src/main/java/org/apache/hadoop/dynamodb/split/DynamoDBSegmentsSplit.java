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

import org.apache.hadoop.dynamodb.filter.DynamoDBQueryFilter;
import org.apache.hadoop.fs.Path;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DynamoDBSegmentsSplit implements DynamoDBSplit {

  private long approxItemCount;
  private int splitId;
  private List<Integer> segments;
  private int totalSegments;
  private DynamoDBQueryFilter filterPushdown;

  public DynamoDBSegmentsSplit() {
    this.segments = new ArrayList<>();
  }

  public DynamoDBSegmentsSplit(Path path, long approxItemCount, int splitId, List<Integer>
      segments, int totalSegments, DynamoDBQueryFilter filterPushdown) {
    this.approxItemCount = approxItemCount;
    this.segments = segments;
    this.splitId = splitId;
    this.totalSegments = totalSegments;
    this.filterPushdown = filterPushdown;
  }

  @Override
  public long getLength() {
    return 0;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    approxItemCount = in.readLong();
    splitId = in.readInt();
    int numSegments = in.readInt();
    this.segments = new ArrayList<>(numSegments);
    for (int i = 0; i < numSegments; i++) {
      this.segments.add(in.readInt());
    }
    totalSegments = in.readInt();
    filterPushdown = new DynamoDBQueryFilter();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(approxItemCount);
    out.writeInt(splitId);
    out.writeInt(segments.size());
    for (Integer segment : segments) {
      out.writeInt(segment);
    }
    out.writeInt(totalSegments);
  }

  @Override
  public String[] getLocations() throws IOException {
    return new String[0];
  }

  @Override
  public long getApproxItemCount() {
    return approxItemCount;
  }

  @Override
  public int getSplitId() {
    return splitId;
  }

  @Override
  public List<Integer> getSegments() {
    return Collections.unmodifiableList(segments);
  }

  @Override
  public int getTotalSegments() {
    return totalSegments;
  }

  @Override
  public DynamoDBQueryFilter getFilterPushdown() {
    return filterPushdown;
  }

  @Override
  public void setDynamoDBFilterPushdown(DynamoDBQueryFilter filterPushdown) {
    this.filterPushdown = filterPushdown;
  }
}
