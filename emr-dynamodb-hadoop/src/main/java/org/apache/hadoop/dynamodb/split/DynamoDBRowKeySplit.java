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

import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.dynamodb.filter.DynamoDBFilterOperator;
import org.apache.hadoop.dynamodb.filter.DynamoDBIndexInfo;
import org.apache.hadoop.dynamodb.filter.DynamoDBQueryFilter;
import org.apache.hadoop.dynamodb.type.DynamoDBTypeFactory;
import org.apache.hadoop.fs.Path;

public class DynamoDBRowKeySplit extends DynamoDBSegmentsSplit {
  private String indexName;
  private String rowKeyName;
  private String sortKeyName;
  private Long sortKeyMinInclusive;
  private Long sortKeyMaxExclusive;

  public DynamoDBRowKeySplit() {
    super();
  }

  public DynamoDBRowKeySplit(Path path, long approxItemCount, int splitId, List<Integer>
      segments, int totalSegments, String indexName, String rowKeyName, String sortKeyName,
      Long sortKeyMinInclusive, Long sortKeyMaxExclusive) {
    super(path, approxItemCount, splitId, segments, totalSegments, null);
    this.indexName = indexName;
    this.rowKeyName = rowKeyName;
    this.sortKeyName = sortKeyName;
    this.sortKeyMinInclusive = sortKeyMinInclusive;
    this.sortKeyMaxExclusive = sortKeyMaxExclusive;
  }

  // With length = 0, the split gets ignored by Spark
  @Override
  public long getLength() {
    return 1;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    indexName = in.readUTF();
    rowKeyName = in.readUTF();
    sortKeyName = in.readUTF();
    sortKeyMinInclusive = in.readLong();
    sortKeyMaxExclusive = in.readLong();
    setDynamoDBFilterPushdown(buildFilterPushdown());
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeUTF(indexName);
    out.writeUTF(rowKeyName);
    out.writeUTF(sortKeyName);
    out.writeLong(sortKeyMinInclusive);
    out.writeLong(sortKeyMaxExclusive);
  }

  private DynamoDBQueryFilter buildFilterPushdown() {
    List<KeySchemaElement> indexSchema = new ArrayList<>();
    indexSchema.add(new KeySchemaElement(rowKeyName, KeyType.HASH));
    indexSchema.add(new KeySchemaElement(sortKeyName, KeyType.RANGE));

    Projection indexProjection = new Projection().withProjectionType(ProjectionType.ALL);

    DynamoDBQueryFilter filterPushdown = new DynamoDBQueryFilter();
    filterPushdown.setIndex(new DynamoDBIndexInfo(indexName, indexSchema, indexProjection));
    filterPushdown.addKeyCondition(
        new DynamoDBNAryFilter(sortKeyName, DynamoDBFilterOperator.BETWEEN,
            DynamoDBTypeFactory.NUMBER_TYPE, Long.toString(sortKeyMinInclusive),
            Long.toString(sortKeyMaxExclusive)));
    // added in MultiKeyQueryRecordReadRequest which knows the segment, which is the parallelization
    // unit within a split
    //filterPushdown.addKeyCondition(
    //    new DynamoDBNAryFilter(rowKeyName, DynamoDBFilterOperator.EQ,
    //        DynamoDBTypeFactory.NUMBER_TYPE, Long.toString(rowKeyValue)));
    return filterPushdown;
  }
}
