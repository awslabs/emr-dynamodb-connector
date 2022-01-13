package org.apache.hadoop.dynamodb.exportformat;

import org.apache.hadoop.dynamodb.read.DynamoDBInputFormat;
import org.apache.hadoop.dynamodb.split.DynamoDBSplitGenerator;
import org.apache.hadoop.dynamodb.split.MultipleRowKeyDynamoDBSplitGenerator;
import org.apache.hadoop.mapred.JobConf;

public class MultipleRowKeyExportInputFormat extends DynamoDBInputFormat {

  @Override
  protected DynamoDBSplitGenerator getSplitGenerator(JobConf conf) {
    return new MultipleRowKeyDynamoDBSplitGenerator(conf);
  }
}
