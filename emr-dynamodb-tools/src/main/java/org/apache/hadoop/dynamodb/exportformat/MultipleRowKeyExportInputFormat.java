package org.apache.hadoop.dynamodb.exportformat;

import org.apache.hadoop.dynamodb.DynamoDBSimpleJsonWritable;
import org.apache.hadoop.dynamodb.preader.DynamoDBRecordReaderContext;
import org.apache.hadoop.dynamodb.read.AbstractDynamoDBInputFormat;
import org.apache.hadoop.dynamodb.read.SimpleJsonDynamoDBRecordReader;
import org.apache.hadoop.dynamodb.split.DynamoDBSplitGenerator;
import org.apache.hadoop.dynamodb.split.MultipleRowKeyDynamoDBSplitGenerator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class MultipleRowKeyExportInputFormat extends
    AbstractDynamoDBInputFormat<Text, DynamoDBSimpleJsonWritable> {

  @Override
  protected DynamoDBSplitGenerator getSplitGenerator(JobConf conf) {
    return new MultipleRowKeyDynamoDBSplitGenerator(conf);
  }

  @Override
  public RecordReader<Text, DynamoDBSimpleJsonWritable> getRecordReader(InputSplit split,
      JobConf conf, Reporter reporter) {
    DynamoDBRecordReaderContext context = buildDynamoDBRecordReaderContext(split, conf, reporter);
    return new SimpleJsonDynamoDBRecordReader(context);
  }
}
