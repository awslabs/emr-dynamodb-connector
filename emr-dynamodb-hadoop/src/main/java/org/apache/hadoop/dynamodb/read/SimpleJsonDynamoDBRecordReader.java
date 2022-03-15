package org.apache.hadoop.dynamodb.read;

import org.apache.hadoop.dynamodb.DynamoDBItemWritable;
import org.apache.hadoop.dynamodb.DynamoDBSimpleJsonWritable;
import org.apache.hadoop.dynamodb.preader.DynamoDBRecordReaderContext;
import org.apache.hadoop.io.Text;

public class SimpleJsonDynamoDBRecordReader extends AbstractDynamoDBRecordReader<Text,
    DynamoDBSimpleJsonWritable> {

  public SimpleJsonDynamoDBRecordReader(DynamoDBRecordReaderContext context) {
    super(context);
  }

  @Override
  protected void convertDynamoDBItemToValue(DynamoDBItemWritable item,
      DynamoDBSimpleJsonWritable toValue) {
    toValue.setItem(item.getItem());
  }

  @Override
  public Text createKey() {
    return new Text();
  }

  @Override
  public DynamoDBSimpleJsonWritable createValue() {
    return new DynamoDBSimpleJsonWritable();
  }
}
