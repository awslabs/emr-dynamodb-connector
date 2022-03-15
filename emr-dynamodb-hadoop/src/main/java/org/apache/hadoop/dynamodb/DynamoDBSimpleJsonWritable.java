package org.apache.hadoop.dynamodb;

import com.amazonaws.services.dynamodbv2.document.ItemUtils;
import java.io.DataInput;

public class DynamoDBSimpleJsonWritable extends DynamoDBItemWritable {

  @Override
  public String writeStream() {
    // Write standard json without type annotations in a way that can be consumed by JsonSerde
    // see https://stackoverflow.com/questions/43812278/converting-dynamodb-json-to-standard-json-with-java
    return ItemUtils.toItem(getItem()).toJSON();
  }

  @Override
  public void readFields(DataInput in) throws UnsupportedOperationException {
    throw new UnsupportedOperationException(
        "This class supports serializing DynamoDB items as json, "
            + "but not deserializing json back to items");
  }
}
