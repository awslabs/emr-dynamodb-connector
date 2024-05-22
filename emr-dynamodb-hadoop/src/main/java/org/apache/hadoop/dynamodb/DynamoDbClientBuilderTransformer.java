package org.apache.hadoop.dynamodb;

import java.util.function.UnaryOperator;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;

/**
 * Transformer of a DynamoDbClientBuilder. Takes as a parameter the DynamoDbClientBuilder instance
 * configured by emr-dynamodb-connector.
 */
@FunctionalInterface
public interface DynamoDbClientBuilderTransformer extends UnaryOperator<DynamoDbClientBuilder> {

  static DynamoDbClientBuilderTransformer identity() {
    return builder -> builder;
  }

}
