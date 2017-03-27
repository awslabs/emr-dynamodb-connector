package org.apache.hadoop.dynamodb.util;

import com.amazonaws.services.dynamodbv2.model.Condition;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.hadoop.dynamodb.filter.DynamoDBQueryFilterDeserializer;

/**
 * Created by red on 12/01/2017.
 */
public class ObjectMapperUtil {
  public static ObjectMapper createDefaultMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.registerModule(new SimpleModule()
        .addDeserializer(Condition.class, new DynamoDBQueryFilterDeserializer())
    );
    return mapper;
  }
}
