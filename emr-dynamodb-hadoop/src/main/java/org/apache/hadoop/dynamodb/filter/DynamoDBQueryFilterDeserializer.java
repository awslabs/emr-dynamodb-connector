package org.apache.hadoop.dynamodb.filter;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by red on 12/01/2017.
 */
public class DynamoDBQueryFilterDeserializer extends JsonDeserializer<Condition> {
  @Override
  public Condition deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
    JsonNode node = jsonParser.readValueAsTree();
    ObjectCodec codec = jsonParser.getCodec();

    String comparisonOperator = node.get("comparisonOperator").asText();

    List<AttributeValue> valueList = new ArrayList<>();
    JsonNode list = node.get("attributeValueList");
    if (list.isArray()) {
      for (JsonNode obj : list) {
        valueList.add(codec.treeToValue(obj, AttributeValue.class));
      }
    }

    Condition condition = new Condition();
    condition.setComparisonOperator(comparisonOperator);
    condition.setAttributeValueList(valueList);
    return condition;
  }
}
