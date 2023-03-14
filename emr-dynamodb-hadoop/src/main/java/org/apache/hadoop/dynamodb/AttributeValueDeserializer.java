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

package org.apache.hadoop.dynamodb;

import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.dynamodb.type.DynamoDBTypeConstants;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class AttributeValueDeserializer implements JsonDeserializer<AttributeValue> {

  @Override
  public AttributeValue deserialize(JsonElement jsonElement, Type type,
                                    JsonDeserializationContext context) throws JsonParseException {
    if (jsonElement.isJsonNull()) {
      return null;
    }

    if (jsonElement.isJsonObject()) {
      JsonObject jsonObject = jsonElement.getAsJsonObject();

      for (Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
        String attributeName = entry.getKey();
        JsonElement attributeValue = entry.getValue();

        if (DynamoDBTypeConstants.BINARY.equalsIgnoreCase(attributeName)) {
          return AttributeValue.fromB(context.deserialize(attributeValue, SdkBytes.class));
        }

        if (DynamoDBTypeConstants.BINARY_SET.equalsIgnoreCase(attributeName)) {
          JsonArray jsonArray = attributeValue.getAsJsonArray();
          if (!jsonArray.isEmpty()) {
            List<SdkBytes> sdkBytesList = new ArrayList<>();
            jsonArray.forEach(item -> sdkBytesList.add(context.deserialize(item, SdkBytes.class)));
            return AttributeValue.fromBs(sdkBytesList);
          }
        }

        if (DynamoDBTypeConstants.BOOLEAN.equalsIgnoreCase(attributeName)) {
          return AttributeValue.fromBool(attributeValue.getAsBoolean());
        }

        if (DynamoDBTypeConstants.NULL.equalsIgnoreCase(attributeName)) {
          return AttributeValue.fromNul(attributeValue.getAsBoolean());
        }

        if (DynamoDBTypeConstants.NUMBER.equalsIgnoreCase(attributeName)) {
          return AttributeValue.fromN(attributeValue.getAsString());
        }

        if (DynamoDBTypeConstants.NUMBER_SET.equalsIgnoreCase(attributeName)) {
          JsonArray jsonArray = attributeValue.getAsJsonArray();
          if (!jsonArray.isEmpty()) {
            List<String> numberList = new ArrayList<>();
            jsonArray.forEach(item -> numberList.add(item.getAsString()));
            return AttributeValue.fromNs(numberList);
          }
        }

        if (DynamoDBTypeConstants.LIST.equalsIgnoreCase(attributeName)) {
          JsonArray jsonArray = attributeValue.getAsJsonArray();
          if (!jsonArray.isEmpty()) {
            List<AttributeValue> avl = new ArrayList<>();
            jsonArray.forEach(element ->
                avl.add(context.deserialize(element, AttributeValue.class)));
            return AttributeValue.fromL(avl);
          }
        }

        if (DynamoDBTypeConstants.MAP.equalsIgnoreCase(attributeName)) {
          JsonObject jsonMap = attributeValue.getAsJsonObject();
          if (jsonMap.size() != 0) {
            Map<String, AttributeValue> avm = new HashMap<>();
            jsonMap.entrySet().forEach(item -> {
              avm.put(item.getKey(), context.deserialize(item.getValue(), AttributeValue.class));
            });
            return AttributeValue.fromM(avm);
          }
        }

        if (DynamoDBTypeConstants.STRING.equalsIgnoreCase(attributeName)) {
          return AttributeValue.fromS(attributeValue.getAsString());
        }

        if (DynamoDBTypeConstants.STRING_SET.equalsIgnoreCase(attributeName)) {
          JsonArray jsonArray = attributeValue.getAsJsonArray();
          if (!jsonArray.isEmpty()) {
            List<String> stringList = new ArrayList<>();
            jsonArray.forEach(item -> stringList.add(item.getAsString()));
            return AttributeValue.fromSs(stringList);
          }
        }
      }
    }

    // Return an empty instance as default value.
    return AttributeValue.builder().build();
  }
}
