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
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import java.lang.reflect.Type;
import org.apache.hadoop.dynamodb.type.DynamoDBTypeConstants;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class AttributeValueSerializer implements JsonSerializer<AttributeValue> {

  @Override
  public JsonElement serialize(AttributeValue attributeValue, Type type,
                               JsonSerializationContext context) {
    if (attributeValue == null) {
      return JsonNull.INSTANCE;
    }

    JsonObject serializedValue = new JsonObject();
    switch (attributeValue.type()) {
      case B:
        serializedValue.add(DynamoDBTypeConstants.BINARY.toLowerCase(),
            context.serialize(attributeValue.b()));
        break;
      case BOOL:
        serializedValue.add(DynamoDBTypeConstants.BOOLEAN.toLowerCase(),
            new JsonPrimitive(attributeValue.bool()));
        break;
      case BS:
        JsonArray sdkBytesList = new JsonArray();
        attributeValue.bs()
            .forEach(item -> sdkBytesList.add(context.serialize(item, SdkBytes.class)));
        serializedValue.add(DynamoDBTypeConstants.BINARY_SET.toLowerCase(), sdkBytesList);
        break;
      case L:
        JsonArray attributeList = new JsonArray();
        attributeValue.l()
            .forEach(item -> attributeList.add(context.serialize(item, AttributeValue.class)));
        serializedValue.add(DynamoDBTypeConstants.LIST.toLowerCase(), attributeList);
        break;
      case M:
        JsonObject avm = new JsonObject();
        attributeValue.m().entrySet()
            .forEach(entry ->
                avm.add(entry.getKey(), context.serialize(entry.getValue(), AttributeValue.class)));
        serializedValue.add(DynamoDBTypeConstants.MAP.toLowerCase(), avm);
        break;
      case N:
        serializedValue.add(DynamoDBTypeConstants.NUMBER.toLowerCase(),
            new JsonPrimitive(attributeValue.n()));
        break;
      case NS:
        JsonArray numberList = new JsonArray();
        attributeValue.ns().forEach(item -> numberList.add(new JsonPrimitive(item)));
        serializedValue.add(DynamoDBTypeConstants.NUMBER_SET.toLowerCase(), numberList);
        break;
      case NUL:
        serializedValue.add(DynamoDBTypeConstants.NULL.toLowerCase(),
            new JsonPrimitive(attributeValue.nul()));
        break;
      case S:
        serializedValue.add(DynamoDBTypeConstants.STRING.toLowerCase(),
            new JsonPrimitive(attributeValue.s()));
        break;
      case SS:
        JsonArray stringList = new JsonArray();
        attributeValue.ss().forEach(item -> stringList.add(new JsonPrimitive(item)));
        serializedValue.add(DynamoDBTypeConstants.STRING_SET.toLowerCase(), stringList);
        break;
      default:
        break;
    }
    return serializedValue;
  }
}
