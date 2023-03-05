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
        serializedValue.add(toV1FieldCasingStyle(DynamoDBTypeConstants.BINARY),
            context.serialize(attributeValue.b()));
        break;
      case BOOL:
        serializedValue.add(toV1FieldCasingStyle(DynamoDBTypeConstants.BOOLEAN),
            new JsonPrimitive(attributeValue.bool()));
        break;
      case BS:
        JsonArray sdkBytesList = new JsonArray();
        attributeValue.bs()
            .forEach(item -> sdkBytesList.add(context.serialize(item, SdkBytes.class)));
        serializedValue.add(toV1FieldCasingStyle(DynamoDBTypeConstants.BINARY_SET), sdkBytesList);
        break;
      case L:
        JsonArray attributeList = new JsonArray();
        attributeValue.l()
            .forEach(item -> attributeList.add(context.serialize(item, AttributeValue.class)));
        serializedValue.add(toV1FieldCasingStyle(DynamoDBTypeConstants.LIST), attributeList);
        break;
      case M:
        JsonObject avm = new JsonObject();
        attributeValue.m().entrySet()
            .forEach(entry ->
                avm.add(entry.getKey(), context.serialize(entry.getValue(), AttributeValue.class)));
        serializedValue.add(toV1FieldCasingStyle(DynamoDBTypeConstants.MAP), avm);
        break;
      case N:
        serializedValue.add(toV1FieldCasingStyle(DynamoDBTypeConstants.NUMBER),
            new JsonPrimitive(attributeValue.n()));
        break;
      case NS:
        JsonArray numberList = new JsonArray();
        attributeValue.ns().forEach(item -> numberList.add(new JsonPrimitive(item)));
        serializedValue.add(toV1FieldCasingStyle(DynamoDBTypeConstants.NUMBER_SET), numberList);
        break;
      case NUL:
        serializedValue.add(toV1FieldCasingStyle(DynamoDBTypeConstants.NULL),
            new JsonPrimitive(attributeValue.nul()));
        break;
      case S:
        serializedValue.add(toV1FieldCasingStyle(DynamoDBTypeConstants.STRING),
            new JsonPrimitive(attributeValue.s()));
        break;
      case SS:
        JsonArray stringList = new JsonArray();
        attributeValue.ss().forEach(item -> stringList.add(new JsonPrimitive(item)));
        serializedValue.add(toV1FieldCasingStyle(DynamoDBTypeConstants.STRING_SET), stringList);
        break;
      default:
        break;
    }
    return serializedValue;
  }

  private static String toV1FieldCasingStyle(String typeConstant) {
    return typeConstant.substring(0, 1).toLowerCase() + typeConstant.substring(1).toUpperCase();
  }
}
