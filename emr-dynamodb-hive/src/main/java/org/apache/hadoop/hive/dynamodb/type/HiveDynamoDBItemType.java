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

package org.apache.hadoop.hive.dynamodb.type;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import org.apache.hadoop.dynamodb.DynamoDBUtil;
import org.apache.hadoop.dynamodb.key.DynamoDBKey;
import org.apache.hadoop.dynamodb.type.DynamoDBItemType;
import org.apache.hadoop.hive.dynamodb.DerivedHiveTypeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class HiveDynamoDBItemType implements DynamoDBItemType, HiveDynamoDBType {

  private static final Type TYPE = new TypeToken<AttributeValue>() {}.getType();

  @Override
  public Object getHiveData(AttributeValue data, String hiveType) {
    throw new UnsupportedOperationException("DynamoDBItemType does not support this operation.");
  }

  @Override
  public AttributeValue getDynamoDBData(Object data, ObjectInspector objectInspector) {
    throw new UnsupportedOperationException("DynamoDBItemType does not support this operation.");
  }

  @Override
  public AttributeValue getAttributeValue(String... values) {
    throw new UnsupportedOperationException("DynamoDBItemType does not support this operation.");
  }

  @Override
  public String getDynamoDBType() {
    throw new UnsupportedOperationException("DynamoDBItemType does not support this operation.");
  }

  @Override
  public DynamoDBKey getKey(String key) {
    throw new UnsupportedOperationException("DynamoDBItemType does not support this operation.");
  }

  /**
   * Converts a DynamoDB item to a Map&lt;String, String&gt;.
   *
   * The keys in this new map are attribute names of the item. The values in the map are JSON
   * serialization of corresponding AttributeValue in the DynamoDB item.
   *
   * @param dynamoDBItem Map representing the DynamoDB AttributeValue
   * @return A Map&lt;String, String&gt; type for Hive to store.
   */
  public Map<String, String> buildHiveData(Map<String, AttributeValue> dynamoDBItem) {

    if (dynamoDBItem == null || dynamoDBItem.isEmpty()) {
      throw new RuntimeException("DynamoDB item cannot be null or empty.");
    }

    Map<String, String> hiveData = new HashMap<>(dynamoDBItem.size());

    for (Entry<String, AttributeValue> attributeNameValue : dynamoDBItem.entrySet()) {
      hiveData.put(attributeNameValue.getKey(), serializeAttributeValue(attributeNameValue
          .getValue()));
    }

    return hiveData;
  }

  /**
   * Converts a Hive column of type {@code Map&lt;String,String&gt;} into a DynamoDB item.
   *
   * It is expected that the Hive data is a map of type &lt;String, String&gt;. The key in Hive data
   * map is converted to a DynamoDB attribute name. The corresponding value in Hive data map is
   * converted into DynamoDB AttributeValue. This attribute value is expected to be a JSON
   * serialized AttributeValue.
   *
   * @param data                 Data from Hive
   * @param fieldObjectInspector The object inspector for the Hive data. Must have TypeName
   *                             Map&lt;String,String&gt;.
   *
   * @return DynamoDB item representation of provided data from Hive as a
   *         Map&lt;String,AttributeValue&gt;.
   *
   * @throws SerDeException
   */
  public Map<String, AttributeValue> parseDynamoDBData(Object data, ObjectInspector
      fieldObjectInspector) throws SerDeException {

    if (fieldObjectInspector.getCategory() != Category.MAP || !DerivedHiveTypeConstants
        .ITEM_MAP_TYPE_NAME.equals(fieldObjectInspector.getTypeName())) {
      throw new SerDeException(getClass().toString() + " Expecting a MapObjectInspector of type "
          + "map<string,string> for a column which maps DynamoDB item. But we got: "
          + fieldObjectInspector.getTypeName() + " Object inspector: " + fieldObjectInspector);
    }

    Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();

    /* map is of type <String, String> */
    MapObjectInspector mapOI = (MapObjectInspector) fieldObjectInspector;
    StringObjectInspector mapKeyObjectInspector = (StringObjectInspector) mapOI
        .getMapKeyObjectInspector();
    StringObjectInspector mapValueObjectInspector = (StringObjectInspector) mapOI
        .getMapValueObjectInspector();

    /*
     * Get the underlying map object. This is expected to be of type
     * <String,String>
     */
    Map<?, ?> map = mapOI.getMap(data);

    if (map == null || map.isEmpty()) {
      throw new SerDeException("Hive data cannot be null.");
    }

    /* Reconstruct the item */
    for (Entry<?, ?> entry : map.entrySet()) {

      /* Get the string key, value pair */
      String dynamoDBAttributeName = mapKeyObjectInspector.getPrimitiveJavaObject(entry.getKey());
      String dynamoDBAttributeValue = mapValueObjectInspector.getPrimitiveJavaObject(entry
          .getValue());

      /* Deserialize the AttributeValue string */
      AttributeValue deserializedAttributeValue = deserializeAttributeValue(dynamoDBAttributeValue);

      item.put(dynamoDBAttributeName, deserializedAttributeValue);
    }
    return item;
  }

  private String serializeAttributeValue(AttributeValue value) {
    Gson gson = DynamoDBUtil.getGson();
    return gson.toJson(value, TYPE);
  }

  public AttributeValue deserializeAttributeValue(String value) {
    Gson gson = DynamoDBUtil.getGson();

    Object fromJson = gson.fromJson(value, TYPE);
    return (AttributeValue) fromJson;
  }

}
