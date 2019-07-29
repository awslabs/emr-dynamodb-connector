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

package org.apache.hadoop.hive.dynamodb.util;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.dynamodb.type.HiveDynamoDBListTypeFactory;
import org.apache.hadoop.hive.dynamodb.type.HiveDynamoDBType;
import org.apache.hadoop.hive.dynamodb.type.HiveDynamoDBTypeFactory;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.BytesWritable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DynamoDBDataParser {
  private static final Log log = LogFactory.getLog(DynamoDBDataParser.class);

  public static String getNumber(Object data, ObjectInspector objectInspector) {
    if (objectInspector.getTypeName().equals(serdeConstants.DOUBLE_TYPE_NAME)) {
      return Double.toString(((DoubleObjectInspector) objectInspector).get(data));
    } else if (objectInspector.getTypeName().equals(serdeConstants.BIGINT_TYPE_NAME)) {
      return Long.toString(((LongObjectInspector) objectInspector).get(data));
    }
    throw new IllegalArgumentException("Unknown object inspector type: " + objectInspector.getCategory()
            + " Type name: " + objectInspector.getTypeName());
  }

  public static Boolean getBoolean(Object data, ObjectInspector objectInspector) {
    return ((BooleanObjectInspector) objectInspector).get(data);
  }

  public static String getString(Object data, ObjectInspector objectInspector) {
    return ((StringObjectInspector) objectInspector).getPrimitiveJavaObject(data);
  }

  public static ByteBuffer getByteBuffer(Object data, ObjectInspector objectInspector) {
    BytesWritable bw = ((BinaryObjectInspector) objectInspector).getPrimitiveWritableObject(data);
    byte[] result = new byte[bw.getLength()];
    System.arraycopy(bw.getBytes(), 0, result, 0, bw.getLength());
    return ByteBuffer.wrap(result);
  }

  public static Map<String, AttributeValue> getMapAttribute(Object data, ObjectInspector objectInspector) {
    MapObjectInspector mapOI = (MapObjectInspector) objectInspector;
    Map<?, ?> dataMap = mapOI.getMap(data);

    if (dataMap == null) {
      return null;
    }

    Map<String, AttributeValue> itemMap = new HashMap<>();
    StringObjectInspector mapKeyOI = (StringObjectInspector) mapOI.getMapKeyObjectInspector();
    ObjectInspector mapValueOI = mapOI.getMapValueObjectInspector();
    HiveDynamoDBType valueType = HiveDynamoDBTypeFactory.getTypeObjectFromHiveType(mapValueOI);

    // borrowed from HiveDynamoDBItemType
    for (Map.Entry<?,?> entry : dataMap.entrySet()) {
      String attributeName = mapKeyOI.getPrimitiveJavaObject(entry.getKey());
      AttributeValue attributeValue = valueType.getDynamoDBData(entry.getValue(), mapValueOI);
      itemMap.put(attributeName, attributeValue);
    }
    return itemMap;
  }

  public static List<AttributeValue> getListAttribute(Object data, ObjectInspector objectInspector) {
    ListObjectInspector listObjectInspector = (ListObjectInspector) objectInspector;
    List<?> dataList = listObjectInspector.getList(data);

    if (dataList == null) {
      return null;
    }

    ObjectInspector itemObjectInspector = listObjectInspector.getListElementObjectInspector();
    HiveDynamoDBType itemType = HiveDynamoDBListTypeFactory.getTypeObjectFromHiveType(itemObjectInspector);
    List<AttributeValue> itemList = new ArrayList<>();
    for (Object dataItem : dataList) {
      if (dataItem == null) {
        throw new NullPointerException("Null element found in list: " + dataList);
      }
      itemList.add(itemType.getDynamoDBData(dataItem, itemObjectInspector));
    }

    return itemList;
  }

  /**
   * This method currently supports StringSet and NumberSet data type of DynamoDB
   */
  public static List<String> getSetAttribute(Object data, ObjectInspector objectInspector, String
          ddType) {
    ListObjectInspector listObjectInspector = (ListObjectInspector) objectInspector;
    List<?> dataList = listObjectInspector.getList(data);

    if (dataList == null) {
      return null;
    }

    ObjectInspector itemObjectInspector = listObjectInspector.getListElementObjectInspector();
    List<String> itemList = new ArrayList<>();
    for (Object dataItem : dataList) {
      if (dataItem == null) {
        throw new NullPointerException("Null element found in list: " + dataList);
      }

      if (ddType.equals("SS")) {
        itemList.add(getString(dataItem, itemObjectInspector));
      } else if (ddType.equals("NS")) {
        itemList.add(getNumber(dataItem, itemObjectInspector));
      } else {
        throw new IllegalArgumentException("Expecting NumberSet or StringSet type: " + ddType);
      }
    }

    return itemList;
  }

  /**
   * This method currently supports BinarySet data type of DynamoDB
   */
  public static List<ByteBuffer> getByteBuffers(Object data, ObjectInspector objectInspector, String
          ddType) {
    ListObjectInspector listObjectInspector = (ListObjectInspector) objectInspector;
    List<?> dataList = listObjectInspector.getList(data);

    if (dataList == null) {
      return null;
    }

    ObjectInspector itemObjectInspector = listObjectInspector.getListElementObjectInspector();
    List<ByteBuffer> itemList = new ArrayList<>();
    for (Object dataItem : dataList) {
      if (dataItem == null) {
        throw new NullPointerException("Null element found in list: " + dataList);
      }

      if (ddType.equals("BS")) {
        itemList.add(getByteBuffer(dataItem, itemObjectInspector));
      } else {
        throw new IllegalArgumentException("Expecting BinarySet type: " + ddType);
      }
    }

    return itemList;
  }

  public static Object getNumberObjectList(List<String> data, ObjectInspector objectInspector) {
    ListObjectInspector listOI = (ListObjectInspector) objectInspector;
    ObjectInspector itemOI = listOI.getListElementObjectInspector();

    List<Object> numberValues = new ArrayList<>();
    for (String item : data) {
      if (item == null) {
        throw new NullPointerException("Null element found in list: " + data);
      }
      numberValues.add(getNumberObject(item, itemOI));
    }
    return numberValues;
  }

  public static Object getNumberObject(String data, ObjectInspector objectInspector) {
    String hiveType = objectInspector.getTypeName();
    if (hiveType.equals(serdeConstants.BIGINT_TYPE_NAME)) {
      return Long.parseLong(data);
    } else if (hiveType.equals(serdeConstants.DOUBLE_TYPE_NAME)) {
      return Double.parseDouble(data);
    }
    throw new IllegalArgumentException("Unsupported Hive type: " + hiveType);
  }

  public static Object getListObject(List<AttributeValue> data, ObjectInspector objectInspector) {
    ListObjectInspector listOI = (ListObjectInspector) objectInspector;
    ObjectInspector elementOI = listOI.getListElementObjectInspector();
    HiveDynamoDBType elementType = HiveDynamoDBListTypeFactory.getTypeObjectFromHiveType(elementOI);

    List<Object> values = new ArrayList<>();
    for (AttributeValue av : data) {
      values.add(elementType.getHiveData(av, elementOI));
    }

    return values;
  }

  public static Object getMapObject(Map<String, AttributeValue> data, ObjectInspector objectInspector) {
    MapObjectInspector mapOI = (MapObjectInspector) objectInspector;
    ObjectInspector mapValueOI = mapOI.getMapValueObjectInspector();
    HiveDynamoDBType valueType = HiveDynamoDBTypeFactory.getTypeObjectFromHiveType(mapValueOI);

    Map<String, Object> values = new HashMap<>();
    for (Map.Entry<String, AttributeValue> entry : data.entrySet()) {
      values.put(entry.getKey(), valueType.getHiveData(entry.getValue(), mapValueOI));
    }

    return values;
  }
}
