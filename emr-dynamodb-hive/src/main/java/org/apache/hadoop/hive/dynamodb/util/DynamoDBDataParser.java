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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.hive.dynamodb.DerivedHiveTypeConstants;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.lazy.LazyDouble;
import org.apache.hadoop.hive.serde2.lazy.LazyMap;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.io.BytesWritable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DynamoDBDataParser {
  private static final Log log = LogFactory.getLog(DynamoDBDataParser.class);

  public String getNumber(Object data, ObjectInspector objectInspector) {
    if (objectInspector instanceof DoubleObjectInspector) {
      return Double.toString(((DoubleObjectInspector) objectInspector).get(data));
    } else if (objectInspector instanceof LongObjectInspector) {
      return Long.toString(((LongObjectInspector) objectInspector).get(data));
    } else {
      throw new RuntimeException("Unknown object inspector type: " + objectInspector.getCategory()
          + " Type name: " + objectInspector.getTypeName());
    }
  }

  public Boolean getBoolean(Object data, ObjectInspector objectInspector) {
            return ((BooleanObjectInspector) objectInspector).get(data);
  }

  public String getString(Object data, ObjectInspector objectInspector) {
    return ((StringObjectInspector) objectInspector).getPrimitiveJavaObject(data);
  }

  public ByteBuffer getByteBuffer(Object data, ObjectInspector objectInspector) {
    if (objectInspector instanceof BinaryObjectInspector) {
      BytesWritable bw = ((BinaryObjectInspector) objectInspector).getPrimitiveWritableObject(data);
      byte[] result = new byte[bw.getLength()];
      System.arraycopy(bw.getBytes(), 0, result, 0, bw.getLength());
      return ByteBuffer.wrap(result);
    } else {
      throw new RuntimeException("Unknown object inspector type: " + objectInspector.getCategory()
          + " Type name: " + objectInspector.getTypeName());
    }
  }

  public Map<String, Object> getMap(Object data, ObjectInspector objectInspector) {
    if (objectInspector instanceof MapObjectInspector) {
      MapObjectInspector mapOI = ((MapObjectInspector) objectInspector);
      Map<?, ?> aMap = mapOI.getMap(data);
      Map<String, Object> item = new HashMap<String, Object>();
      StringObjectInspector mapKeyObjectInspector = (StringObjectInspector) mapOI
        .getMapKeyObjectInspector();

      // borrowed from HiveDynamoDbItemType
      for (Map.Entry<?,?> entry : aMap.entrySet()) {
        String dynamoDBAttributeName = mapKeyObjectInspector.getPrimitiveJavaObject(entry.getKey());
        Object dynamoDBAttributeValue = entry.getValue();
        item.put(dynamoDBAttributeName, dynamoDBAttributeValue);
      }
      return item;
    } else {
      throw new RuntimeException("Unknown object inspector type: " + objectInspector.getCategory()
        + " Type name: " + objectInspector.getTypeName());
    }
  }

  public List<Object> getListAttribute(Object data, ObjectInspector objectInspector, String
    ddType) {
    ListObjectInspector listObjectInspector = (ListObjectInspector) objectInspector;
    List<?> dataList = listObjectInspector.getList(data);

    if (dataList == null) {
      return null;
    }

    ObjectInspector itemObjectInspector = listObjectInspector.getListElementObjectInspector();
    List<Object> itemList = new ArrayList<Object>();
    // we know hive arrays cannot contain multiple types so we cache the first
    // one and assume all others are the same
    Class listType = null;
    for (Object dataItem : dataList) {
      if (dataItem == null) {
        throw new RuntimeException("Null element found in list: " + dataList);
      }

      if (ddType.equals("L")) {
        if (listType == LazyString.class || dataItem instanceof LazyString || dataItem instanceof String) {
          itemList.add(getString(dataItem, itemObjectInspector));
          listType = LazyString.class;
        } else if (listType == LazyMap.class || dataItem instanceof LazyMap || dataItem instanceof HashMap) {
          itemList.add(getMap(dataItem, itemObjectInspector));
          listType = LazyMap.class;
        } else {
          itemList.add(getNumber(dataItem, itemObjectInspector));
          listType = LazyDouble.class;
        }
      } else {
        throw new RuntimeException("Unsupported dynamodb type: " + ddType +
          " dataItem class: " + dataItem.getClass().getName());
      }
    }

    return itemList;
  }

  /**
   * This method currently supports StringSet and NumberSet data type of DynamoDB
   */
  public List<String> getSetAttribute(Object data, ObjectInspector objectInspector, String
      ddType) {
    ListObjectInspector listObjectInspector = (ListObjectInspector) objectInspector;
    List<?> dataList = listObjectInspector.getList(data);

    if (dataList == null) {
      return null;
    }

    ObjectInspector itemObjectInspector = listObjectInspector.getListElementObjectInspector();
    List<String> itemList = new ArrayList<String>();
    for (Object dataItem : dataList) {
      if (dataItem == null) {
        throw new RuntimeException("Null element found in list: " + dataList);
      }

      if (ddType.equals("SS")) {
        itemList.add(getString(dataItem, itemObjectInspector));
      } else if (ddType.equals("NS")) {
        itemList.add(getNumber(dataItem, itemObjectInspector));
      } else {
        throw new RuntimeException("Unsupported dynamodb type: " + ddType);
      }
    }

    return itemList;
  }

  /**
   * This method currently supports BinarySet data type of DynamoDB
   */
  public List<ByteBuffer> getByteBuffers(Object data, ObjectInspector objectInspector, String
      ddType) {
    ListObjectInspector listObjectInspector = (ListObjectInspector) objectInspector;
    List<?> dataList = listObjectInspector.getList(data);

    if (dataList == null) {
      return null;
    }

    ObjectInspector itemObjectInspector = listObjectInspector.getListElementObjectInspector();
    List<ByteBuffer> itemList = new ArrayList<ByteBuffer>();
    for (Object dataItem : dataList) {
      if (dataItem == null) {
        throw new RuntimeException("Null element found in list: " + dataList);
      }

      if (ddType.equals("BS")) {
        itemList.add(getByteBuffer(dataItem, itemObjectInspector));
      } else {
        throw new RuntimeException("Expecting BinarySet type: " + ddType);
      }
    }

    return itemList;
  }

  public Object getNumberObjectList(List<String> data, String hiveType) {
    List<Object> doubleValues = new ArrayList<Object>();
    if (data == null) {
      return null;
    }
    String hiveSubType;
    if (hiveType.equals(DerivedHiveTypeConstants.DOUBLE_ARRAY_TYPE_NAME)) {
      hiveSubType = Constants.DOUBLE_TYPE_NAME;
    } else {
      hiveSubType = Constants.BIGINT_TYPE_NAME;
    }
    for (String dataElement : data) {
      doubleValues.add(getNumberObject(dataElement, hiveSubType));
    }
    return doubleValues;
  }

  public Object getNumberObject(String data, String hiveType) {
    if (data != null) {
      if (hiveType.equals(Constants.BIGINT_TYPE_NAME)) {
        return Long.parseLong(data);
      } else {
        return Double.parseDouble(data);
      }
    } else {
      return null;
    }
  }
}
