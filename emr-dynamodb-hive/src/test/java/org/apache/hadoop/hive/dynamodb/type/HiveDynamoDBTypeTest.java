/**
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

import org.apache.hadoop.dynamodb.test.DynamoDBTestUtils;
import org.apache.hadoop.dynamodb.type.DynamoDBTypeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyBoolean;
import org.apache.hadoop.hive.serde2.lazy.LazyDouble;
import org.apache.hadoop.hive.serde2.lazy.LazyLong;
import org.apache.hadoop.hive.serde2.lazy.LazyObject;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyPrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.json.JSONObject;
import org.junit.Test;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import static org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyPrimitiveObjectInspectorFactory.LAZY_BOOLEAN_OBJECT_INSPECTOR;
import static org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyPrimitiveObjectInspectorFactory.LAZY_DOUBLE_OBJECT_INSPECTOR;
import static org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyPrimitiveObjectInspectorFactory.LAZY_LONG_OBJECT_INSPECTOR;
import static org.junit.Assert.assertEquals;

public class HiveDynamoDBTypeTest {

  private static final ObjectInspector STRING_OBJECT_INSPECTOR = PrimitiveObjectInspectorFactory
      .getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING);
  private static final ObjectInspector DOUBLE_OBJECT_INSPECTOR = PrimitiveObjectInspectorFactory
      .getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.DOUBLE);
  private static final ObjectInspector LONG_OBJECT_INSPECTOR = PrimitiveObjectInspectorFactory
      .getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.LONG);
  private static final ObjectInspector BOOLEAN_OBJECT_INSPECTOR = PrimitiveObjectInspectorFactory
      .getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN);
  private static final ObjectInspector LONG_LIST_OBJECT_INSPECTOR = ObjectInspectorFactory
      .getStandardListObjectInspector(LONG_OBJECT_INSPECTOR);
  private static final ObjectInspector STRING_LIST_OBJECT_INSPECTOR = ObjectInspectorFactory
      .getStandardListObjectInspector(STRING_OBJECT_INSPECTOR);
  private static final ObjectInspector STRING_MAP_OBJECT_INSPECTOR = ObjectInspectorFactory
      .getStandardMapObjectInspector(STRING_OBJECT_INSPECTOR, STRING_OBJECT_INSPECTOR);
  private static final ObjectInspector LONG_MAP_OBJECT_INSPECTOR = ObjectInspectorFactory
      .getStandardMapObjectInspector(STRING_OBJECT_INSPECTOR, LONG_OBJECT_INSPECTOR);

  private static final List<String> STRING_LIST = Lists.newArrayList("123", "456", "7890", "98765", "4321");
  private static final List<Long> LONG_LIST = new ArrayList<>();
  private static final Map<String, String> STRING_MAP = new HashMap<>();
  private static final Map<String, Long> LONG_MAP = new HashMap<>();
  static {
    for (String l : STRING_LIST) {
      LONG_LIST.add(Long.parseLong(l));
      STRING_MAP.put(l, l + l);
      LONG_MAP.put(l, Long.parseLong(l + l));
    }
  }
  private static final double TEST_DOUBLE = 123.45;

  /**
   * See: https://github.com/apache/hive/blob/ae008b79b5d52ed6a38875b73025a505725828eb/serde/src/test/org/apache/hadoop/hive/serde2/lazy/TestLazyPrimitive.java#L265
   */
  private static void initLazyObject(LazyObject lo, byte[] data, int start, int length) {
    ByteArrayRef b = new ByteArrayRef();
    b.setData(data);
    lo.init(b, start, length);
  }

  @Test
  public void testString() {
    String val = STRING_LIST.get(0);
    HiveDynamoDBType ddType = HiveDynamoDBTypeFactory.getTypeObjectFromHiveType(STRING_OBJECT_INSPECTOR);
    AttributeValue expectedAV = AttributeValue.fromS(val);
    LazyString ls = new LazyString(LazyPrimitiveObjectInspectorFactory
        .getLazyStringObjectInspector(false, (byte) 0));
    initLazyObject(ls, val.getBytes(), 0, val.length());

    for (Object o : new Object[]{val, new Text(val), ls}) {
      AttributeValue actualAV = ddType.getDynamoDBData(o, STRING_OBJECT_INSPECTOR, false);
      assertEquals(expectedAV, actualAV);
      Object actualStr = ddType.getHiveData(actualAV, STRING_OBJECT_INSPECTOR);
      assertEquals(val, actualStr);
    }
  }

  @Test
  public void testDouble() {
    double val = TEST_DOUBLE;
    String valString = Double.toString(val);
    HiveDynamoDBType ddType = HiveDynamoDBTypeFactory.getTypeObjectFromHiveType(DOUBLE_OBJECT_INSPECTOR);
    AttributeValue expectedAV = AttributeValue.fromN(valString);
    AttributeValue actualAV = ddType.getDynamoDBData(val, DOUBLE_OBJECT_INSPECTOR, false);
    assertEquals(expectedAV, actualAV);
    Object actualDouble = ddType.getHiveData(actualAV, DOUBLE_OBJECT_INSPECTOR);
    assertEquals(val, actualDouble);

    LazyDouble ld = new LazyDouble(LAZY_DOUBLE_OBJECT_INSPECTOR);
    initLazyObject(ld, valString.getBytes(), 0, valString.length());
    actualAV = ddType.getDynamoDBData(ld, LAZY_DOUBLE_OBJECT_INSPECTOR, false);
    actualDouble = ddType.getHiveData(actualAV, DOUBLE_OBJECT_INSPECTOR);
    assertEquals(val, actualDouble);
  }

  @Test
  public void testLong() {
    long val = LONG_LIST.get(0);
    String valString = Long.toString(val);
    HiveDynamoDBType ddType = HiveDynamoDBTypeFactory.getTypeObjectFromHiveType(LONG_OBJECT_INSPECTOR);
    AttributeValue expectedAV = AttributeValue.fromN(valString);
    AttributeValue actualAV = ddType.getDynamoDBData(val, LONG_OBJECT_INSPECTOR, false);
    assertEquals(expectedAV, actualAV);
    Object actualLong = ddType.getHiveData(actualAV, LONG_OBJECT_INSPECTOR);
    assertEquals(val, actualLong);

    LazyLong ll = new LazyLong(LAZY_LONG_OBJECT_INSPECTOR);
    initLazyObject(ll, valString.getBytes(), 0, valString.length());
    actualAV = ddType.getDynamoDBData(ll, LAZY_LONG_OBJECT_INSPECTOR, false);
    assertEquals(expectedAV, actualAV);
    actualLong = ddType.getHiveData(actualAV, LONG_OBJECT_INSPECTOR);
    assertEquals(val, actualLong);
  }

  @Test
  public void testBoolean() {
    boolean val = true;
    String valString = Boolean.toString(val);
    HiveDynamoDBType ddType = HiveDynamoDBTypeFactory.getTypeObjectFromHiveType(BOOLEAN_OBJECT_INSPECTOR);
    AttributeValue expectedAV = AttributeValue.fromBool(val);
    AttributeValue actualAV = ddType.getDynamoDBData(val, BOOLEAN_OBJECT_INSPECTOR, false);
    assertEquals(expectedAV, actualAV);
    Object actualBoolean = ddType.getHiveData(actualAV, BOOLEAN_OBJECT_INSPECTOR);
    assertEquals(val, actualBoolean);

    LazyBoolean lb = new LazyBoolean(LAZY_BOOLEAN_OBJECT_INSPECTOR);
    initLazyObject(lb, valString.getBytes(), 0, valString.length());
    actualAV = ddType.getDynamoDBData(lb, LAZY_BOOLEAN_OBJECT_INSPECTOR, false);
    actualBoolean = ddType.getHiveData(actualAV, BOOLEAN_OBJECT_INSPECTOR);
    assertEquals(val, actualBoolean);
  }

  @Test
  public void testList() {
    List<AttributeValue> longAVList = new ArrayList<>();
    List<AttributeValue> strAVList = new ArrayList<>();
    for (String str : STRING_LIST) {
      longAVList.add(AttributeValue.fromN(str));
      strAVList.add(AttributeValue.fromS(str));
    }
    HiveDynamoDBType ddType = HiveDynamoDBTypeFactory.getTypeObjectFromHiveType(LONG_LIST_OBJECT_INSPECTOR);
    AttributeValue expectedAV = AttributeValue.fromL(longAVList);
    AttributeValue actualAV = ddType.getDynamoDBData(LONG_LIST, LONG_LIST_OBJECT_INSPECTOR, false);
    assertEquals(expectedAV, actualAV);
    Object actualList = ddType.getHiveData(actualAV, LONG_LIST_OBJECT_INSPECTOR);
    assertEquals(LONG_LIST, actualList);

    ddType = HiveDynamoDBTypeFactory.getTypeObjectFromHiveType(STRING_LIST_OBJECT_INSPECTOR);
    expectedAV = AttributeValue.fromL(strAVList);
    actualAV = ddType.getDynamoDBData(STRING_LIST, STRING_LIST_OBJECT_INSPECTOR, false);
    assertEquals(expectedAV, actualAV);
    actualList = ddType.getHiveData(actualAV, STRING_LIST_OBJECT_INSPECTOR);
    assertEquals(STRING_LIST, actualList);
  }

  @Test
  public void testSet() {
    HiveDynamoDBType ddType = HiveDynamoDBTypeFactory.getTypeObjectFromDynamoDBType(DynamoDBTypeConstants.NUMBER_SET);
    AttributeValue expectedAV = AttributeValue.fromNs(STRING_LIST);
    AttributeValue actualAV = ddType.getDynamoDBData(LONG_LIST, LONG_LIST_OBJECT_INSPECTOR, false);
    assertEquals(expectedAV, actualAV);
    Object actualList = ddType.getHiveData(actualAV, LONG_LIST_OBJECT_INSPECTOR);
    assertEquals(LONG_LIST, actualList);

    ddType = HiveDynamoDBTypeFactory.getTypeObjectFromDynamoDBType(DynamoDBTypeConstants.STRING_SET);
    expectedAV = AttributeValue.fromSs(STRING_LIST);
    actualAV = ddType.getDynamoDBData(STRING_LIST, STRING_LIST_OBJECT_INSPECTOR, false);
    assertEquals(expectedAV, actualAV);
    actualList = ddType.getHiveData(actualAV, STRING_LIST_OBJECT_INSPECTOR);
    assertEquals(STRING_LIST, actualList);
  }

  @Test
  public void testItem() throws SerDeException {
    Map<String, String> hiveStringItem = new HashMap<>();
    Map<String, AttributeValue> expectedStringItem = new HashMap<>();
    Map<String, String> hiveNumberItem = new HashMap<>();
    Map<String, AttributeValue> expectedNumberItem = new HashMap<>();
    for (String str : STRING_MAP.keySet()) {
      String avStringField = DynamoDBTestUtils.toAttributeValueFieldFormat(DynamoDBTypeConstants.STRING);
      hiveStringItem.put(str, new JSONObject().put(avStringField, STRING_MAP.get(str)).toString());
      expectedStringItem.put(str, AttributeValue.fromS(STRING_MAP.get(str)));

      String avNumberField = DynamoDBTestUtils.toAttributeValueFieldFormat(DynamoDBTypeConstants.NUMBER);
      hiveNumberItem.put(str, new JSONObject().put(avNumberField, STRING_MAP.get(str)).toString());
      expectedNumberItem.put(str, AttributeValue.fromN(STRING_MAP.get(str)));
    }

    HiveDynamoDBItemType ddType = (HiveDynamoDBItemType) HiveDynamoDBTypeFactory
        .getTypeObjectFromHiveType(STRING_MAP_OBJECT_INSPECTOR);
    Map<String, AttributeValue> actualItem = ddType.parseDynamoDBData(hiveStringItem, STRING_MAP_OBJECT_INSPECTOR);
    assertEquals(expectedStringItem, actualItem);
    Map<String, String> actualMap = ddType.buildHiveData(actualItem);
    assertEquals(hiveStringItem, actualMap);

    actualItem = ddType.parseDynamoDBData(hiveNumberItem, STRING_MAP_OBJECT_INSPECTOR);
    assertEquals(expectedNumberItem, actualItem);
    actualMap = ddType.buildHiveData(actualItem);
    assertEquals(hiveNumberItem, actualMap);
  }

  @Test
  public void testMap() {
    Map<String, AttributeValue> longAVMap = new HashMap<>();
    Map<String, AttributeValue> stringAVMap = new HashMap<>();
    for (String str : STRING_MAP.keySet()) {
      longAVMap.put(str, AttributeValue.fromN(STRING_MAP.get(str)));
      stringAVMap.put(str, AttributeValue.fromS(STRING_MAP.get(str)));
    }
    HiveDynamoDBType ddType = HiveDynamoDBTypeFactory.getTypeObjectFromHiveType(LONG_MAP_OBJECT_INSPECTOR);
    AttributeValue expectedAV = AttributeValue.fromM(longAVMap);
    AttributeValue actualAV = ddType.getDynamoDBData(LONG_MAP, LONG_MAP_OBJECT_INSPECTOR, false);
    assertEquals(expectedAV, actualAV);
    Object actualMap = ddType.getHiveData(actualAV, LONG_MAP_OBJECT_INSPECTOR);
    assertEquals(LONG_MAP, actualMap);

    ddType = HiveDynamoDBTypeFactory.getTypeObjectFromDynamoDBType(DynamoDBTypeConstants.MAP);
    expectedAV = AttributeValue.fromM(stringAVMap);
    actualAV = ddType.getDynamoDBData(STRING_MAP, STRING_MAP_OBJECT_INSPECTOR, false);
    assertEquals(expectedAV, actualAV);
    actualMap = ddType.getHiveData(actualAV, STRING_MAP_OBJECT_INSPECTOR);
    assertEquals(STRING_MAP, actualMap);
  }

  @Test
  public void testStruct() {
    List<Object> struct = Lists.newArrayList((Object) STRING_LIST.get(0), LONG_LIST.get(1), TEST_DOUBLE);
    Map<String, AttributeValue> structAVMap = new HashMap<>();
    structAVMap.put(STRING_LIST.get(0), AttributeValue.fromS(STRING_LIST.get(0)));
    structAVMap.put(STRING_LIST.get(1), AttributeValue.fromN(Long.toString(LONG_LIST.get(1))));
    structAVMap.put(STRING_LIST.get(2), AttributeValue.fromN(Double.toString(TEST_DOUBLE)));
    List<String> structFieldNames = STRING_LIST.subList(0, 3);
    List<ObjectInspector> structFieldOIs = Lists.newArrayList(STRING_OBJECT_INSPECTOR, LONG_OBJECT_INSPECTOR,
            DOUBLE_OBJECT_INSPECTOR);
    ObjectInspector structObjectInspector = ObjectInspectorFactory
            .getStandardStructObjectInspector(structFieldNames, structFieldOIs);

    HiveDynamoDBType ddType = HiveDynamoDBTypeFactory.getTypeObjectFromHiveType(structObjectInspector);
    AttributeValue expectedAV = AttributeValue.fromM(structAVMap);
    AttributeValue actualAV = ddType.getDynamoDBData(struct, structObjectInspector, false);
    assertEquals(expectedAV, actualAV);
    Object actualStruct = ddType.getHiveData(actualAV, structObjectInspector);
    assertEquals(struct, actualStruct);
  }

  @Test
  public void testMultipleTypeList() {
    List<AttributeValue> avList = new ArrayList<>();
    avList.add(AttributeValue.fromS(STRING_LIST.get(0)));
    avList.add(AttributeValue.fromN(STRING_LIST.get(0)));
    AttributeValue av = AttributeValue.fromL(avList);

    HiveDynamoDBType ddType = HiveDynamoDBTypeFactory.getTypeObjectFromHiveType(STRING_LIST_OBJECT_INSPECTOR);
    List<String> expectedStringList = Lists.newArrayList(STRING_LIST.get(0), null);
    Object actualList = ddType.getHiveData(av, STRING_LIST_OBJECT_INSPECTOR);
    assertEquals(expectedStringList, actualList);

    ddType = HiveDynamoDBTypeFactory.getTypeObjectFromHiveType(LONG_LIST_OBJECT_INSPECTOR);
    List<Long> expectedLongList = Lists.newArrayList(null, LONG_LIST.get(0));
    actualList = ddType.getHiveData(av, LONG_LIST_OBJECT_INSPECTOR);
    assertEquals(expectedLongList, actualList);
  }

  @Test
  public void testNullSerialization() {
    String val = null;
    HiveDynamoDBType ddType = HiveDynamoDBTypeFactory.getTypeObjectFromHiveType(STRING_OBJECT_INSPECTOR);
    AttributeValue expectedAV = AttributeValue.fromNul(true);
    AttributeValue actualAV = ddType.getDynamoDBData(val, STRING_OBJECT_INSPECTOR, true);
    assertEquals(expectedAV, actualAV);
    Object actualNull = ddType.getHiveData(actualAV, STRING_OBJECT_INSPECTOR);
    assertEquals(val, actualNull);

    List<String> nullStringList = new ArrayList<>(STRING_LIST);
    nullStringList.set(0, null);
    List<AttributeValue> strAVList = new ArrayList<>();
    strAVList.add(AttributeValue.fromNul(true));
    for (int i = 1; i < nullStringList.size(); i++) {
      strAVList.add(AttributeValue.fromS(nullStringList.get(i)));
    }

    ddType = HiveDynamoDBTypeFactory.getTypeObjectFromHiveType(STRING_LIST_OBJECT_INSPECTOR);
    expectedAV = AttributeValue.fromL(strAVList);
    actualAV = ddType.getDynamoDBData(nullStringList, STRING_LIST_OBJECT_INSPECTOR, true);
    assertEquals(expectedAV, actualAV);
    Object actualList = ddType.getHiveData(actualAV, STRING_LIST_OBJECT_INSPECTOR);
    assertEquals(nullStringList, actualList);
  }
}
