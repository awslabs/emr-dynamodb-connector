package org.apache.hadoop.hive.dynamodb;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import org.apache.hadoop.hive.dynamodb.type.HiveDynamoDBListTypeFactory;
import org.apache.hadoop.hive.dynamodb.type.HiveDynamoDBType;
import org.apache.hadoop.hive.dynamodb.type.HiveDynamoDBTypeFactory;
import org.apache.hadoop.hive.dynamodb.type.HiveDynamoDBTypeUtil;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
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
import org.junit.Test;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyPrimitiveObjectInspectorFactory
        .LAZY_DOUBLE_OBJECT_INSPECTOR;
import static org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyPrimitiveObjectInspectorFactory
        .LAZY_LONG_OBJECT_INSPECTOR;
import static org.junit.Assert.assertEquals;

public class HiveDynamoDBTypeUtilTest {

  private static final ObjectInspector STRING_OBJECT_INSPECTOR = PrimitiveObjectInspectorFactory
          .getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING);
  private static final ObjectInspector DOUBLE_OBJECT_INSPECTOR = PrimitiveObjectInspectorFactory
          .getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.DOUBLE);
  private static final ObjectInspector LONG_OBJECT_INSPECTOR = PrimitiveObjectInspectorFactory
          .getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.LONG);
  private static final ObjectInspector LONG_LIST_OBJECT_INSPECTOR = ObjectInspectorFactory
          .getStandardListObjectInspector(LONG_OBJECT_INSPECTOR);
  private static final ObjectInspector STRING_LIST_OBJECT_INSPECTOR = ObjectInspectorFactory
          .getStandardListObjectInspector(STRING_OBJECT_INSPECTOR);

  private static final List<String> TEST_STRINGS = Lists.newArrayList("123", "456", "7890", "98765", "4321");
  private static final List<Long> TEST_LONGS = new ArrayList<>();
  static {
    for (String l : TEST_STRINGS) {
      TEST_LONGS.add(Long.parseLong(l));
    }
  }
  private static final double TEST_DOUBLE = 123.45;

  @Test
  public void testString() {
    String val = TEST_STRINGS.get(0);
    HiveDynamoDBType ddType = HiveDynamoDBTypeFactory.getTypeObjectFromHiveType(serdeConstants.STRING_TYPE_NAME);
    AttributeValue expectedAV = new AttributeValue().withS(val);
    LazyString ls = new LazyString(LazyPrimitiveObjectInspectorFactory
            .getLazyStringObjectInspector(false, (byte) 0));
    initLazyObject(ls, val.getBytes(), 0, val.length());

    for (Object o : new Object[]{val, new Text(val), ls}) {
      AttributeValue actualAV = ddType.getDynamoDBData(o, STRING_OBJECT_INSPECTOR);
      assertEquals(expectedAV, actualAV);
      Object actualStr = ddType.getHiveData(actualAV, serdeConstants.STRING_TYPE_NAME);
      assertEquals(val, actualStr);
    }
  }

  /**
   * See: https://github.com/apache/hive/blob/ae008b79b5d52ed6a38875b73025a505725828eb/serde/src/test/org/apache/hadoop/hive/serde2/lazy/TestLazyPrimitive.java#L265
   */
  private static void initLazyObject(LazyObject lo, byte[] data, int start, int length) {
    ByteArrayRef b = new ByteArrayRef();
    b.setData(data);
    lo.init(b, start, length);
  }

  @Test
  public void testDouble() {
    double val = TEST_DOUBLE;
    String valString = Double.toString(val);
    HiveDynamoDBType ddType = HiveDynamoDBTypeFactory.getTypeObjectFromHiveType(serdeConstants.DOUBLE_TYPE_NAME);
    AttributeValue expectedAV = new AttributeValue().withN(valString);
    AttributeValue actualAV = ddType.getDynamoDBData(val, DOUBLE_OBJECT_INSPECTOR);
    assertEquals(expectedAV, actualAV);
    Object actualDouble = ddType.getHiveData(actualAV, serdeConstants.DOUBLE_TYPE_NAME);
    assertEquals(val, actualDouble);

    LazyDouble ld = new LazyDouble(LAZY_DOUBLE_OBJECT_INSPECTOR);
    initLazyObject(ld, valString.getBytes(), 0, valString.length());
    actualAV = ddType.getDynamoDBData(ld, LAZY_DOUBLE_OBJECT_INSPECTOR);
    actualDouble = ddType.getHiveData(actualAV, serdeConstants.DOUBLE_TYPE_NAME);
    assertEquals(val, actualDouble);
  }

  @Test
  public void testLong() {
    long val = TEST_LONGS.get(0);
    String valString = Long.toString(val);
    HiveDynamoDBType ddType = HiveDynamoDBTypeFactory.getTypeObjectFromHiveType(serdeConstants.BIGINT_TYPE_NAME);
    AttributeValue expectedAV = new AttributeValue().withN(valString);
    AttributeValue actualAV = ddType.getDynamoDBData(val, LONG_OBJECT_INSPECTOR);
    assertEquals(expectedAV, actualAV);
    Object actualLong = ddType.getHiveData(actualAV, serdeConstants.BIGINT_TYPE_NAME);
    assertEquals(val, actualLong);

    LazyLong ll = new LazyLong(LAZY_LONG_OBJECT_INSPECTOR);
    initLazyObject(ll, valString.getBytes(), 0, valString.length());
    actualAV = ddType.getDynamoDBData(ll, LAZY_LONG_OBJECT_INSPECTOR);
    assertEquals(expectedAV, actualAV);
    actualLong = ddType.getHiveData(actualAV, serdeConstants.BIGINT_TYPE_NAME);
    assertEquals(val, actualLong);
  }

  @Test
  public void testList() {
    List<AttributeValue> longAVList = new ArrayList<>();
    List<AttributeValue> strAVList = new ArrayList<>();
    for (String str : TEST_STRINGS) {
      longAVList.add(new AttributeValue().withN(str));
      strAVList.add(new AttributeValue(str));
    }
    HiveDynamoDBType ddType = HiveDynamoDBListTypeFactory
            .getTypeObjectFromHiveType(DerivedHiveTypeConstants.BIGINT_ARRAY_TYPE_NAME);
    AttributeValue expectedAV = new AttributeValue().withL(longAVList);
    AttributeValue actualAV = ddType.getDynamoDBData(TEST_LONGS, LONG_LIST_OBJECT_INSPECTOR);
    assertEquals(expectedAV, actualAV);
    Object actualList = ddType.getHiveData(actualAV, DerivedHiveTypeConstants.BIGINT_ARRAY_TYPE_NAME);
    assertEquals(TEST_LONGS, actualList);

    ddType = HiveDynamoDBListTypeFactory.getTypeObjectFromHiveType(DerivedHiveTypeConstants.STRING_ARRAY_TYPE_NAME);
    expectedAV = new AttributeValue().withL(strAVList);
    actualAV = ddType.getDynamoDBData(TEST_STRINGS, STRING_LIST_OBJECT_INSPECTOR);
    assertEquals(expectedAV, actualAV);
    actualList = ddType.getHiveData(actualAV, DerivedHiveTypeConstants.STRING_ARRAY_TYPE_NAME);
    assertEquals(TEST_STRINGS, actualList);
  }

  @Test
  public void testSet() {
    HiveDynamoDBType ddType = HiveDynamoDBTypeFactory
            .getTypeObjectFromHiveType(DerivedHiveTypeConstants.BIGINT_ARRAY_TYPE_NAME);
    AttributeValue expectedAV = new AttributeValue().withNS(TEST_STRINGS);
    AttributeValue actualAV = ddType.getDynamoDBData(TEST_LONGS, LONG_LIST_OBJECT_INSPECTOR);
    assertEquals(expectedAV, actualAV);
    Object actualList = ddType.getHiveData(actualAV, DerivedHiveTypeConstants.BIGINT_ARRAY_TYPE_NAME);
    assertEquals(TEST_LONGS, actualList);

    ddType = HiveDynamoDBTypeFactory.getTypeObjectFromHiveType(DerivedHiveTypeConstants.STRING_ARRAY_TYPE_NAME);
    expectedAV = new AttributeValue().withSS(TEST_STRINGS);
    actualAV = ddType.getDynamoDBData(TEST_STRINGS, STRING_LIST_OBJECT_INSPECTOR);
    assertEquals(expectedAV, actualAV);
    actualList = ddType.getHiveData(actualAV, DerivedHiveTypeConstants.STRING_ARRAY_TYPE_NAME);
    assertEquals(TEST_STRINGS, actualList);
  }

  @Test
  public void testMap() {
    Map<String, Object> aMap = new HashMap<String, Object>();
    Map<String, AttributeValue> exMap = new HashMap<String, AttributeValue>();
    aMap.put("A", "B");
    exMap.put("A", new AttributeValue().withS("B"));
    aMap.put("C", "D");
    exMap.put("C", new AttributeValue().withS("D"));
    AttributeValue expected = new AttributeValue().withM(exMap);
    AttributeValue actual = HiveDynamoDBTypeUtil.parseObject(aMap);
    assertEquals(expected, actual);
  }

  @Test
  public void testMapDouble() {
    Map<String, Object> aMap = new HashMap<String, Object>();
    Map<String, AttributeValue> exMap = new HashMap<String, AttributeValue>();
    LazyDouble ld1 = new LazyDouble(LAZY_DOUBLE_OBJECT_INSPECTOR);
    initLazyObject(ld1, "1.0".getBytes(), 0, "1.0".length());
    aMap.put("A", ld1);
    exMap.put("A", new AttributeValue().withN("1.0"));
    LazyDouble ld2 = new LazyDouble(LAZY_DOUBLE_OBJECT_INSPECTOR);
    initLazyObject(ld2, "2".getBytes(), 0, "2".length());
    aMap.put("B", ld2);
    exMap.put("B", new AttributeValue().withN("2.0"));
    AttributeValue expected = new AttributeValue().withM(exMap);
    AttributeValue actual = HiveDynamoDBTypeUtil.parseObject(aMap);
    assertEquals(expected, actual);
  }

  @Test(expected = RuntimeException.class)
  public void testMapExcept() {
    Map<String, Object> aMap = new HashMap<String, Object>();
    aMap.put("Good", "good");
    aMap.put("Bad", false);
    HiveDynamoDBTypeUtil.parseObject(aMap);
  }
}
