package org.apache.hadoop.hive.dynamodb;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import org.apache.hadoop.hive.dynamodb.type.HiveDynamoDBTypeUtil;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyDouble;
import org.apache.hadoop.hive.serde2.lazy.LazyObject;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyPrimitiveObjectInspectorFactory.LAZY_DOUBLE_OBJECT_INSPECTOR;
import static org.junit.Assert.assertEquals;

public class HiveDynamoDBTypeUtilTest {

  @Test
  public void testString() {
    String val = "boblablah";
    AttributeValue expected = new AttributeValue().withS(val);
    for (Object o : new Object[]{val, new Text(val)}){
      AttributeValue actual = HiveDynamoDBTypeUtil.parseObject(o);
      assertEquals(expected, actual);
    }
  }

  /**
   * See: https://github.com/apache/hive/blob/ae008b79b5d52ed6a38875b73025a505725828eb/serde/src/test/org/apache/hadoop/hive/serde2/lazy/TestLazyPrimitive.java#L265
   */
  public static void initLazyObject(LazyObject lo, byte[] data, int start,
                                    int length) {
    ByteArrayRef b = new ByteArrayRef();
    b.setData(data);
    lo.init(b, start, length);
  }

  @Test
  public void testNumber() {
    String val = "123.45";
    AttributeValue expected = new AttributeValue().withN(val);
    LazyDouble ld = new LazyDouble(LAZY_DOUBLE_OBJECT_INSPECTOR);
    initLazyObject(ld, val.getBytes(), 0, val.length());
    AttributeValue actual = HiveDynamoDBTypeUtil.parseObject(ld);
    assertEquals(expected, actual);
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
