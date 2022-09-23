package org.apache.hadoop.dynamodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

public class DynamoDBSimpleJsonWritableTest {

  private DynamoDBItemWritable item;

  @Before
  public void setup() {
    item = new DynamoDBSimpleJsonWritable();
    setTestData();
  }

  @Test
  public void testSerialization() throws IOException {
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(outStream);
    item.write(out);
    outStream.close();

    String jsonString = item.readStringFromDataInput(
        new DataInputStream(IOUtils.toInputStream(outStream.toString())));
    checkReturnedItem(jsonString);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testDeserializationNotSupported() throws IOException {
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(outStream);
    item.write(out);
    outStream.close();

    String data = outStream.toString();

    item.setItem(null);
    assertNull(item.getItem());

    item.readFields(new DataInputStream(IOUtils.toInputStream(data)));
  }

  private void checkReturnedItem(String jsonString) throws IOException {
    assertEquals("{\"ss\":[],\"s\":\"test\",\"ns\":[1.0,1.10,2.0],\"l\":[\"1.0\",\"0\"],\"n\":1234}", jsonString);

    ObjectMapper mapper = new ObjectMapper();
    JsonNode returnedData = mapper.readTree(jsonString);

    assertEquals(5, returnedData.size());
    assertEquals("test", returnedData.get("s").asText());
    assertEquals(1234, returnedData.get("n").intValue());
    assertEquals(0, returnedData.get("ss").size());
    assertEquals(3, returnedData.get("ns").size());
    assertEquals(2, returnedData.get("l").size());

    List<JsonNode> ns = returnedData.findValues("ns");
    assertEquals("[1.0,1.1,2.0]", ns.get(0).toString());

    List<JsonNode> l = returnedData.findValues("l");
    assertEquals("[\"1.0\",\"0\"]", l.get(0).toString());
    assertEquals("1.0", l.get(0).get(0).asText());
    assertEquals("0", l.get(0).get(1).asText());
  }

  private void setTestData() {
    List<String> ss = new ArrayList<>();
    List<String> ns = new ArrayList<>();
    ns.add("1.0");
    ns.add("1.10");
    ns.add("2.0");
    List<AttributeValue> l = new ArrayList<>();
    l.add(new AttributeValue("1.0"));
    l.add(new AttributeValue("0"));

    Map<String, AttributeValue> sampleData = new HashMap<>();
    sampleData.put("s", new AttributeValue().withS("test"));
    sampleData.put("n", new AttributeValue().withN("1234"));
    sampleData.put("ss", new AttributeValue().withSS(ss));
    sampleData.put("ns", new AttributeValue().withNS(ns));
    sampleData.put("l", new AttributeValue().withL(l));

    item.setItem(sampleData);
  }
}
