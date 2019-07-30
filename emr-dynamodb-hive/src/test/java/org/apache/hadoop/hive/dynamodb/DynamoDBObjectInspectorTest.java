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

package org.apache.hadoop.hive.dynamodb;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import org.apache.hadoop.dynamodb.DynamoDBItemWritable;
import org.apache.hadoop.hive.dynamodb.DynamoDBObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.json.JSONObject;
import org.junit.Test;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class DynamoDBObjectInspectorTest {

  private static final TypeInfo STRING_TYPE_INFO = TypeInfoFactory.stringTypeInfo;
  private static final TypeInfo DOUBLE_TYPE_INFO = TypeInfoFactory.doubleTypeInfo;
  private static final TypeInfo LONG_TYPE_INFO = TypeInfoFactory.longTypeInfo;
  private static final TypeInfo BOOLEAN_TYPE_INFO = TypeInfoFactory.booleanTypeInfo;
  private static final TypeInfo STRING_LIST_TYPE_INFO = TypeInfoFactory.getListTypeInfo(STRING_TYPE_INFO);
  private static final TypeInfo STRING_MAP_TYPE_INFO = TypeInfoFactory.getMapTypeInfo(STRING_TYPE_INFO,
          STRING_TYPE_INFO);
  private static final TypeInfo LONG_MAP_TYPE_INFO = TypeInfoFactory.getMapTypeInfo(STRING_TYPE_INFO,
          LONG_TYPE_INFO);
  private static final TypeInfo LIST_MAP_TYPE_INFO = TypeInfoFactory.getMapTypeInfo(STRING_TYPE_INFO,
          STRING_LIST_TYPE_INFO);

  @Test
  public void testPrimitives() {
    List<String> attributeNames = Lists.newArrayList("animal", "height", "weight", "endangered");
    List<TypeInfo> colTypeInfos = Lists.newArrayList(STRING_TYPE_INFO, DOUBLE_TYPE_INFO, LONG_TYPE_INFO,
            BOOLEAN_TYPE_INFO);

    List<String> data = Lists.newArrayList("giraffe", "5.5", "1360", "true");

    List<Object> expectedRowData = Lists.newArrayList();
    expectedRowData.add(data.get(0));
    expectedRowData.add(Double.parseDouble(data.get(1)));
    expectedRowData.add(Long.parseLong(data.get(2)));
    expectedRowData.add(Boolean.valueOf(data.get(3)));

    Map<String, AttributeValue> itemMap = Maps.newHashMap();
    itemMap.put(attributeNames.get(0), new AttributeValue(data.get(0)));
    itemMap.put(attributeNames.get(1), new AttributeValue().withN(data.get(1)));
    itemMap.put(attributeNames.get(2), new AttributeValue().withN(data.get(2)));
    itemMap.put(attributeNames.get(3), new AttributeValue().withBOOL(Boolean.valueOf(data.get(3))));
    List<Object> actualRowData = getDeserializedRow(attributeNames, colTypeInfos, itemMap);

    assertEquals(expectedRowData, actualRowData);
  }

  @Test
  public void testArray() {
    List<String> attributeNames = Lists.newArrayList("list", "items");
    List<TypeInfo> colTypeInfos = Lists.newArrayList(STRING_TYPE_INFO, STRING_LIST_TYPE_INFO);

    String list = "groceries";
    List<String> items = Lists.newArrayList("milk", "bread", "eggs", "milk");
    List<AttributeValue> itemsAV = Lists.newArrayList();
    for (String item : items) {
      itemsAV.add(new AttributeValue(item));
    }
    List<Object> expectedRowData = Lists.newArrayList(list, items);

    Map<String, AttributeValue> itemMap = Maps.newHashMap();
    itemMap.put(attributeNames.get(0), new AttributeValue(list));
    itemMap.put(attributeNames.get(1), new AttributeValue().withL(itemsAV));
    List<Object> actualRowData = getDeserializedRow(attributeNames, colTypeInfos, itemMap);

    assertEquals(expectedRowData, actualRowData);
  }

  @Test
  public void testMap() {
    List<String> attributeNames = Lists.newArrayList("map", "ids", "lists");
    List<TypeInfo> colTypeInfos = Lists.newArrayList(STRING_TYPE_INFO, LONG_MAP_TYPE_INFO, LIST_MAP_TYPE_INFO);

    String map = "people";
    List<String> people = Lists.newArrayList("bob", "linda", "tina", "gene", "louise");
    List<AttributeValue> peopleAV = Lists.newArrayList();
    Map<String, Long> ids = Maps.newHashMap();
    Map<String, AttributeValue> idsAV = Maps.newHashMap();
    Map<String, List<String>> lists = Maps.newHashMap();
    Map<String, AttributeValue> listsAV = Maps.newHashMap();
    for (int i = 0; i < people.size(); i++) {
      String person = people.get(i);
      long id = (long) i;

      peopleAV.add(new AttributeValue(person));
      ids.put(person, id);
      idsAV.put(person, new AttributeValue().withN(Long.toString(id)));
      lists.put(person, people.subList(0, i + 1));
      listsAV.put(person, new AttributeValue().withL(Lists.newArrayList(peopleAV)));
    }
    List<Object> expectedRowData = Lists.newArrayList(map, ids, lists);

    Map<String, AttributeValue> itemMap = Maps.newHashMap();
    itemMap.put(attributeNames.get(0), new AttributeValue(map));
    itemMap.put(attributeNames.get(1), new AttributeValue().withM(idsAV));
    itemMap.put(attributeNames.get(2), new AttributeValue().withM(listsAV));
    List<Object> actualRowData = getDeserializedRow(attributeNames, colTypeInfos, itemMap);

    assertEquals(expectedRowData, actualRowData);
  }

  @Test
  public void testItem() {
    List<String> colNames = Lists.newArrayList("ddbitem");
    List<TypeInfo> colTypeInfos = Lists.newArrayList(STRING_MAP_TYPE_INFO);

    List<String> attributeNames = Lists.newArrayList("animal", "height", "weight", "endangered");
    List<String> attributeTypes = Lists.newArrayList("s", "n", "n", "bOOL");
    List<String> data = Lists.newArrayList("giraffe", "5.5", "1360", "true");
    Map<String, String> colItemMap = Maps.newHashMap();
    for (int i = 0; i < attributeNames.size(); i++) {
      String type = attributeTypes.get(i);
      Object value = data.get(i);
      if (type.equalsIgnoreCase("bool")) {
        value = Boolean.valueOf(data.get(i));
      }
      colItemMap.put(attributeNames.get(i), new JSONObject().put(type, value).toString());
    }
    List<Object> expectedRowData = Lists.newArrayList();
    expectedRowData.add(colItemMap);

    Map<String, AttributeValue> itemMap = Maps.newHashMap();
    itemMap.put(attributeNames.get(0), new AttributeValue(data.get(0)));
    itemMap.put(attributeNames.get(1), new AttributeValue().withN(data.get(1)));
    itemMap.put(attributeNames.get(2), new AttributeValue().withN(data.get(2)));
    itemMap.put(attributeNames.get(3), new AttributeValue().withBOOL(Boolean.valueOf(data.get(3))));

    List<Object> actualRowData = getDeserializedRow(colNames, colTypeInfos, itemMap);

    assertEquals(expectedRowData, actualRowData);

    // backup item
    colNames.add(attributeNames.get(0));
    colTypeInfos.add(STRING_TYPE_INFO);
    expectedRowData.add(data.get(0));
    actualRowData = getDeserializedRow(colNames, colTypeInfos, itemMap);

    assertEquals(expectedRowData, actualRowData);
  }

  private List<Object> getDeserializedRow(List<String> attributeNames, List<TypeInfo> colTypeInfos,
                                          Map<String, AttributeValue> itemMap) {
    Map<String, String> colMapping = Maps.newHashMap();
    for (String name : attributeNames) {
      colMapping.put(name, name);
    }

    DynamoDBObjectInspector ddbOI = new DynamoDBObjectInspector(attributeNames, colTypeInfos, colMapping);
    return ddbOI.getStructFieldsDataAsList(new DynamoDBItemWritable(itemMap));
  }
}
