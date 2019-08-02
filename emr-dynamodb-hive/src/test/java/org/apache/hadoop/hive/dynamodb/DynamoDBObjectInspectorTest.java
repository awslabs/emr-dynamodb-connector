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
import org.apache.hadoop.dynamodb.test.DynamoDBTestUtils;
import org.apache.hadoop.dynamodb.type.DynamoDBTypeConstants;
import org.apache.hadoop.hive.dynamodb.type.HiveDynamoDBType;
import org.apache.hadoop.hive.dynamodb.type.HiveDynamoDBTypeFactory;
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

  private static final List<String> PRIMITIVE_FIELDS = Lists.newArrayList("animal", "height", "weight", "endangered");
  private static final List<TypeInfo> PRIMITIVE_TYPE_INFOS = Lists.newArrayList(STRING_TYPE_INFO,
      DOUBLE_TYPE_INFO, LONG_TYPE_INFO, BOOLEAN_TYPE_INFO);
  private static final List<String> PRIMITIVE_STRING_DATA = Lists.newArrayList("giraffe", "5.5", "1360", "true");
  private static final TypeInfo PRIMITIVE_STRUCT_TYPE_INFO = TypeInfoFactory
      .getStructTypeInfo(PRIMITIVE_FIELDS, PRIMITIVE_TYPE_INFOS);

  @Test
  public void testPrimitives() {
    List<String> attributeNames = PRIMITIVE_FIELDS;
    List<TypeInfo> colTypeInfos = PRIMITIVE_TYPE_INFOS;

    List<String> data = PRIMITIVE_STRING_DATA;

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
  public void testNull() {
    List<String> attributeNames = PRIMITIVE_FIELDS.subList(0, 2);
    List<TypeInfo> colTypeInfos = PRIMITIVE_TYPE_INFOS.subList(0, 2);

    List<String> data = Lists.newArrayList(PRIMITIVE_STRING_DATA.subList(0, 2));
    data.set(1, null);

    List<Object> expectedRowData = Lists.newArrayList();
    expectedRowData.addAll(data);

    Map<String, AttributeValue> itemMap = Maps.newHashMap();
    itemMap.put(attributeNames.get(0), new AttributeValue(data.get(0)));
    itemMap.put(attributeNames.get(1), new AttributeValue().withNULL(true));
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

    // alternate mapping
    Map<String, HiveDynamoDBType> typeMapping = Maps.newHashMap();
    typeMapping.put(attributeNames.get(1),
        HiveDynamoDBTypeFactory.getTypeObjectFromDynamoDBType(DynamoDBTypeConstants.STRING_SET));
    items = items.subList(0, 3);
    itemMap.put(attributeNames.get(1), new AttributeValue().withSS(items));
    expectedRowData.set(1, items);
    actualRowData = getDeserializedRow(attributeNames, colTypeInfos, typeMapping, itemMap);

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

    // alternate mapping
    attributeNames.add("names");
    colTypeInfos.add(STRING_MAP_TYPE_INFO);
    Map<String, HiveDynamoDBType> altTypeMapping = Maps.newHashMap();
    altTypeMapping.put(attributeNames.get(3),
        HiveDynamoDBTypeFactory.getTypeObjectFromDynamoDBType(DynamoDBTypeConstants.MAP));
    Map<String, String> names = Maps.newHashMap();
    Map<String, AttributeValue> namesAV = Maps.newHashMap();
    for (String person : people) {
      names.put(person, person);
      namesAV.put(person, new AttributeValue(person));
    }
    itemMap.put(attributeNames.get(3), new AttributeValue().withM(namesAV));
    expectedRowData.add(names);
    actualRowData = getDeserializedRow(attributeNames, colTypeInfos, altTypeMapping, itemMap);

    assertEquals(expectedRowData, actualRowData);
  }

  @Test
  public void testStruct() {
    List<String> attributeNames = Lists.newArrayList("struct", "data");
    List<TypeInfo> colTypeInfos = Lists.newArrayList(STRING_TYPE_INFO, PRIMITIVE_STRUCT_TYPE_INFO);

    String struct = "animal";
    List<String> data = PRIMITIVE_STRING_DATA;

    List<Object> structData = Lists.newArrayList();
    structData.add(data.get(0));
    structData.add(Double.parseDouble(data.get(1)));
    structData.add(Long.parseLong(data.get(2)));
    structData.add(Boolean.valueOf(data.get(3)));

    List<Object> expectedRowData = Lists.newArrayList();
    expectedRowData.add(struct);
    expectedRowData.add(structData);

    Map<String, AttributeValue> dataAV = Maps.newHashMap();
    dataAV.put(PRIMITIVE_FIELDS.get(0), new AttributeValue(data.get(0)));
    dataAV.put(PRIMITIVE_FIELDS.get(1), new AttributeValue().withN(data.get(1)));
    dataAV.put(PRIMITIVE_FIELDS.get(2), new AttributeValue().withN(data.get(2)));
    dataAV.put(PRIMITIVE_FIELDS.get(3), new AttributeValue().withBOOL(Boolean.valueOf(data.get(3))));

    Map<String, AttributeValue> itemMap = Maps.newHashMap();
    itemMap.put(attributeNames.get(0), new AttributeValue(struct));
    itemMap.put(attributeNames.get(1), new AttributeValue().withM(dataAV));

    List<Object> actualRowData = getDeserializedRow(attributeNames, colTypeInfos, itemMap);

    assertEquals(expectedRowData, actualRowData);
  }

  @Test
  public void testItem() {
    List<String> colNames = Lists.newArrayList("ddbitem");
    List<TypeInfo> colTypeInfos = Lists.newArrayList(STRING_MAP_TYPE_INFO);

    List<String> attributeNames = PRIMITIVE_FIELDS;
    List<String> attributeTypes = DynamoDBTestUtils.toAttributeValueFieldFormatList(
        DynamoDBTypeConstants.STRING,
        DynamoDBTypeConstants.NUMBER,
        DynamoDBTypeConstants.NUMBER,
        DynamoDBTypeConstants.BOOLEAN
    );
    List<String> data = PRIMITIVE_STRING_DATA;
    Map<String, String> colItemMap = Maps.newHashMap();
    for (int i = 0; i < attributeNames.size(); i++) {
      String type = attributeTypes.get(i);
      Object value = data.get(i);
      if (type.equalsIgnoreCase(DynamoDBTypeConstants.BOOLEAN)) {
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
    return getDeserializedRow(attributeNames, colTypeInfos, Maps.<String, HiveDynamoDBType>newHashMap(), itemMap);
  }

  private List<Object> getDeserializedRow(List<String> attributeNames, List<TypeInfo> colTypeInfos,
                                          Map<String, HiveDynamoDBType> altTypeMapping,
                                          Map<String, AttributeValue> itemMap) {
    Map<String, String> colMapping = Maps.newHashMap();
    Map<String, HiveDynamoDBType> typeMapping = Maps.newHashMap();
    for (int i = 0; i < attributeNames.size(); i++) {
      String name = attributeNames.get(i);
      colMapping.put(name, name);

      HiveDynamoDBType ddType = altTypeMapping.containsKey(name) ? altTypeMapping.get(name) :
          HiveDynamoDBTypeFactory.getTypeObjectFromHiveType(colTypeInfos.get(i));
      typeMapping.put(name, ddType);
    }

    DynamoDBObjectInspector ddbOI = new DynamoDBObjectInspector(attributeNames, colTypeInfos, colMapping, typeMapping);
    return ddbOI.getStructFieldsDataAsList(new DynamoDBItemWritable(itemMap));
  }
}
