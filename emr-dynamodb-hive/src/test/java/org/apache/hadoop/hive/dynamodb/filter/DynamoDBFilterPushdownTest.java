package org.apache.hadoop.hive.dynamodb.filter;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.dynamodb.filter.DynamoDBFilterOperator;
import org.apache.hadoop.dynamodb.filter.DynamoDBQueryFilter;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler.DecomposedPredicate;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DynamoDBFilterPushdownTest {

  private DynamoDBFilterPushdown dynamoDBFilterPushdown;

  private static final String HASH_KEY_NAME = "hashKey";
  private static final String HASH_KEY_TYPE = "HASH";
  private static final String HASH_KEY_VALUE = "1";
  private static final String RANGE_KEY_NAME = "rangeKey";
  private static final String RANGE_KEY_TYPE = "RANGE";
  private static final String RANGE_KEY_VALUE = "2";
  private static final String COLUMN1_NAME = "column1";
  private static final String COLUMN1_VALUE = "3";
  private static final String COLUMN2_NAME = "column2";
  private static final String COLUMN2_VALUE = "4";
  private static final String COLUMN3_NAME = "column3";
  private static final String COLUMN4_NAME = "column4";
  private static final String LOCAL_SECONDARY_INDEX_NAME = "LSI";
  private static final String GLOBAL_SECONDARY_INDEX_NAME = "GSI";

  private final ExprNodeDesc hashKeyPredicate = new ExprNodeGenericFuncDesc(
      TypeInfoFactory.booleanTypeInfo,
      new GenericUDFOPEqual(), Lists.newArrayList(
      new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, HASH_KEY_NAME, null, false),
      new ExprNodeConstantDesc(TypeInfoFactory.longTypeInfo, HASH_KEY_VALUE)
  ));
  private final ExprNodeDesc rangeKeyPredicate = new ExprNodeGenericFuncDesc(
      TypeInfoFactory.booleanTypeInfo,
      new GenericUDFOPEqual(), Lists.newArrayList(
      new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, RANGE_KEY_NAME, null, false),
      new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, RANGE_KEY_VALUE)
  ));
  private final ExprNodeDesc column1Predicate = new ExprNodeGenericFuncDesc(
      TypeInfoFactory.booleanTypeInfo,
      new GenericUDFOPEqual(), Lists.newArrayList(
      new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, COLUMN1_NAME, null, false),
      new ExprNodeConstantDesc(TypeInfoFactory.longTypeInfo, COLUMN1_VALUE)
  ));
  private final ExprNodeDesc column2Predicate = new ExprNodeGenericFuncDesc(
      TypeInfoFactory.booleanTypeInfo,
      new GenericUDFOPEqual(), Lists.newArrayList(
      new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, COLUMN2_NAME, null, false),
      new ExprNodeConstantDesc(TypeInfoFactory.longTypeInfo, COLUMN2_VALUE)
  ));

  private final List<KeySchemaElement> tableKeySchema =
      createKeySchema(HASH_KEY_NAME, RANGE_KEY_NAME);
  private final Map<String, String> hiveDynamoDBMapping = initHiveDynamoDBMapping();
  private final Map<String, String> hiveTypeMapping = initHiveTypeMapping();

  @Before
  public void setup() {
    dynamoDBFilterPushdown = new DynamoDBFilterPushdown();
  }

  @Test
  public void testPushPredicate() {
    assertPushablePredicate(serdeConstants.DOUBLE_TYPE_NAME,
        new ExprNodeConstantDesc(TypeInfoFactory.doubleTypeInfo, 1.0));
    assertPushablePredicate(serdeConstants.BIGINT_TYPE_NAME,
        new ExprNodeConstantDesc(TypeInfoFactory.longTypeInfo, 1));
    assertPushablePredicate(serdeConstants.STRING_TYPE_NAME,
        new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "1"));
    assertPushablePredicate(serdeConstants.BINARY_TYPE_NAME,
        new ExprNodeConstantDesc(TypeInfoFactory.binaryTypeInfo, new byte[]{1}));

    assertUnpushablePredicate(serdeConstants.BOOLEAN_TYPE_NAME,
        new ExprNodeConstantDesc(TypeInfoFactory.booleanTypeInfo, true));
    assertUnpushablePredicate(serdeConstants.LIST_TYPE_NAME,
        new ExprNodeConstantDesc(TypeInfoFactory.getListTypeInfo(TypeInfoFactory.longTypeInfo),
            Lists.newArrayList(1)));
  }

  private void assertPushablePredicate(String hiveType, ExprNodeDesc constantDesc) {
    Map<String, String> hiveTypeMapping = new HashMap<>();
    hiveTypeMapping.put("column", hiveType);

    ExprNodeDesc predicate = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
        new GenericUDFOPEqual(), Lists.newArrayList(
        new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "column", null, false),
        constantDesc
    ));

    DecomposedPredicate decomposedPredicate =
        dynamoDBFilterPushdown.pushPredicate(hiveTypeMapping, predicate);
    Assert.assertEquals(decomposedPredicate.pushedPredicate.toString(),
        predicate.toString());
  }

  private void assertUnpushablePredicate(String hiveType, ExprNodeDesc constantDesc) {
    Map<String, String> hiveTypeMapping = new HashMap<>();
    hiveTypeMapping.put("column", hiveType);

    ExprNodeDesc predicate = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
        new GenericUDFOPEqual(), Lists.newArrayList(
        new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "column", null, false),
        constantDesc
    ));

    DecomposedPredicate decomposedPredicate =
        dynamoDBFilterPushdown.pushPredicate(hiveTypeMapping, predicate);
    Assert.assertNull(decomposedPredicate);
  }

  @Test
  public void testPushPredicateDoubleTypeNotInHiveTypeMappingNotPushed() {
    Map<String, String> hiveTypeMapping = new HashMap<>();

    ExprNodeDesc predicate = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
        new GenericUDFOPEqual(), Lists.newArrayList(
        new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "column", null, false),
        new ExprNodeConstantDesc(TypeInfoFactory.doubleTypeInfo, 1.0)
    ));

    DecomposedPredicate decomposedPredicate =
        dynamoDBFilterPushdown.pushPredicate(hiveTypeMapping, predicate);
    Assert.assertNull(decomposedPredicate);
  }

  @Test
  public void testPredicateToDynamoDBFilterWithNoIndexesAndNoHashKey() {
    ExprNodeDesc combinedPredicate = buildPredicate(Lists.newArrayList(
        rangeKeyPredicate,
        column2Predicate));

    DynamoDBQueryFilter dynamoDBQueryFilter = dynamoDBFilterPushdown.predicateToDynamoDBFilter(
        tableKeySchema, null, null,
        hiveDynamoDBMapping, hiveTypeMapping, combinedPredicate);

    Assert.assertNull(dynamoDBQueryFilter.getIndex());
    Assert.assertEquals(0, dynamoDBQueryFilter.getKeyConditions().size());
  }

  @Test
  public void testPredicateToDynamoDBFilterWithNoIndexesAndTableOnlyHasHashKey() {
    ExprNodeDesc combinedPredicate = buildPredicate(Lists.newArrayList(
        hashKeyPredicate,
        column1Predicate));

    DynamoDBQueryFilter dynamoDBQueryFilter = dynamoDBFilterPushdown.predicateToDynamoDBFilter(
        tableKeySchema, null, null,
        hiveDynamoDBMapping, hiveTypeMapping, combinedPredicate);

    Assert.assertNull(dynamoDBQueryFilter.getIndex());
    Assert.assertEquals(1, dynamoDBQueryFilter.getKeyConditions().size());
    assertKeyCondition(HASH_KEY_NAME, HASH_KEY_VALUE, dynamoDBQueryFilter);
  }

  @Test
  public void testPredicateToDynamoDBFilterWithHashKeyAndRangeKey() {
    ExprNodeDesc combinedPredicate = buildPredicate(Lists.newArrayList(
        hashKeyPredicate,
        rangeKeyPredicate,
        column1Predicate,
        column2Predicate));

    GlobalSecondaryIndexDescription gsi = createGSI(GLOBAL_SECONDARY_INDEX_NAME,
        COLUMN1_NAME, COLUMN2_NAME, ProjectionType.ALL.toString(), null);

    DynamoDBQueryFilter dynamoDBQueryFilter = dynamoDBFilterPushdown.predicateToDynamoDBFilter(
        tableKeySchema, null, Lists.newArrayList(gsi),
        hiveDynamoDBMapping, hiveTypeMapping, combinedPredicate);

    Assert.assertNull(dynamoDBQueryFilter.getIndex());
    Assert.assertEquals(2, dynamoDBQueryFilter.getKeyConditions().size());
    assertKeyCondition(HASH_KEY_NAME, HASH_KEY_VALUE, dynamoDBQueryFilter);
    assertKeyCondition(RANGE_KEY_NAME, RANGE_KEY_VALUE, dynamoDBQueryFilter);
  }

  @Test
  public void testPredicateToDynamoDBFilterWithLSIAllProjection() {
    ExprNodeDesc combinedPredicate = buildPredicate(Lists.newArrayList(
        hashKeyPredicate,
        column1Predicate,
        column2Predicate));

    LocalSecondaryIndexDescription lsi = createLSI(LOCAL_SECONDARY_INDEX_NAME,
        HASH_KEY_NAME, COLUMN1_NAME, ProjectionType.ALL.toString(), null);

    DynamoDBQueryFilter dynamoDBQueryFilter = dynamoDBFilterPushdown.predicateToDynamoDBFilter(
        tableKeySchema, Lists.newArrayList(lsi), null,
        hiveDynamoDBMapping, hiveTypeMapping, combinedPredicate);

    Assert.assertEquals(LOCAL_SECONDARY_INDEX_NAME, dynamoDBQueryFilter.getIndex().getIndexName());
    Assert.assertEquals(2, dynamoDBQueryFilter.getKeyConditions().size());
    assertKeyCondition(HASH_KEY_NAME, HASH_KEY_VALUE, dynamoDBQueryFilter);
    assertKeyCondition(COLUMN1_NAME, COLUMN1_VALUE, dynamoDBQueryFilter);
  }

  @Test
  public void testPredicateToDynamoDBFilterWithLSIKeysOnlyProjection() {
    ExprNodeDesc combinedPredicate = buildPredicate(Lists.newArrayList(
        hashKeyPredicate,
        column1Predicate,
        column2Predicate));

    LocalSecondaryIndexDescription lsi = createLSI(LOCAL_SECONDARY_INDEX_NAME,
        HASH_KEY_NAME, COLUMN1_NAME, ProjectionType.KEYS_ONLY.toString(), null);

    DynamoDBQueryFilter dynamoDBQueryFilter = dynamoDBFilterPushdown.predicateToDynamoDBFilter(
        tableKeySchema, Lists.newArrayList(lsi), null,
        hiveDynamoDBMapping, hiveTypeMapping, combinedPredicate);

    Assert.assertNull(dynamoDBQueryFilter.getIndex());
    Assert.assertEquals(1, dynamoDBQueryFilter.getKeyConditions().size());
  }

  @Test
  public void testPredicateToDynamoDBFilterWithLSIIncludeProjectionNotContainAllMapping() {
    ExprNodeDesc combinedPredicate = buildPredicate(Lists.newArrayList(
        hashKeyPredicate,
        column1Predicate,
        column2Predicate));

    LocalSecondaryIndexDescription lsi = createLSI(LOCAL_SECONDARY_INDEX_NAME,
        HASH_KEY_NAME, COLUMN1_NAME, ProjectionType.INCLUDE.toString(),
        Lists.newArrayList(COLUMN2_NAME));

    DynamoDBQueryFilter dynamoDBQueryFilter = dynamoDBFilterPushdown.predicateToDynamoDBFilter(
        tableKeySchema, Lists.newArrayList(lsi), null,
        hiveDynamoDBMapping, hiveTypeMapping, combinedPredicate);

    Assert.assertNull(dynamoDBQueryFilter.getIndex());
    Assert.assertEquals(1, dynamoDBQueryFilter.getKeyConditions().size());
  }

  @Test
  public void testPredicateToDynamoDBFilterWithLSIIncludeProjectionContainAllMapping() {
    ExprNodeDesc combinedPredicate = buildPredicate(Lists.newArrayList(
        hashKeyPredicate,
        column1Predicate,
        column2Predicate));

    LocalSecondaryIndexDescription lsi = createLSI(LOCAL_SECONDARY_INDEX_NAME,
        HASH_KEY_NAME, COLUMN1_NAME, ProjectionType.INCLUDE.toString(),
        Lists.newArrayList(COLUMN2_NAME, COLUMN3_NAME, COLUMN4_NAME));

    DynamoDBQueryFilter dynamoDBQueryFilter = dynamoDBFilterPushdown.predicateToDynamoDBFilter(
        tableKeySchema, Lists.newArrayList(lsi), null,
        hiveDynamoDBMapping, hiveTypeMapping, combinedPredicate);

    Assert.assertEquals(LOCAL_SECONDARY_INDEX_NAME, dynamoDBQueryFilter.getIndex().getIndexName());
    Assert.assertEquals(2, dynamoDBQueryFilter.getKeyConditions().size());
    assertKeyCondition(HASH_KEY_NAME, HASH_KEY_VALUE, dynamoDBQueryFilter);
    assertKeyCondition(COLUMN1_NAME, COLUMN1_VALUE, dynamoDBQueryFilter);
  }

  @Test
  public void testPredicateToDynamoDBFilterWithHashKeyAndMismatchLSI() {
    ExprNodeDesc combinedPredicate = buildPredicate(Lists.newArrayList(
        hashKeyPredicate,
        column2Predicate));

    LocalSecondaryIndexDescription lsi = createLSI(LOCAL_SECONDARY_INDEX_NAME,
        HASH_KEY_NAME, COLUMN1_NAME, ProjectionType.ALL.toString(), null);

    DynamoDBQueryFilter dynamoDBQueryFilter = dynamoDBFilterPushdown.predicateToDynamoDBFilter(
        tableKeySchema, Lists.newArrayList(lsi), null,
        hiveDynamoDBMapping, hiveTypeMapping, combinedPredicate);

    Assert.assertNull(dynamoDBQueryFilter.getIndex());
    Assert.assertEquals(1, dynamoDBQueryFilter.getKeyConditions().size());
    assertKeyCondition(HASH_KEY_NAME, HASH_KEY_VALUE, dynamoDBQueryFilter);
  }

  @Test
  public void testPredicateToDynamoDBFilterWithGSIAllProjection() {
    ExprNodeDesc combinedPredicate = buildPredicate(Lists.newArrayList(
        rangeKeyPredicate,
        column1Predicate,
        column2Predicate));

    GlobalSecondaryIndexDescription gsi = createGSI(GLOBAL_SECONDARY_INDEX_NAME,
        COLUMN1_NAME, COLUMN2_NAME, ProjectionType.ALL.toString(), null);

    DynamoDBQueryFilter dynamoDBQueryFilter = dynamoDBFilterPushdown.predicateToDynamoDBFilter(
        tableKeySchema, null, Lists.newArrayList(gsi),
        hiveDynamoDBMapping, hiveTypeMapping, combinedPredicate);

    Assert.assertEquals(GLOBAL_SECONDARY_INDEX_NAME, dynamoDBQueryFilter.getIndex().getIndexName());
    Assert.assertEquals(2, dynamoDBQueryFilter.getKeyConditions().size());
    assertKeyCondition(COLUMN1_NAME, COLUMN1_VALUE, dynamoDBQueryFilter);
    assertKeyCondition(COLUMN2_NAME, COLUMN2_VALUE, dynamoDBQueryFilter);
  }

  @Test
  public void testPredicateToDynamoDBFilterWithGSIKeysOnlyProjection() {
    ExprNodeDesc combinedPredicate = buildPredicate(Lists.newArrayList(
        rangeKeyPredicate,
        column1Predicate,
        column2Predicate));

    GlobalSecondaryIndexDescription gsi = createGSI(GLOBAL_SECONDARY_INDEX_NAME,
        COLUMN1_NAME, COLUMN2_NAME, ProjectionType.KEYS_ONLY.toString(), null);

    DynamoDBQueryFilter dynamoDBQueryFilter = dynamoDBFilterPushdown.predicateToDynamoDBFilter(
        tableKeySchema, null, Lists.newArrayList(gsi),
        hiveDynamoDBMapping, hiveTypeMapping, combinedPredicate);

    Assert.assertNull(dynamoDBQueryFilter.getIndex());
    Assert.assertEquals(0, dynamoDBQueryFilter.getKeyConditions().size());
  }

  @Test
  public void testPredicateToDynamoDBFilterWithGSIIncludeProjectionNotContainAllMapping() {
    ExprNodeDesc combinedPredicate = buildPredicate(Lists.newArrayList(
        rangeKeyPredicate,
        column1Predicate,
        column2Predicate));

    GlobalSecondaryIndexDescription gsi = createGSI(GLOBAL_SECONDARY_INDEX_NAME,
        COLUMN1_NAME, COLUMN2_NAME, ProjectionType.INCLUDE.toString(),
        Lists.newArrayList(COLUMN3_NAME));

    DynamoDBQueryFilter dynamoDBQueryFilter = dynamoDBFilterPushdown.predicateToDynamoDBFilter(
        tableKeySchema, null, Lists.newArrayList(gsi),
        hiveDynamoDBMapping, hiveTypeMapping, combinedPredicate);

    Assert.assertNull(dynamoDBQueryFilter.getIndex());
    Assert.assertEquals(0, dynamoDBQueryFilter.getKeyConditions().size());
  }

  @Test
  public void testPredicateToDynamoDBFilterWithGSIIncludeProjectionContainAllMapping() {
    ExprNodeDesc combinedPredicate = buildPredicate(Lists.newArrayList(
        rangeKeyPredicate,
        column1Predicate,
        column2Predicate));

    GlobalSecondaryIndexDescription gsi = createGSI(GLOBAL_SECONDARY_INDEX_NAME,
        COLUMN1_NAME, COLUMN2_NAME, ProjectionType.INCLUDE.toString(),
        Lists.newArrayList(COLUMN3_NAME, COLUMN4_NAME));

    DynamoDBQueryFilter dynamoDBQueryFilter = dynamoDBFilterPushdown.predicateToDynamoDBFilter(
        tableKeySchema, null, Lists.newArrayList(gsi),
        hiveDynamoDBMapping, hiveTypeMapping, combinedPredicate);

    Assert.assertEquals(GLOBAL_SECONDARY_INDEX_NAME, dynamoDBQueryFilter.getIndex().getIndexName());
    Assert.assertEquals(2, dynamoDBQueryFilter.getKeyConditions().size());
    assertKeyCondition(COLUMN1_NAME, COLUMN1_VALUE, dynamoDBQueryFilter);
    assertKeyCondition(COLUMN2_NAME, COLUMN2_VALUE, dynamoDBQueryFilter);
  }

  @Test
  public void testPredicateToDynamoDBFilterWithMultipleGSIsHavingTheSameHashKey() {
    ExprNodeDesc combinedPredicate = buildPredicate(Lists.newArrayList(
        rangeKeyPredicate,
        column1Predicate));

    GlobalSecondaryIndexDescription gsi1 = createGSI(GLOBAL_SECONDARY_INDEX_NAME + "1",
        COLUMN1_NAME, COLUMN2_NAME, ProjectionType.ALL.toString(), null);
    GlobalSecondaryIndexDescription gsi2 = createGSI(GLOBAL_SECONDARY_INDEX_NAME + "2",
        COLUMN1_NAME, RANGE_KEY_NAME, ProjectionType.ALL.toString(), null);

    DynamoDBQueryFilter dynamoDBQueryFilter = dynamoDBFilterPushdown.predicateToDynamoDBFilter(
        tableKeySchema, null, Lists.newArrayList(gsi1, gsi2),
        hiveDynamoDBMapping, hiveTypeMapping, combinedPredicate);

    Assert.assertEquals(GLOBAL_SECONDARY_INDEX_NAME + "2",
        dynamoDBQueryFilter.getIndex().getIndexName());
    Assert.assertEquals(2, dynamoDBQueryFilter.getKeyConditions().size());
    assertKeyCondition(COLUMN1_NAME, COLUMN1_VALUE, dynamoDBQueryFilter);
    assertKeyCondition(RANGE_KEY_NAME, RANGE_KEY_VALUE, dynamoDBQueryFilter);
  }

  @Test
  public void testPredicateToDynamoDBFilterWithGSIHashKeyOnly() {
    ExprNodeDesc combinedPredicate = buildPredicate(Lists.newArrayList(
        rangeKeyPredicate,
        column1Predicate));

    GlobalSecondaryIndexDescription gsi = createGSI(GLOBAL_SECONDARY_INDEX_NAME,
        COLUMN1_NAME, COLUMN2_NAME, ProjectionType.ALL.toString(), null);

    DynamoDBQueryFilter dynamoDBQueryFilter = dynamoDBFilterPushdown.predicateToDynamoDBFilter(
        tableKeySchema, null, Lists.newArrayList(gsi),
        hiveDynamoDBMapping, hiveTypeMapping, combinedPredicate);

    Assert.assertNull(dynamoDBQueryFilter.getIndex());
    Assert.assertEquals(0, dynamoDBQueryFilter.getKeyConditions().size());
  }

  @Test
  public void testPredicateToDynamoDBFilterWithoutAnyMatchedGSI() {
    ExprNodeDesc combinedPredicate = buildPredicate(Lists.newArrayList(
        rangeKeyPredicate,
        column2Predicate));

    GlobalSecondaryIndexDescription gsi = createGSI(GLOBAL_SECONDARY_INDEX_NAME,
        COLUMN1_NAME, COLUMN2_NAME, ProjectionType.ALL.toString(), null);

    DynamoDBQueryFilter dynamoDBQueryFilter = dynamoDBFilterPushdown.predicateToDynamoDBFilter(
        tableKeySchema, null, Lists.newArrayList(gsi),
        hiveDynamoDBMapping, hiveTypeMapping, combinedPredicate);

    Assert.assertNull(dynamoDBQueryFilter.getIndex());
    Assert.assertEquals(0, dynamoDBQueryFilter.getKeyConditions().size());
  }

  private ExprNodeDesc buildPredicate(List<ExprNodeDesc> predicates) {
    ExprNodeDesc combinedPredicate = null;
    for (ExprNodeDesc predicate : predicates) {
      if (combinedPredicate == null) {
        combinedPredicate = predicate;
        continue;
      }
      List<ExprNodeDesc> children = new ArrayList<>();
      children.add(combinedPredicate);
      children.add(predicate);
      combinedPredicate = new ExprNodeGenericFuncDesc(
          TypeInfoFactory.booleanTypeInfo,
          FunctionRegistry.getGenericUDFForAnd(),
          children);
    }
    return combinedPredicate;
  }

  private List<KeySchemaElement> createKeySchema(String hashKeyName, String rangeKeyName) {
    List<KeySchemaElement> schema = new ArrayList<>();
    schema.add(new KeySchemaElement(hashKeyName, HASH_KEY_TYPE));
    if (rangeKeyName != null) {
      schema.add(new KeySchemaElement(rangeKeyName, RANGE_KEY_TYPE));
    }
    return schema;
  }

  private Map<String, String> initHiveDynamoDBMapping() {
    Map<String, String> hiveDynamoDBMapping = new HashMap<>();
    hiveDynamoDBMapping.put(HASH_KEY_NAME, HASH_KEY_NAME);
    hiveDynamoDBMapping.put(RANGE_KEY_NAME, RANGE_KEY_NAME);
    hiveDynamoDBMapping.put(COLUMN1_NAME, COLUMN1_NAME);
    hiveDynamoDBMapping.put(COLUMN2_NAME, COLUMN2_NAME);
    hiveDynamoDBMapping.put(COLUMN3_NAME, COLUMN3_NAME);
    hiveDynamoDBMapping.put(COLUMN4_NAME, COLUMN4_NAME);
    return hiveDynamoDBMapping;
  }

  private Map<String, String> initHiveTypeMapping() {
    Map<String, String> hiveTypeMapping = new HashMap<>();
    hiveTypeMapping.put(HASH_KEY_NAME, serdeConstants.STRING_TYPE_NAME);
    hiveTypeMapping.put(RANGE_KEY_NAME, serdeConstants.STRING_TYPE_NAME);
    hiveTypeMapping.put(COLUMN1_NAME, serdeConstants.STRING_TYPE_NAME);
    hiveTypeMapping.put(COLUMN2_NAME, serdeConstants.STRING_TYPE_NAME);
    hiveTypeMapping.put(COLUMN3_NAME, serdeConstants.STRING_TYPE_NAME);
    hiveTypeMapping.put(COLUMN4_NAME, serdeConstants.STRING_TYPE_NAME);
    return  hiveTypeMapping;
  }

  private GlobalSecondaryIndexDescription createGSI(String indexName, String hashKeyName,
      String rangeKeyName, String projectionType, List<String> nonKeyAttributes) {
    List<KeySchemaElement> schema = createKeySchema(hashKeyName, rangeKeyName);
    Projection projection = new Projection()
        .withProjectionType(projectionType)
        .withNonKeyAttributes(nonKeyAttributes);
    return new GlobalSecondaryIndexDescription()
        .withIndexName(indexName)
        .withKeySchema(schema)
        .withProjection(projection);
  }

  private LocalSecondaryIndexDescription createLSI(String indexName, String hashKeyName,
      String rangeKeyName, String projectionType, List<String> nonKeyAttributes) {
    List<KeySchemaElement> schema = createKeySchema(hashKeyName, rangeKeyName);
    Projection projection = new Projection()
        .withProjectionType(projectionType)
        .withNonKeyAttributes(nonKeyAttributes);
    return new LocalSecondaryIndexDescription()
        .withIndexName(indexName)
        .withKeySchema(schema)
        .withProjection(projection);
  }

  private void assertKeyCondition(String columnName, String columnValue,
      DynamoDBQueryFilter filter) {
    Condition hashKeyCondition = new Condition();
    List<AttributeValue> hashKeyAttributeValueList = new ArrayList<>();
    hashKeyAttributeValueList.add(new AttributeValue(columnValue));
    hashKeyCondition.setAttributeValueList(hashKeyAttributeValueList);
    hashKeyCondition.setComparisonOperator(DynamoDBFilterOperator.EQ.getDynamoDBName());
    Assert.assertEquals((hashKeyCondition.toString()),
        filter.getKeyConditions().get(columnName).toString());
  }
}
