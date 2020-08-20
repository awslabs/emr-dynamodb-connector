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

package org.apache.hadoop.hive.dynamodb.filter;

import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.dynamodb.filter.DynamoDBFilter;
import org.apache.hadoop.dynamodb.filter.DynamoDBFilterOperator;
import org.apache.hadoop.dynamodb.filter.DynamoDBIndexInfo;
import org.apache.hadoop.dynamodb.filter.DynamoDBQueryFilter;
import org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler.DecomposedPredicate;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde.serdeConstants;

public class DynamoDBFilterPushdown {

  private static final Log log = LogFactory.getLog(DynamoDBFilterPushdown.class);

  private final Set<String> eligibleHiveTypes = new HashSet<>();
  private final Set<DynamoDBFilterOperator> eligibleOperatorsForRange = new HashSet<>();
  private final IndexPredicateAnalyzer analyzer = new IndexPredicateAnalyzer();

  private static final int HASH_KEY_INDEX = 0;
  private static final int RANGE_KEY_INDEX = 1;
  private static final String DYNAMODB_KEY_TYPE_HASH = "HASH";

  public DynamoDBFilterPushdown() {
    eligibleHiveTypes.add(serdeConstants.DOUBLE_TYPE_NAME);
    eligibleHiveTypes.add(serdeConstants.BIGINT_TYPE_NAME);
    eligibleHiveTypes.add(serdeConstants.STRING_TYPE_NAME);
    eligibleHiveTypes.add(serdeConstants.BINARY_TYPE_NAME);

    // Not all scan operators are supported by DynamoDB Query API
    eligibleOperatorsForRange.add(DynamoDBFilterOperator.EQ);
    eligibleOperatorsForRange.add(DynamoDBFilterOperator.LE);
    eligibleOperatorsForRange.add(DynamoDBFilterOperator.LT);
    eligibleOperatorsForRange.add(DynamoDBFilterOperator.GE);
    eligibleOperatorsForRange.add(DynamoDBFilterOperator.GT);
    eligibleOperatorsForRange.add(DynamoDBFilterOperator.BETWEEN);
  }

  public DecomposedPredicate pushPredicate(Map<String, String> hiveTypeMapping, ExprNodeDesc
      predicate) {
    log.info("Checking predicates for pushdown in DynamoDB query");
    List<IndexSearchCondition> searchConditions = getGenericSearchConditions(hiveTypeMapping,
        predicate);
    log.info("Pushed predicates: " + searchConditions);
    if (searchConditions.isEmpty()) {
      return null;
    } else {
      List<IndexSearchCondition> finalSearchCondition =
          prioritizeSearchConditions(searchConditions);
      DecomposedPredicate decomposedPredicate = new DecomposedPredicate();
      decomposedPredicate.pushedPredicate =
          analyzer.translateSearchConditions(finalSearchCondition);
      decomposedPredicate.residualPredicate = (ExprNodeGenericFuncDesc) predicate;
      return decomposedPredicate;
    }
  }

  public DynamoDBQueryFilter predicateToDynamoDBFilter(List<KeySchemaElement> schema,
      List<LocalSecondaryIndexDescription> localSecondaryIndexes,
      List<GlobalSecondaryIndexDescription> globalSecondaryIndexes,
      Map<String, String> hiveDynamoDBMapping,
      Map<String, String> hiveTypeMapping,
      ExprNodeDesc predicate) {
    List<IndexSearchCondition> searchConditions = getGenericSearchConditions(hiveTypeMapping,
        predicate);

    if (searchConditions.isEmpty()) {
      return null;
    }

    Map<String, DynamoDBFilter> filterMap = new HashMap<>();
    DynamoDBFilterFactory factory = new DynamoDBFilterFactory();

    // The search conditions are supposed to be unique at this point, so not
    // prioritizing them
    for (IndexSearchCondition condition : searchConditions) {
      String hiveColumnName = condition.getColumnDesc().getColumn();
      String dynamoDBColumnName = hiveDynamoDBMapping.get(hiveColumnName);
      DynamoDBFilterOperator op =
          DynamoDBFilterOperator.getFilterOperationFromHiveClass(condition.getComparisonOp());
      DynamoDBFilter filter =
          factory.getFilter(op, dynamoDBColumnName, hiveTypeMapping.get(hiveColumnName), condition);

      if (filterMap.containsKey(dynamoDBColumnName)) {
        // We have special case code for DynamoDB filter BETWEEN because
        // it does not directly map to any Hive predicate
        DynamoDBFilter existingFilter = filterMap.get(dynamoDBColumnName);
        if (isBetweenFilter(op, existingFilter.getOperator())) {
          filterMap.put(dynamoDBColumnName, getBetweenFilter(filter, existingFilter));
        } else {
          throw new RuntimeException("Found two filters for same column: " + dynamoDBColumnName
              + " Filter 1: " + op + " Filter 2: " + existingFilter.getOperator());
        }
      } else {
        filterMap.put(dynamoDBColumnName, filter);
      }
    }

    return getDynamoDBQueryFilter(schema,
        localSecondaryIndexes,
        globalSecondaryIndexes,
        hiveDynamoDBMapping,
        filterMap);
  }

  private boolean isBetweenFilter(DynamoDBFilterOperator op1, DynamoDBFilterOperator op2) {
    if (op1.equals(DynamoDBFilterOperator.GE) && op2.equals(DynamoDBFilterOperator.LE)) {
      return true;
    } else {
      return op2.equals(DynamoDBFilterOperator.GE) && op1.equals(DynamoDBFilterOperator.LE);
    }
  }

  private DynamoDBFilter getBetweenFilter(DynamoDBFilter filter1, DynamoDBFilter filter2) {
    String val1;
    String val2;
    if (filter1.getOperator().equals(DynamoDBFilterOperator.GE)) {
      val1 = ((DynamoDBBinaryFilter) filter1).getColumnValue();
      val2 = ((DynamoDBBinaryFilter) filter2).getColumnValue();
    } else {
      val1 = ((DynamoDBBinaryFilter) filter2).getColumnValue();
      val2 = ((DynamoDBBinaryFilter) filter1).getColumnValue();
    }
    return new DynamoDBFilterFactory().getFilter(DynamoDBFilterOperator.BETWEEN,
        filter1.getColumnName(), filter1.getColumnType(), val1, val2);
  }

  private List<IndexSearchCondition> getGenericSearchConditions(Map<String, String> hiveTypeMapping,
      ExprNodeDesc predicate) {

    analyzer.clearAllowedColumnNames();

    // DynamoDB does not support filters on columns of types set
    for (Entry<String, String> entry : hiveTypeMapping.entrySet()) {
      if (eligibleHiveTypes.contains(entry.getValue())) {
        analyzer.allowColumnName(entry.getKey());
      }
    }

    for (DynamoDBFilterOperator op : DynamoDBFilterOperator.values()) {
      if (op.getHiveClass() != null) {
        analyzer.addComparisonOp(op.getHiveClass());
      }
    }

    List<IndexSearchCondition> searchConditions = new ArrayList<>();
    analyzer.analyzePredicate(predicate, searchConditions);
    return searchConditions;
  }

  private List<IndexSearchCondition> prioritizeSearchConditions(List<IndexSearchCondition>
      searchConditions) {
    Map<String, IndexSearchCondition> priorityFilterMap = new HashMap<>();
    Map<String, List<IndexSearchCondition>> completeFilterMap = new HashMap<>();

    for (IndexSearchCondition condition : searchConditions) {
      String name = condition.getColumnDesc().getColumn();
      DynamoDBFilterOperator op =
          DynamoDBFilterOperator.getFilterOperationFromHiveClass(condition.getComparisonOp());
      if (priorityFilterMap.containsKey(name)) {
        if (DynamoDBFilterOperator.getFilterOperationFromHiveClass(priorityFilterMap.get(name)
            .getComparisonOp()).isLowerPrecedence(op)) {
          priorityFilterMap.put(name, condition);
        }
      } else {
        completeFilterMap.put(name, new ArrayList<IndexSearchCondition>());
        priorityFilterMap.put(name, condition);
      }
      completeFilterMap.get(name).add(condition);
    }

    List<IndexSearchCondition> finalSearchConditions = new ArrayList<>();
    finalSearchConditions.addAll(priorityFilterMap.values());

    // Special case code for BETWEEN filter as it does not directly map to a
    // Hive filter

    for (Map.Entry<String, List<IndexSearchCondition>> entry : completeFilterMap.entrySet()) {
      for (IndexSearchCondition condition : entry.getValue()) {
        DynamoDBFilterOperator op1 =
            DynamoDBFilterOperator.getFilterOperationFromHiveClass(condition.getComparisonOp());
        DynamoDBFilterOperator op2 =
            DynamoDBFilterOperator.getFilterOperationFromHiveClass(
                priorityFilterMap.get(entry.getKey()).getComparisonOp()
            );
        if (isBetweenFilter(op1, op2)) {
          finalSearchConditions.add(condition);
        }
      }
    }

    return finalSearchConditions;
  }

  /*
   * This method sets the query filter / scan filter parameters appropriately.
   * It first check table key schema if any query condition match with table hash and range key.
   * If not all of table keys are found in query conditions, check following cases:
   *
   *  1. If no table key is found matching with the query conditions, iterate through GSIs
   * (Global Secondary Indexes) to see if we can find both hash key and range key of a GSI in the
   * query conditions. NOTE: If we only find GSI hash key but not range key (for GSI has 2 keys), we
   * still cannot use this GSI because items might be missed in the query result.
   *     e.g. item {'hk': 'hash key value', 'rk': 'range key value', 'column1': 'value'}
   *          will not exist in a GSI with 'column1' as hash key and 'column2' as range key.
   *          So query with "where column1 = 'value'" will not return the item if we use this GSI
   *          since the item is not written to the GSI due to missing of index range key 'column2'
   *
   *  2. If only table hash key is found, iterate through LSIs (Local Secondary Indexes) to see if
   * we can find a LSI having index range key match with any query condition.
   */
  private DynamoDBQueryFilter getDynamoDBQueryFilter(List<KeySchemaElement> schema,
      List<LocalSecondaryIndexDescription> localSecondaryIndexes,
      List<GlobalSecondaryIndexDescription> globalSecondaryIndexes,
      Map<String, String> hiveDynamoDBMapping,
      Map<String, DynamoDBFilter> filterMap) {

    DynamoDBQueryFilter filter = new DynamoDBQueryFilter();

    List<DynamoDBFilter> keyFiltersUseForQuery = getDynamoDBFiltersFromSchema(schema, filterMap);
    DynamoDBIndexInfo indexUseForQuery = null;

    if (keyFiltersUseForQuery.size() < schema.size()) {
      if (keyFiltersUseForQuery.size() == 0 && globalSecondaryIndexes != null) {
        // Hash key not found. Check GSIs.
        indexUseForQuery = getIndexUseForQuery(
            schema,
            globalSecondaryIndexes.stream()
                .map(index -> new DynamoDBIndexInfo(index.getIndexName(),
                    index.getKeySchema(), index.getProjection()))
                .collect(Collectors.toList()),
            hiveDynamoDBMapping,
            filterMap,
            keyFiltersUseForQuery);

        // Don't use GSI when it is not a fully match.
        // Refer to method comment for detailed explanation
        if (indexUseForQuery != null
            && indexUseForQuery.getIndexSchema().size() > keyFiltersUseForQuery.size()) {
          indexUseForQuery = null;
          keyFiltersUseForQuery.clear();
        }
      } else if (keyFiltersUseForQuery.size() == 1 && localSecondaryIndexes != null) {
        // Hash key found but Range key not found. Check LSIs.
        indexUseForQuery = getIndexUseForQuery(
            schema,
            localSecondaryIndexes.stream()
                .map(index -> new DynamoDBIndexInfo(index.getIndexName(),
                    index.getKeySchema(), index.getProjection()))
                .collect(Collectors.toList()),
            hiveDynamoDBMapping,
            filterMap,
            keyFiltersUseForQuery);
      }
    }

    if (indexUseForQuery != null) {
      log.info("Setting index name used for query: " + indexUseForQuery.getIndexName());
      filter.setIndex(indexUseForQuery);
    }
    for (DynamoDBFilter f : keyFiltersUseForQuery) {
      filter.addKeyCondition(f);
    }
    for (DynamoDBFilter f : filterMap.values()) {
      if (!filter.getKeyConditions().containsKey(f.getColumnName())) {
        filter.addScanFilter(f);
      }
    }
    return filter;
  }

  /*
   * This method has side effect to update keyFiltersUseForQuery to be the optimal combination
   * seen so far.
   * The return value is the most efficient index for querying the matched items.
   */
  private DynamoDBIndexInfo getIndexUseForQuery(List<KeySchemaElement> tableSchema,
      List<DynamoDBIndexInfo> indexes,
      Map<String, String> hiveDynamoDBMapping,
      Map<String, DynamoDBFilter> filterMap,
      List<DynamoDBFilter> keyFiltersUseForQuery) {
    DynamoDBIndexInfo indexUseForQuery = null;
    for (DynamoDBIndexInfo index : indexes) {
      List<DynamoDBFilter> indexFilterList =
          getDynamoDBFiltersFromSchema(index.getIndexSchema(), filterMap);
      if (indexFilterList.size() > keyFiltersUseForQuery.size()
          && indexIncludesAllMappedAttributes(tableSchema, index, hiveDynamoDBMapping)) {
        keyFiltersUseForQuery.clear();
        keyFiltersUseForQuery.addAll(indexFilterList);
        indexUseForQuery = index;
        if (keyFiltersUseForQuery.size() == 2) {
          break;
        }
      }
    }
    return indexUseForQuery;
  }

  private boolean indexIncludesAllMappedAttributes(List<KeySchemaElement> tableSchema,
      DynamoDBIndexInfo index, Map<String, String> hiveDynamoDBMapping) {
    Projection indexProjection = index.getIndexProjection();
    if (ProjectionType.ALL.toString().equals(indexProjection.getProjectionType())) {
      return true;
    }

    Set<String> projectionAttributes = new HashSet<>();
    for (KeySchemaElement keySchemaElement: tableSchema) {
      projectionAttributes.add(keySchemaElement.getAttributeName());
    }
    for (KeySchemaElement keySchemaElement: index.getIndexSchema()) {
      projectionAttributes.add(keySchemaElement.getAttributeName());
    }
    if (ProjectionType.INCLUDE.toString().equals(indexProjection.getProjectionType())) {
      projectionAttributes.addAll(indexProjection.getNonKeyAttributes());
    }

    log.info("Checking if all mapped attributes " + hiveDynamoDBMapping.values()
        + " are included in the index " + index.getIndexName()
        + " having attributes " + projectionAttributes);
    for (String queriedAttribute : hiveDynamoDBMapping.values()) {
      if (!projectionAttributes.contains(queriedAttribute)) {
        log.info("Not all mapped attributes are included in the index. Won't use index: "
            + index.getIndexName());
        return false;
      }
    }
    return true;
  }

  private List<DynamoDBFilter> getDynamoDBFiltersFromSchema(List<KeySchemaElement> schema,
      Map<String, DynamoDBFilter> filterMap) {
    List<DynamoDBFilter> dynamoDBFilters = new ArrayList<>();

    boolean hashKeyFilterExists = false;
    if (schema.size() > 0
        && DYNAMODB_KEY_TYPE_HASH.equals(schema.get(HASH_KEY_INDEX).getKeyType())) {
      String hashKeyName = schema.get(HASH_KEY_INDEX).getAttributeName();
      if (filterMap.containsKey(hashKeyName)) {
        DynamoDBFilter hashKeyFilter = filterMap.get(hashKeyName);
        if (DynamoDBFilterOperator.EQ.equals(hashKeyFilter.getOperator())) {
          dynamoDBFilters.add(hashKeyFilter);
          hashKeyFilterExists = true;
        }
      }
    }

    if (hashKeyFilterExists && schema.size() > 1) {
      String rangeKeyName = schema.get(RANGE_KEY_INDEX).getAttributeName();
      if (filterMap.containsKey(rangeKeyName)) {
        DynamoDBFilter rangeKeyFilter = filterMap.get(rangeKeyName);
        if (eligibleOperatorsForRange.contains(rangeKeyFilter.getOperator())) {
          dynamoDBFilters.add(rangeKeyFilter);
        }
      }
    }
    return dynamoDBFilters;
  }
}
