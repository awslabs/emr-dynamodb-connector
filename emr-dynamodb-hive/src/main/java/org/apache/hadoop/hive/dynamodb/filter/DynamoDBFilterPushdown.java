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

import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.dynamodb.filter.DynamoDBFilter;
import org.apache.hadoop.dynamodb.filter.DynamoDBFilterOperator;
import org.apache.hadoop.dynamodb.filter.DynamoDBQueryFilter;
import org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler.DecomposedPredicate;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde.serdeConstants;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class DynamoDBFilterPushdown {

  private static final Log log = LogFactory.getLog(DynamoDBFilterPushdown.class);

  private final Set<String> eligibleHiveTypes = new HashSet<>();
  private final Set<DynamoDBFilterOperator> eligibleOperatorsForRange = new HashSet<>();

  public DynamoDBFilterPushdown() {
    eligibleHiveTypes.add(serdeConstants.DOUBLE_TYPE_NAME);
    eligibleHiveTypes.add(serdeConstants.BIGINT_TYPE_NAME);
    eligibleHiveTypes.add(serdeConstants.STRING_TYPE_NAME);

    // Not all scan operators are supported by DynanoDB Query API
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
      IndexPredicateAnalyzer analyzer = new IndexPredicateAnalyzer();
      DecomposedPredicate decomposedPredicate = new DecomposedPredicate();
      decomposedPredicate.pushedPredicate =
          analyzer.translateSearchConditions(finalSearchCondition);
      decomposedPredicate.residualPredicate = (ExprNodeGenericFuncDesc) predicate;
      return decomposedPredicate;
    }
  }

  public DynamoDBQueryFilter predicateToDynamoDBFilter(List<KeySchemaElement> schema, Map<String,
      String> hiveDynamoDBMapping, Map<String, String> hiveTypeMapping, ExprNodeDesc predicate) {
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

    return getDynamoDBQueryFilter(schema, filterMap);
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

    IndexPredicateAnalyzer analyzer = new IndexPredicateAnalyzer();

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
   * This method sets the query filter / scan filter parameters appropriately
   */
  private DynamoDBQueryFilter getDynamoDBQueryFilter(List<KeySchemaElement> schema, Map<String,
      DynamoDBFilter> filterMap) {
    DynamoDBQueryFilter filter = new DynamoDBQueryFilter();

    boolean hashKeyFilterExists = false;
    if (schema.size() > 0 && "HASH".equals(schema.get(0).getKeyType())) {
      String hashKeyName = schema.get(0).getAttributeName();
      if (filterMap.containsKey(hashKeyName)) {
        DynamoDBFilter hashKeyFilter = filterMap.get(hashKeyName);
        if (DynamoDBFilterOperator.EQ.equals(hashKeyFilter.getOperator())) {
          filter.addKeyCondition(hashKeyFilter);
          hashKeyFilterExists = true;
        }
      }
    }

    if (hashKeyFilterExists && schema.size() > 1) {
      String rangeKeyName = schema.get(1).getAttributeName();
      if (filterMap.containsKey(rangeKeyName)) {
        DynamoDBFilter rangeKeyFilter = filterMap.get(rangeKeyName);
        if (eligibleOperatorsForRange.contains(rangeKeyFilter.getOperator())) {
          filter.addKeyCondition(rangeKeyFilter);
        }
      }
    }

    for (DynamoDBFilter f : filterMap.values()) {
      if (!filter.getKeyConditions().containsKey(f.getColumnName())) {
        filter.addScanFilter(f);
      }
    }

    return filter;
  }
}
