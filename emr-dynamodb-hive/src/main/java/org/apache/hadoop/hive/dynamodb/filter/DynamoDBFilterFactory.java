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

import org.apache.hadoop.dynamodb.filter.DynamoDBFilter;
import org.apache.hadoop.dynamodb.filter.DynamoDBFilterOperator;
import org.apache.hadoop.hive.dynamodb.shims.ShimsLoader;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;

import java.util.List;

public class DynamoDBFilterFactory {

  public DynamoDBFilter getFilter(DynamoDBFilterOperator operator, String columnName, String
      columnType, String... values) {
    switch (operator.getType()) {
      case UNARY:
        return new DynamoDBUnaryFilter(columnName, operator, columnType);
      case BINARY:
        return new DynamoDBBinaryFilter(columnName, operator, columnType, values[0]);
      case NARY:
        return new DynamoDBNAryFilter(columnName, operator, columnType, values);
      default:
        throw new RuntimeException("Unknown operator type. Operator: " + operator + " "
            + "OperatorType: " + operator.getType());
    }
  }

  public DynamoDBFilter getFilter(DynamoDBFilterOperator operator, String columnName, String
      columnType, IndexSearchCondition condition) {
    switch (operator.getType()) {
      case UNARY:
        return getFilter(operator, columnName, columnType);
      case BINARY:
        return getFilter(operator, columnName, columnType, condition.getConstantDesc().getValue()
            .toString());
      case NARY:
        List<ExprNodeDesc> children = ShimsLoader.getHiveShims().getIndexExpression(condition)
            .getChildren();
        String[] values = new String[children.size() - 1];
        // This currently supports IN clause only
        // The first element is column name and rest of the elements are
        // the values it can take
        for (int i = 1; i < children.size(); i++) {
          values[i - 1] = ((ExprNodeConstantDesc) children.get(i)).getValue().toString();
        }
        return getFilter(operator, columnName, columnType, values);
      default:
        throw new RuntimeException("Unknown operator type. Operator: " + operator + " "
            + "OperatorType: " + operator.getType());
    }
  }
}
