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

import com.amazonaws.services.dynamodbv2.model.Condition;

import org.apache.hadoop.dynamodb.filter.DynamoDBFilter;
import org.apache.hadoop.dynamodb.filter.DynamoDBFilterOperator;

public abstract class AbstractDynamoDBFilter implements DynamoDBFilter {

  protected String columnName;
  protected DynamoDBFilterOperator operator;
  protected String columnType;
  protected Condition condition;

  public AbstractDynamoDBFilter(String columnName, DynamoDBFilterOperator operator, String
      columnType) {
    super();
    this.columnName = columnName;
    this.operator = operator;
    this.columnType = columnType;
  }

  @Override
  public String getColumnName() {
    return columnName;
  }

  @Override
  public String getColumnType() {
    return columnType;
  }

  @Override
  public DynamoDBFilterOperator getOperator() {
    return operator;
  }

  @Override
  public Condition getDynamoDBCondition() {
    if (condition == null) {
      condition = createCondition();
    }
    return condition;
  }

  protected abstract Condition createCondition();
}
