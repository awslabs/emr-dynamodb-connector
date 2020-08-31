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

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.Condition;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.dynamodb.filter.DynamoDBFilterOperator;
import org.apache.hadoop.hive.dynamodb.type.HiveDynamoDBTypeFactory;

public class DynamoDBNAryFilter extends AbstractDynamoDBFilter {

  private final String[] columnValues;

  public DynamoDBNAryFilter(String columnName, DynamoDBFilterOperator operator, String
      columnType, String... values) {
    super(columnName, operator, columnType);
    this.columnValues = values;
  }

  @Override
  public Condition createCondition() {
    Condition condition = new Condition();
    condition.setComparisonOperator(operator.getDynamoDBName());

    List<AttributeValue> attributeValueList = new ArrayList<>();
    for (String value : columnValues) {
      attributeValueList.add(HiveDynamoDBTypeFactory.getTypeObjectFromHiveType(columnType)
          .getAttributeValue(value));
    }
    condition.setAttributeValueList(attributeValueList);

    return condition;
  }
}
