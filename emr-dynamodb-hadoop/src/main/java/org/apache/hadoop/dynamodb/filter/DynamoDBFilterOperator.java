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

package org.apache.hadoop.dynamodb.filter;

import java.util.HashMap;
import java.util.Map;

public enum DynamoDBFilterOperator {
  // Operator type    Operator symbol   Operator class    DynamoDB operator    Precedence number
  EQ(DynamoDBFilterOperatorType.BINARY, "=", DynamoDBFilterOperatorConstants.UDF_PREFIX + "Equal", "EQ", 1),
  IN(DynamoDBFilterOperatorType.NARY, "in", DynamoDBFilterOperatorConstants.UDF_PREFIX + "In", "IN", 2),
  // Between does not map directly to any Hive predicate
  BETWEEN(DynamoDBFilterOperatorType.NARY, null, null, "BETWEEN", 3),
  LE(DynamoDBFilterOperatorType.BINARY, "<=", DynamoDBFilterOperatorConstants.UDF_OP_PREFIX + "OPEqualOrLessThan", "LE", 4),
  GE(DynamoDBFilterOperatorType.BINARY, ">=", DynamoDBFilterOperatorConstants.UDF_OP_PREFIX + "OPEqualOrGreaterThan", "GE", 5),
  LT(DynamoDBFilterOperatorType.BINARY, "<", DynamoDBFilterOperatorConstants.UDF_OP_PREFIX + "OPLessThan", "LT", 6),
  GT(DynamoDBFilterOperatorType.BINARY, ">", DynamoDBFilterOperatorConstants.UDF_OP_PREFIX + "OPGreaterThan", "GT", 7),
  NE(DynamoDBFilterOperatorType.BINARY, "<>", DynamoDBFilterOperatorConstants.UDF_OP_PREFIX + "OPNotEqual", "NE", 8),
  N_NULL(DynamoDBFilterOperatorType.UNARY, "isnull", DynamoDBFilterOperatorConstants.UDF_OP_PREFIX + "OPNotNull", "NOT_NULL", 9),
  NULL(DynamoDBFilterOperatorType.UNARY, "isnotnull", DynamoDBFilterOperatorConstants.UDF_OP_PREFIX + "OPNull", "NULL", 10);

  private static final Map<String, DynamoDBFilterOperator> operatorSymbolMap;
  private static final Map<String, DynamoDBFilterOperator> hiveClassMap;

  static {
    operatorSymbolMap = new HashMap<>();
    hiveClassMap = new HashMap<>();
    for (DynamoDBFilterOperator op : DynamoDBFilterOperator.values()) {
      operatorSymbolMap.put(op.getOperatorSymbol(), op);
      hiveClassMap.put(op.getHiveClass(), op);
    }
  }

  private final DynamoDBFilterOperatorType type;
  private final String operatorSymbol;
  private final String hiveClass;
  private final String dynamoDBName;
  private final int precedenceNumber;

  DynamoDBFilterOperator(DynamoDBFilterOperatorType type, String operatorSymbol, String
      hiveClass, String dynamoDBName, int precedenceNumber) {
    this.type = type;
    this.operatorSymbol = operatorSymbol;
    this.hiveClass = hiveClass;
    this.dynamoDBName = dynamoDBName;
    this.precedenceNumber = precedenceNumber;
  }

  public static DynamoDBFilterOperator getFilterOperationFromOperatorSymbol(String comparisonOp) {
    return operatorSymbolMap.get(comparisonOp);
  }

  public static DynamoDBFilterOperator getFilterOperationFromHiveClass(String comparisonOp) {
    return hiveClassMap.get(comparisonOp);
  }

  public DynamoDBFilterOperatorType getType() {
    return type;
  }

  public String getHiveClass() {
    return hiveClass;
  }

  public String getOperatorSymbol() {
    return operatorSymbol;
  }

  public String getDynamoDBName() {
    return dynamoDBName;
  }

  public boolean isLowerPrecedence(DynamoDBFilterOperator op) {
    return this.precedenceNumber > op.precedenceNumber;
  }
}
