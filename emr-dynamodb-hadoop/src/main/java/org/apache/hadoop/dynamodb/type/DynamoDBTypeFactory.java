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

package org.apache.hadoop.dynamodb.type;

import java.util.HashMap;
import java.util.Map;

public class DynamoDBTypeFactory {

  public static final DynamoDBType STRING_TYPE = new DynamoDBStringType();
  public static final DynamoDBType NUMBER_TYPE = new DynamoDBNumberType();
  public static final DynamoDBType BINARY_TYPE = new DynamoDBBinaryType();
  public static final DynamoDBType NUMBER_SET_TYPE = new DynamoDBNumberSetType();
  public static final DynamoDBType STRING_SET_TYPE = new DynamoDBStringSetType();
  public static final DynamoDBType BINARY_SET_TYPE = new DynamoDBBinarySetType();

  private static final Map<String, DynamoDBType> dynamoDBTypeMap = new HashMap<>();

  static {
    dynamoDBTypeMap.put(STRING_TYPE.getDynamoDBType(), STRING_TYPE);
    dynamoDBTypeMap.put(NUMBER_TYPE.getDynamoDBType(), NUMBER_TYPE);
    dynamoDBTypeMap.put(BINARY_TYPE.getDynamoDBType(), BINARY_TYPE);
    dynamoDBTypeMap.put(NUMBER_SET_TYPE.getDynamoDBType(), NUMBER_SET_TYPE);
    dynamoDBTypeMap.put(STRING_SET_TYPE.getDynamoDBType(), STRING_SET_TYPE);
    dynamoDBTypeMap.put(BINARY_SET_TYPE.getDynamoDBType(), BINARY_SET_TYPE);
  }

  public static DynamoDBType getTypeObjectFromDynamoDBType(String dynamoDBType) {
    return dynamoDBTypeMap.get(dynamoDBType);
  }
}
