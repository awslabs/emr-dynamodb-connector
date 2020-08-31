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

package org.apache.hadoop.hive.dynamodb.type;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.dynamodb.type.DynamoDBTypeConstants;
import org.apache.hadoop.dynamodb.type.DynamoDBTypeFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

public class HiveDynamoDBTypeFactory extends DynamoDBTypeFactory {

  /* Hive Map type encapsulates a DynamoDB item */
  private static final HiveDynamoDBType DYNAMODB_ITEM_TYPE = new HiveDynamoDBItemType();

  private static final Set<HiveDynamoDBType> DEFAULT_HIVE_DYNAMODB_TYPES = Sets.newHashSet(
      new HiveDynamoDBStringType(),
      new HiveDynamoDBBinaryType(),
      new HiveDynamoDBNumberType(),
      new HiveDynamoDBBooleanType(),

      new HiveDynamoDBListType(),

      new HiveDynamoDBItemType(),
      new HiveDynamoDBMapType()
  );

  private static final Set<HiveDynamoDBType> ALTERNATE_HIVE_DYNAMODB_TYPES = Sets.newHashSet(
      new HiveDynamoDBStringSetType(),
      new HiveDynamoDBBinarySetType(),
      new HiveDynamoDBNumberSetType(),
      new HiveDynamoDBNullType()
  );

  private static final Map<TypeInfo, HiveDynamoDBType> SIMPLE_HIVE_DYNAMODB_TYPES_MAP =
      Maps.newHashMap();
  private static final Set<HiveDynamoDBType> COMPLEX_HIVE_DYNAMODB_TYPES_SET = Sets.newHashSet();
  private static final Map<String, HiveDynamoDBType> DYNAMODB_TYPE_MAP = Maps.newHashMap();

  static {
    for (HiveDynamoDBType type : DEFAULT_HIVE_DYNAMODB_TYPES) {
      try {
        SIMPLE_HIVE_DYNAMODB_TYPES_MAP.put(type.getSupportedHiveType(), type);
      } catch (UnsupportedOperationException e) {
        COMPLEX_HIVE_DYNAMODB_TYPES_SET.add(type);
      }
      DYNAMODB_TYPE_MAP.put(type.getDynamoDBType(), type);
    }

    for (HiveDynamoDBType type : ALTERNATE_HIVE_DYNAMODB_TYPES) {
      DYNAMODB_TYPE_MAP.put(type.getDynamoDBType(), type);
    }
  }

  public static HiveDynamoDBType getTypeObjectFromHiveType(String hiveType) {
    return getTypeObjectFromHiveType(TypeInfoUtils.getTypeInfoFromTypeString(hiveType));
  }

  public static HiveDynamoDBType getTypeObjectFromHiveType(ObjectInspector objectInspector) {
    return getTypeObjectFromHiveType(TypeInfoUtils.getTypeInfoFromObjectInspector(objectInspector));
  }

  public static HiveDynamoDBType getTypeObjectFromHiveType(TypeInfo typeInfo) {
    if (SIMPLE_HIVE_DYNAMODB_TYPES_MAP.containsKey(typeInfo)) {
      return SIMPLE_HIVE_DYNAMODB_TYPES_MAP.get(typeInfo);
    }
    for (HiveDynamoDBType type : COMPLEX_HIVE_DYNAMODB_TYPES_SET) {
      if (type.supportsHiveType(typeInfo)) {
        return type;
      }
    }
    throw new IllegalArgumentException("Unsupported Hive type: " + typeInfo.getTypeName());
  }

  public static HiveDynamoDBType getTypeObjectFromDynamoDBType(String dynamoDBType) {
    if (DYNAMODB_TYPE_MAP.containsKey(dynamoDBType)) {
      return DYNAMODB_TYPE_MAP.get(dynamoDBType);
    }
    throw new IllegalArgumentException("Unsupported DynamoDB type: " + dynamoDBType);
  }

  /**
   * Checks if the given hiveType is a map type. Does not check key and value types.
   *
   * @param hiveType hiveType to check
   * @return {@code true} if the provided hiveType is a map type, {@code false} otherwise
   */
  public static boolean isHiveDynamoDBItemMapType(String hiveType) {
    return DYNAMODB_ITEM_TYPE.supportsHiveType(TypeInfoUtils.getTypeInfoFromTypeString(hiveType));
  }

  public static boolean isHiveDynamoDBItemMapType(ObjectInspector objectInspector) {
    return DYNAMODB_ITEM_TYPE.supportsHiveType(
        TypeInfoUtils.getTypeInfoFromObjectInspector(objectInspector));
  }

  public static boolean isHiveDynamoDBItemMapType(HiveDynamoDBType ddType) {
    return ddType.getDynamoDBType().equals(DynamoDBTypeConstants.ITEM);
  }
}
