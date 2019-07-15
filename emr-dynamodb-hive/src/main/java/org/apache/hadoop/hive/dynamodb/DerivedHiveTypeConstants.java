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

package org.apache.hadoop.hive.dynamodb;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.serde.serdeConstants;

import java.util.Arrays;
import java.util.List;

public class DerivedHiveTypeConstants {

  private static final List<Character> COLLECTION_TYPE_DELIMITERS = Arrays.asList('<', '>');
  private static final char MAP_KEY_VALUE_TYPE_DELIMITER = ',';

  public static final String BIGINT_ARRAY_TYPE_NAME = setArrayElementType(serdeConstants.BIGINT_TYPE_NAME);
  public static final String DOUBLE_ARRAY_TYPE_NAME = setArrayElementType(serdeConstants.DOUBLE_TYPE_NAME);
  public static final String STRING_ARRAY_TYPE_NAME = setArrayElementType(serdeConstants.STRING_TYPE_NAME);
  public static final String BINARY_ARRAY_TYPE_NAME = setArrayElementType(serdeConstants.BINARY_TYPE_NAME);

  /* A map<string, string> map. */
  public static final String ITEM_MAP_TYPE_NAME = setMapKeyValueTypes(serdeConstants.STRING_TYPE_NAME,
          serdeConstants.STRING_TYPE_NAME);

  private static String setArrayElementType(String elementType) {
    return serdeConstants.LIST_TYPE_NAME + StringUtils.join(COLLECTION_TYPE_DELIMITERS, elementType);
  }

  private static String setMapKeyValueTypes(String... types) {
    return serdeConstants.MAP_TYPE_NAME + StringUtils.join(COLLECTION_TYPE_DELIMITERS,
            StringUtils.join(types, MAP_KEY_VALUE_TYPE_DELIMITER));
  }

  public static String getArrayElementType(String hiveType) {
    return hiveType.substring(hiveType.indexOf(COLLECTION_TYPE_DELIMITERS.get(0)) + 1, hiveType.length() - 1);
  }
}
