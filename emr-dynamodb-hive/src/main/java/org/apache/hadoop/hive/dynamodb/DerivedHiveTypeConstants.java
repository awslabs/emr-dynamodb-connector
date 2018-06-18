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

import org.apache.hadoop.hive.serde.Constants;

public class DerivedHiveTypeConstants {

  public static final String BIGINT_ARRAY_TYPE_NAME = Constants.LIST_TYPE_NAME + "<" + Constants
      .BIGINT_TYPE_NAME + ">";

  public static final String DOUBLE_ARRAY_TYPE_NAME = Constants.LIST_TYPE_NAME + "<" + Constants
      .DOUBLE_TYPE_NAME + ">";

  public static final String STRING_ARRAY_TYPE_NAME = Constants.LIST_TYPE_NAME + "<" + Constants
      .STRING_TYPE_NAME + ">";

  public static final String BINARY_ARRAY_TYPE_NAME = Constants.LIST_TYPE_NAME + "<" + Constants
      .BINARY_TYPE_NAME + ">";

  /* A map<string, string> map. */
  public static final String ITEM_MAP_TYPE_NAME = Constants.MAP_TYPE_NAME + "<" + Constants
      .STRING_TYPE_NAME + "," + Constants.STRING_TYPE_NAME + ">";

  public static final String STRING_BIGINT_MAP_TYPE_NAME = Constants.MAP_TYPE_NAME + "<" + Constants.STRING_TYPE_NAME
    + "," + Constants.BIGINT_TYPE_NAME + ">";
  public static final String STRING_DOUBLE_MAP_TYPE_NAME = Constants.MAP_TYPE_NAME + "<" + Constants.STRING_TYPE_NAME
    + "," + Constants.DOUBLE_TYPE_NAME + ">";

  public static final String BIGINT_ARRAY_LIST_TYPE_NAME = Constants.LIST_TYPE_NAME + "<" + Constants
    .BIGINT_TYPE_NAME + ">";
  public static final String DOUBLE_ARRAY_LIST_TYPE_NAME = Constants.LIST_TYPE_NAME + "<" + Constants
    .DOUBLE_TYPE_NAME + ">";
  public static final String STRING_ARRAY_LIST_TYPE_NAME = Constants.LIST_TYPE_NAME + "<" + Constants
    .STRING_TYPE_NAME + ">";


  public static final String LIST_ITEM_MAP_TYPE_NAME = Constants.LIST_TYPE_NAME + "<" +
    ITEM_MAP_TYPE_NAME + ">";
  public static final String LIST_STRING_BIG_INT_MAP_TYPE_NAME = Constants.LIST_TYPE_NAME + "<" +
    STRING_BIGINT_MAP_TYPE_NAME + ">";
  public static final String LIST_STRING_BIG_DOUBLE_MAP_TYPE_NAME = Constants.LIST_TYPE_NAME + "<" +
    STRING_DOUBLE_MAP_TYPE_NAME + ">";
}
