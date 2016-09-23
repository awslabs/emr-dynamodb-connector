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

package org.apache.hadoop.hive.dynamodb.util;

import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.mapred.JobConf;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public final class HiveDynamoDBUtil {

  private static final Log LOG = LogFactory.getLog(HiveDynamoDBUtil.class);
  private static final Type MAP_TYPE = new TypeToken<Map<String, String>>() {}.getType();
  private static final Gson gson = new GsonBuilder().disableHtmlEscaping().create();

  private HiveDynamoDBUtil() {}

  public static String getDynamoDBTableName(String tablePropertyDefinedTableName, String
      hiveTableName) {
    if ((tablePropertyDefinedTableName == null) || (tablePropertyDefinedTableName.isEmpty())) {
      return hiveTableName;
    } else {
      return tablePropertyDefinedTableName;
    }
  }

  public static Map<String, String> getHiveToDynamoDBSchemaMapping(String mapping) {
    return getSchemaMapping(mapping, false);
  }

  public static String toJsonString(Map<String, String> dynamoDBTypeMapping) {
    return gson.toJson(dynamoDBTypeMapping, MAP_TYPE);
  }

  public static Map<String, String> fromJsonString(String jsonString) {
    return gson.fromJson(jsonString, MAP_TYPE);
  }

  /**
   * Please note that this method converts the hive column names to lower case for consistency
   * with other Hive code
   */
  private static Map<String, String> getSchemaMapping(String mapping, boolean reverseMapping) {
    if ((mapping == null) || (mapping.isEmpty())) {
      /*
       * User may have chosen to map entire DynamoDB item to a single hive
       * column of map<string, string> type.
       */
      return Maps.newHashMap();
    }

    String[] colMapArr = mapping.split(",");
    Map<String, String> columnMapping = new HashMap<>();
    for (String colMap : colMapArr) {
      String[] nameMap = colMap.split(":", 2);
      if (nameMap.length != 2) {
        throw new RuntimeException("Invalid column mapping " + colMap);
      }
      if (reverseMapping) {
        columnMapping.put(nameMap[1], nameMap[0].toLowerCase());
      } else {
        columnMapping.put(nameMap[0].toLowerCase(), nameMap[1]);
      }
    }

    return columnMapping;
  }

  /**
   * Extracts column to type mapping for the hive table from the job configuration It uses 2
   * standard Hive Job parameters: LIST_COLUMNS ("columns") and LIST_COLUMN_TYPES
   * LIST_COLUMN_TYPES ("columns.types").
   *
   * @param jobConf a job configuration
   * @return mapping between column names and column types
   * @see serdeConstants#LIST_COLUMNS
   * @see serdeConstants#LIST_COLUMN_TYPES
   */
  public static Map<String, String> extractHiveTypeMapping(JobConf jobConf) {
    Map<String, String> map = new HashMap<>();
    String columnsString = jobConf.get(serdeConstants.LIST_COLUMNS);
    if (columnsString == null || columnsString.isEmpty()) {
      LOG.warn("List of columns was not provided in job configuration");
      return map;
    }
    String[] columns = columnsString.split(",");
    String[] types = splitStructs(jobConf.get(serdeConstants.LIST_COLUMN_TYPES), ',');

    //There is a possibility that column names and types are still not well formatted
    //(separated by commas) when this method is called from Hive. In that case, just return
    //empty map.
    if (types == null) {
      LOG.warn("Invalid input for LIST_COLUMN_TYPES");
      return map;
    }
    if (columns.length != types.length) {
      LOG.warn("Expected " + columns.length + " types but found " + types.length);
      return map;
    }

    for (int i = 0; i < columns.length; i++) {
      map.put(columns[i], types[i]);
    }
    LOG.debug("Hive columns to types mapping: " + map);
    return map;
  }

  private static String[] splitStructs(String str, char separator) {
    if (str == null) {
      return null;
    }
    int len = str.length();
    if (len == 0) {
      return null;
    }
    ArrayList<String> list = new ArrayList<String>();
    int index = 0;
    int start = 0;
    int match = 0;
    while (index < len) {
      if (str.charAt(index) == '<') {
        match++;
      } else if (str.charAt(index) == '>') {
        match--;
      } else if (str.charAt(index) == separator) {
        if (match == 0) {
          list.add(str.substring(start, index).trim());
          start = ++index;
          continue;
        }
      }
      index++;
    }

    if (match != 0) {
      return null;
    }
    list.add(str.substring(start, index).trim());
    String[] result = new String[list.size()];
    list.toArray(result);

    return result;
  }

}
