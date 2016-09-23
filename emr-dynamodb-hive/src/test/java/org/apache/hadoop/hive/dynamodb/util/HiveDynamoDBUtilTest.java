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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class HiveDynamoDBUtilTest {
  static final String COL = "col";
  static final String BLOCK__OFFSET_INSIDE_FILE = "BLOCK__OFFSET_INSIDE_FILE";
  static final String INPUT_FILE_NAME = "INPUT_FILE_NAME";
  static final String ROW_ID = "ROW_ID";
  static final String COLUMN_STRING = String.format("%s,%s,%s,%s", COL,
      BLOCK__OFFSET_INSIDE_FILE, INPUT_FILE_NAME, ROW_ID);
  private final JobConf conf = new JobConf();
  private Map<String, String> map;

  @Before
  public void setup() {
    map = new HashMap<>();
    conf.set(serdeConstants.LIST_COLUMNS, COLUMN_STRING);
  }

  @Test
  public void testExtractHiveTypeMappingWithStruct() {
    String typeString = "string,bigint,string,struct<transactionid:bigint,bucketid:int," +
        "rowid:bigint>";
    conf.set(serdeConstants.LIST_COLUMN_TYPES, typeString);
    map = HiveDynamoDBUtil.extractHiveTypeMapping(conf);

    assertFalse(map.isEmpty());
    assertTrue(map.get(COL).equals("string"));
    assertTrue(map.get(BLOCK__OFFSET_INSIDE_FILE).equals("bigint"));
    assertTrue(map.get(INPUT_FILE_NAME).equals("string"));
    assertTrue(map.get(ROW_ID).equals("struct<transactionid:bigint,bucketid:int,rowid:bigint>"));
  }

  @Test
  public void testExtractHiveTypeMappingWithOutStruct() {
    String typeString = "string,bigint,string,bigint";
    conf.set(serdeConstants.LIST_COLUMN_TYPES, typeString);
    map = HiveDynamoDBUtil.extractHiveTypeMapping(conf);

    assertFalse(map.isEmpty());
    assertTrue(map.get(COL).equals("string"));
    assertTrue(map.get(BLOCK__OFFSET_INSIDE_FILE).equals("bigint"));
    assertTrue(map.get(INPUT_FILE_NAME).equals("string"));
    assertTrue(map.get(ROW_ID).equals("bigint"));
  }

  @Test
  public void testExtractHiveTypeMappingWithNestedStruct() {
    String typeString = "string,bigint,struct<transactionid:bigint,bucketid:int,rowid:bigint>," +
        "struct<transactionid:bigint,bucketid:int,rowid:struct<bigint>>";
    conf.set(serdeConstants.LIST_COLUMN_TYPES, typeString);
    map = HiveDynamoDBUtil.extractHiveTypeMapping(conf);

    assertFalse(map.isEmpty());
    assertTrue(map.get(COL).equals("string"));
    assertTrue(map.get(BLOCK__OFFSET_INSIDE_FILE).equals("bigint"));
    assertTrue(map.get(INPUT_FILE_NAME).equals("struct<transactionid:bigint,bucketid:int," +
        "rowid:bigint>"));
    assertTrue(map.get(ROW_ID).equals("struct<transactionid:bigint,bucketid:int," +
        "rowid:struct<bigint>>"));
  }

  @Test
  public void testExtractHiveTypeMappingWithInvalidInput() {
    String typeString = "string,bigint,string,struct<bigint";
    conf.set(serdeConstants.LIST_COLUMN_TYPES, typeString);
    map = HiveDynamoDBUtil.extractHiveTypeMapping(conf);

    assertTrue(map.isEmpty());
  }

  @Test
  public void testExtractHiveTypeMappingWithDiffColumnAndTypeNumber() {
    String typeString = "string,bigint,struct<transactionid:bigint,bucketid:int,rowid:bigint>";
    conf.set(serdeConstants.LIST_COLUMN_TYPES, typeString);
    map = HiveDynamoDBUtil.extractHiveTypeMapping(conf);

    assertTrue(map.isEmpty());
  }
}
