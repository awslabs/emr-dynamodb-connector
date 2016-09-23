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

package org.apache.hadoop.dynamodb;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

public class DynamoDBItemWritable implements Writable {

  public static final Type type = new TypeToken<Map<String, AttributeValue>>() {}.getType();

  // ^B, a.k.a STX
  static final String START_OF_TEXT = Character.toString((char) 02);
  // ^C, a.k.a ETX
  static final String END_OF_TEXT = Character.toString((char) 03);

  private Map<String, AttributeValue> dynamoDBItem = new HashMap<>();

  // Remember: By changing this method, you change how items are serialized
  // to disk, including for backups in S3. At least the read method needs to
  // be backwards compatible - if it's not, move marshalling into the export
  // format itself and make sure that's backward compatible.
  @Override
  public void readFields(DataInput in) throws IOException {
    readFieldsStream(in.readUTF());
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(writeStream());
  }

  public void readFieldsStream(String string) {
    Gson gson = DynamoDBUtil.getGson();
    String itemJson = fixMalformedJson(string);
    dynamoDBItem = gson.fromJson(itemJson, type);
  }

  public String writeStream() {
    Gson gson = DynamoDBUtil.getGson();
    return gson.toJson(dynamoDBItem, type);
  }

  public Map<String, AttributeValue> getItem() {
    return dynamoDBItem;
  }

  public void setItem(Map<String, AttributeValue> dynamoDBItem) {
    this.dynamoDBItem = dynamoDBItem;
  }

  @Override
  public String toString() {
    return writeStream();
  }

  /**
   * The old data pipeline export job (which leverages Hive commands) outputs a broken JSON
   * representation of DynamoDB items, exception.g,
   * id^C{"s":"Seattle"}^Bscores^C{"nS":["100","30","45"]}. These ^B and ^C control characters need
   * to be replaced with commas and colons, respectively.
   */
  private String fixMalformedJson(String string) {
    string = string.trim();
    if (!string.startsWith("{")) {
      string = "{" + string + "}";
      string = string.replaceAll(START_OF_TEXT, ",");
      string = string.replaceAll(END_OF_TEXT, ":");
    }
    return string;
  }
}
