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

import java.io.DataInputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.lang.Math;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

public class DynamoDBItemWritable implements Writable, Serializable {

  public static final Type type = new TypeToken<Map<String, AttributeValue>>() {}.getType();

  // ^B, a.k.a STX
  static final String START_OF_TEXT = Character.toString((char) 02);
  // ^C, a.k.a ETX
  static final String END_OF_TEXT = Character.toString((char) 03);
  private static final char FIRST_MAGIC_BYTES = 0x0001;
  private static final byte NEXT_MAGIC_BYTE = 0x00;

  private Map<String, AttributeValue> dynamoDBItem;

  public DynamoDBItemWritable() {
    dynamoDBItem = new HashMap<>();
  }

  public DynamoDBItemWritable(Map<String, AttributeValue> dynamoDBItem) {
    this.dynamoDBItem = dynamoDBItem;
  }

  // Remember: By changing this method, you change how items are serialized
  // to disk, including for backups in S3. At least the read method needs to
  // be backwards compatible - if it's not, move marshalling into the export
  // format itself and make sure that's backward compatible.
  @Override
  public void readFields(DataInput in) throws IOException {
    readFieldsStream(readStringFromDataInput(in));
  }

  // Reads what might be either the result of a single call to
  // DataOutput.writeUTF() (serialized with a previous version of this class) or
  // the current chunked format.  DataOutput.writeUTF8 writes a two byte length
  // (in number of bytes) field, and then writes "modified UTF8" to encode the
  // string.  Crucially, in that modified UTF8 format the null character
  // '\u0000' is always written with two bytes.  Thus the sequence 00000000
  // 00000001 00000000 will not occur as a result of DataOutput.writeUTF and so
  // we use it as a magic sequence signifying the chunked format.
  private String readStringFromDataInput(DataInput in) throws IOException {
    byte[] data;
    char firstBytes = in.readChar();

    if (firstBytes == FIRST_MAGIC_BYTES) {
      byte nextByte = in.readByte();
      if (nextByte == NEXT_MAGIC_BYTE) {
        // After those three magic bytes the real input begins
        return readChunks(in);
      } else {
        data = new byte[3];
        data[0] = (byte) (firstBytes >> 8);
        data[1] = (byte) firstBytes;
        data[2] = nextByte;
      }
    } else {
      data = new byte[firstBytes + 2];
      data[0] = (byte) (firstBytes >> 8);
      data[1] = (byte) firstBytes;
      in.readFully(data, 2, firstBytes);
    }

    // In case the read bytes are not from the new input format, back up and
    // return the result readUTF would have returned
    return new DataInputStream(new ByteArrayInputStream(data)).readUTF();
  }

  private String readChunks(DataInput in) throws IOException {
    int numChunks = in.readInt();
    StringBuilder whole = new StringBuilder();
    for (int c = 0; c < numChunks; c++) {
      String chunk = in.readUTF();
      whole.append(chunk);
    }
    return whole.toString();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    String whole = writeStream();
    int chunkSize = 1 << 14;
    int chunks = whole.length() / chunkSize;
    if (whole.length() % chunkSize != 0) {
      chunks += 1;
    }

    out.writeChar(FIRST_MAGIC_BYTES);
    out.write(new byte[]{NEXT_MAGIC_BYTE});

    out.writeInt(chunks);
    for (int c = 0; c < chunks; c++) {
      String chunk = whole.substring(c * chunkSize, Math.min((c + 1) * chunkSize, whole.length()));
      out.writeUTF(chunk);
    }
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
