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

package org.apache.hadoop.dynamodb.exportformat;

import org.apache.hadoop.dynamodb.DynamoDBItemWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

class ExportRecordWriter implements RecordWriter<NullWritable, DynamoDBItemWritable> {

  private static final String UTF_8 = "UTF-8";
  private static final byte[] NEWLINE;

  static {
    try {
      NEWLINE = "\n".getBytes(UTF_8);
    } catch (UnsupportedEncodingException uee) {
      throw new IllegalArgumentException("can't find " + UTF_8 + " encoding");
    }
  }

  private final DataOutputStream out;

  public ExportRecordWriter(DataOutputStream out) throws IOException {
    this.out = out;
  }

  @Override
  public synchronized void write(NullWritable key, DynamoDBItemWritable value) throws IOException {
    out.write(value.writeStream().getBytes(UTF_8));
    out.write(NEWLINE);
  }

  @Override
  public synchronized void close(Reporter reporter) throws IOException {
    out.close();
  }

}
