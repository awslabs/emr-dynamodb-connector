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

import com.google.common.base.Charsets;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class ExportManifestRecordWriter<K> implements RecordWriter<K, Text> {

  public static final int FORMAT_VERSION = 3;

  private static final String UTF_8 = "UTF-8";
  private static final String RIGHT_BRACE = "}";

  private static final String ENTRIES_START = "\"entries\": [\n";
  private static final String ENTRIES_END = "\n]";

  private static final String S3N_PREFIX = "s3n://";
  private static final String S3_PREFIX = "s3://";

  private static final byte[] SEPARATOR_NEWLINE;

  static {
    try {
      SEPARATOR_NEWLINE = ",\n".getBytes(UTF_8);
    } catch (UnsupportedEncodingException uee) {
      throw new IllegalArgumentException("Can't find " + UTF_8 + " encoding");
    }
  }

  private final DataOutputStream out;
  private final Path outputFolder;
  private final String entrySuffix;
  private int itemCount = 0;

  public ExportManifestRecordWriter(DataOutputStream out, Path outputFolder, String entrySuffix)
      throws IOException {
    this.out = out;
    this.outputFolder = outputFolder;
    this.entrySuffix = entrySuffix;
    writeHeader();
  }

  @Override
  public synchronized void write(K key, Text value) throws IOException {
    if (itemCount > 0) {
      out.write(SEPARATOR_NEWLINE);
    }
    itemCount++;

    ExportManifestEntry entry = createExportEntry(value);
    out.write(entry.writeStream().getBytes(UTF_8));
  }

  @Override
  public synchronized void close(Reporter reporter) throws IOException {
    out.write(ENTRIES_END.getBytes(UTF_8));
    out.write(RIGHT_BRACE.getBytes(UTF_8));
    out.close();
  }

  private ExportManifestEntry createExportEntry(Text value) {
    String entryName = new String(value.getBytes(), Charsets.UTF_8) + entrySuffix;
    String path = new Path(outputFolder, entryName).toUri().toString();

    if (path.startsWith(S3N_PREFIX)) {
      path = S3_PREFIX + path.substring(S3N_PREFIX.length());
    }

    return new ExportManifestEntry(path);
  }

  /**
   * An example output is as follows:
   *
   * {"name":"DynamoDB-export", "version":3, "entries":[
   */
  private void writeHeader() throws IOException {
    String headerJson = new ExportFileHeader(FORMAT_VERSION).writeStream();
    String headerString = headerJson.substring(0, headerJson.lastIndexOf(RIGHT_BRACE.charAt(0)));
    out.write(headerString.getBytes(UTF_8));

    out.write(SEPARATOR_NEWLINE);
    out.write(ENTRIES_START.getBytes(UTF_8));
  }
}
