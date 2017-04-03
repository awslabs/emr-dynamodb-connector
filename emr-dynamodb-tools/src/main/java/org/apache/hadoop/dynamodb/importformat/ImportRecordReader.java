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

package org.apache.hadoop.dynamodb.importformat;

import org.apache.hadoop.dynamodb.DynamoDBItemWritable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

public class ImportRecordReader implements RecordReader<NullWritable, DynamoDBItemWritable> {

  private final LineReader lineReader;

  public ImportRecordReader(JobConf job, Path path) throws IOException {
    FileSystem fs = path.getFileSystem(job);
    FSDataInputStream fileIn = fs.open(path);
    CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(job);
    CompressionCodec codec = compressionCodecs.getCodec(path);
    if (null != codec) {
      Decompressor decompressor = CodecPool.getDecompressor(codec);
      this.lineReader = new LineReader(codec.createInputStream(fileIn, decompressor), job);
    } else {
      this.lineReader = new LineReader(fileIn, job);
    }
  }

  @Override
  public void close() throws IOException {
    if (lineReader != null) {
      lineReader.close();
    }
  }

  @Override
  public NullWritable createKey() {
    return NullWritable.get();
  }

  @Override
  public DynamoDBItemWritable createValue() {
    return new DynamoDBItemWritable();
  }

  @Override
  public long getPos() throws IOException {
    return 0;
  }

  @Override
  public float getProgress() throws IOException {
    return 0;
  }

  @Override
  public boolean next(NullWritable key, DynamoDBItemWritable value) throws IOException {
    Text line = new Text();
    int bytes = lineReader.readLine(line);
    if (bytes == 0) {
      return false;
    }
    value.readFieldsStream(line.toString());
    return true;
  }

}
