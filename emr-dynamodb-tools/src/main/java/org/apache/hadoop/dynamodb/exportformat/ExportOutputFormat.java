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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataOutputStream;
import java.io.IOException;

public class ExportOutputFormat extends FileOutputFormat<NullWritable, DynamoDBItemWritable> {

  @Override
  public RecordWriter<NullWritable, DynamoDBItemWritable> getRecordWriter(FileSystem ignored,
      JobConf job, String name, Progressable progress) throws IOException {
    boolean isCompressed = getCompressOutput(job);
    CompressionCodec codec = null;
    String extension = "";
    DataOutputStream fileOut;

    if (isCompressed) {
      Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job, GzipCodec.class);
      codec = ReflectionUtils.newInstance(codecClass, job);
      extension = codec.getDefaultExtension();
    }

    Path file = new Path(FileOutputFormat.getOutputPath(job), name + extension);
    FileSystem fs = file.getFileSystem(job);

    if (!isCompressed) {
      fileOut = fs.create(file, progress);
    } else {
      fileOut = new DataOutputStream(codec.createOutputStream(fs.create(file, progress)));
    }

    return new ExportRecordWriter(fileOut);
  }
}
