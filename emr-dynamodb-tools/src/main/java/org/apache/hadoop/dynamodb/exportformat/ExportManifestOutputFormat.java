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

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

/**
 * Output format for the manifest.
 *
 * We mimic the Redshift manifest output format and JSON serialize each entry. We hand-craft the
 * header and footer for now, may change to a gson stream once/if we add metadata (number of files,
 * average item size, etc).
 */
public class ExportManifestOutputFormat<K> extends FileOutputFormat<K, Text> {

  public static final String MANIFEST_FILENAME = "manifest";

  @Override
  public RecordWriter<K, Text> getRecordWriter(FileSystem ignored, JobConf job, String name,
      Progressable progress) throws IOException {
    String extension = "";
    Path file = FileOutputFormat.getTaskOutputPath(job, MANIFEST_FILENAME);
    FileSystem fs = file.getFileSystem(job);
    FSDataOutputStream fileOut = fs.create(file, progress);
    if (getCompressOutput(job)) {
      Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job, GzipCodec.class);
      CompressionCodec codec = ReflectionUtils.newInstance(codecClass, job);
      extension = codec.getDefaultExtension();
    }
    return new ExportManifestRecordWriter<>(fileOut, FileOutputFormat.getOutputPath(job),
        extension);
  }
}
