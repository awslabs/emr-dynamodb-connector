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

package org.apache.hadoop.hive.dynamodb.write;

import org.apache.hadoop.dynamodb.DynamoDBItemWritable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.Properties;

public class HiveDynamoDBOutputFormat implements HiveOutputFormat<Text, DynamoDBItemWritable> {

  @Override
  public RecordWriter getHiveRecordWriter(JobConf jc, Path finalOutPath, Class<? extends
      Writable> valueClass, boolean isCompressed, Properties tableProperties, Progressable
      progressable) throws IOException {
    return new HiveDynamoDBRecordWriter(jc, progressable);
  }

  @Override
  public void checkOutputSpecs(FileSystem arg0, JobConf arg1) throws IOException {
  }

  @Override
  public org.apache.hadoop.mapred.RecordWriter<Text, DynamoDBItemWritable> getRecordWriter(
      FileSystem fs, JobConf conf, String str, Progressable progressable) throws IOException {
    throw new RuntimeException("This class implements HiveRecordReader. It is not compatible with"
        + " Hadoop Record Reader.");
  }

}
