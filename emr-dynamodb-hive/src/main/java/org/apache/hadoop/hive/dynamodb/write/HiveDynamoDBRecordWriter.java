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
import org.apache.hadoop.dynamodb.write.DefaultDynamoDBRecordWriter;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;

class HiveDynamoDBRecordWriter extends DefaultDynamoDBRecordWriter implements RecordWriter {

  private static final Text DUMMY_KEY = new Text();

  HiveDynamoDBRecordWriter(JobConf jobConf, Progressable progress) throws IOException {
    super(jobConf, progress);
  }

  @Override
  public void close(boolean abort) throws IOException {
    close(Reporter.NULL);
  }

  @Override
  public void write(Writable writable) throws IOException {
    write(DUMMY_KEY, (DynamoDBItemWritable) writable);
  }
}
