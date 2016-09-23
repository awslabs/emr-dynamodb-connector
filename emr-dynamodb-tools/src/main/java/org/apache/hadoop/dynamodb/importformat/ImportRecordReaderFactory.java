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

import org.apache.hadoop.dynamodb.DynamoDBConstants;
import org.apache.hadoop.dynamodb.DynamoDBItemWritable;
import org.apache.hadoop.dynamodb.exportformat.ExportManifestRecordWriter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

import java.io.IOException;

final class ImportRecordReaderFactory {

  static RecordReader<NullWritable, DynamoDBItemWritable> getRecordReader(
      InputSplit inputSplit, JobConf job, Reporter reporter) throws IOException {
    // CombineFileSplit indicates the new export format which includes a manifest file
    if (inputSplit instanceof CombineFileSplit) {
      int version = job.getInt(DynamoDBConstants.EXPORT_FORMAT_VERSION, -1);
      if (version != ExportManifestRecordWriter.FORMAT_VERSION) {
        throw new IOException("Unknown version: " + job.get(DynamoDBConstants
            .EXPORT_FORMAT_VERSION));
      }
      return new ImportCombineFileRecordReader((CombineFileSplit) inputSplit, job, reporter);
    } else if (inputSplit instanceof FileSplit) {
      // FileSplit indicates the old data pipeline format which doesn't include a manifest file
      Path path = ((FileSplit) inputSplit).getPath();
      return new ImportRecordReader(job, path);
    } else {
      throw new IOException("Expecting CombineFileSplit or FileSplit but the input split type is:"
          + " " + inputSplit.getClass());
    }
  }

}
