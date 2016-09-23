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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

import java.io.IOException;

public class ImportCombineFileRecordReader implements RecordReader<NullWritable,
    DynamoDBItemWritable> {

  private final CombineFileSplit combineFileSplit;
  private final JobConf job;
  private final Reporter reporter;

  private RecordReader<NullWritable, DynamoDBItemWritable> currentRecordReader;
  private int processedPathCount;

  public ImportCombineFileRecordReader(CombineFileSplit combineFileSplit, JobConf job, Reporter
      reporter) throws IOException {
    this.combineFileSplit = combineFileSplit;
    this.job = job;
    this.reporter = reporter;

    processedPathCount = 0;
    currentRecordReader = getRecordReader(combineFileSplit.getPath(processedPathCount));
  }

  @Override
  public boolean next(NullWritable key, DynamoDBItemWritable value) throws IOException {
    if (currentRecordReader.next(key, value)) {
      return true;
    }

    processedPathCount++;
    if (processedPathCount < combineFileSplit.getNumPaths()) {
      currentRecordReader.close();
      currentRecordReader = getRecordReader(combineFileSplit.getPath(processedPathCount));
      return currentRecordReader.next(key, value);
    }

    return false;
  }

  @Override
  public NullWritable createKey() {
    return currentRecordReader.createKey();
  }

  @Override
  public DynamoDBItemWritable createValue() {
    return currentRecordReader.createValue();
  }

  @Override
  public long getPos() throws IOException {
    return currentRecordReader.getPos();
  }

  @Override
  public float getProgress() throws IOException {
    return (float) (processedPathCount) / combineFileSplit.getNumPaths();
  }

  @Override
  public void close() throws IOException {
    currentRecordReader.close();
  }

  private RecordReader<NullWritable, DynamoDBItemWritable> getRecordReader(Path path) throws
      IOException {
    reporter.setStatus("Reading " + path);
    return new ImportRecordReader(job, path);
  }

}
