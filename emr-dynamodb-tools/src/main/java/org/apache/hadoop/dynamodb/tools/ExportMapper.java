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

package org.apache.hadoop.dynamodb.tools;

import java.io.IOException;
import java.util.UUID;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.dynamodb.DynamoDBItemWritable;
import org.apache.hadoop.dynamodb.exportformat.ExportFileFlusher;
import org.apache.hadoop.dynamodb.exportformat.ExportOutputFormat;
import org.apache.hadoop.dynamodb.util.TimeSource;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

class ExportMapper extends MapReduceBase implements Mapper<Text, DynamoDBItemWritable, Text, Text> {

  private static final Log log = LogFactory.getLog(ExportMapper.class);
  //10M/file for observations creates ~400 files and for audience identity creates ~2500 files.
  //  good enough for now...
  private static final int MAX_ITEM_COUNT_PER_FILE = 100000 * 100;
  private final OutputFormat<NullWritable, DynamoDBItemWritable> outputFormat = new
      ExportOutputFormat();
  private final TimeSource time = new TimeSource();
  private final ExportFileFlusher flusher = new ExportFileFlusher(time);
  private long itemCount = 0;
  private JobConf jobConf;
  private RecordWriter<NullWritable, DynamoDBItemWritable> recordWriter;

  @Override
  public void map(Text key, DynamoDBItemWritable value, OutputCollector<Text, Text> output,
      Reporter reporter) throws IOException {
    // Rotate output file if needed
    if (itemCount % MAX_ITEM_COUNT_PER_FILE == 0) {
      long start = time.getNanoTime();
      if (recordWriter != null) {
        flusher.close(recordWriter, reporter);
      }

      String newOutputFilename = generateFilename();
      recordWriter = outputFormat.getRecordWriter(null, jobConf, newOutputFilename, reporter);

      long duration = time.getTimeSinceMs(start);
      log.info("Rotated over to file: " + newOutputFilename + " in " + (duration / 1000.0) + " "
          + "seconds.");

      // When the reducer collects these filenames we want them to be
      // shuffled around - both to increase write spread on DynamoDB and
      // read spread on S3 when we later consume the data. We achieve this
      // by providing the reverse of the filename as the key in the mapper
      // output.
      String sortKey = new StringBuilder(newOutputFilename).reverse().toString();
      output.collect(new Text(sortKey), new Text(newOutputFilename));
      reporter.incrCounter(Counters.OUTPUT_FILES, 1);
    }

    // Write item to output file
    recordWriter.write(NullWritable.get(), value);
    reporter.incrCounter(Counters.DYNAMODB_ITEMS_READ, 1);

    itemCount++;
  }

  @Override
  public void close() throws IOException {
    if (recordWriter != null) {
      flusher.close(recordWriter, Reporter.NULL);
    }
    flusher.sync();
  }

  @Override
  public void configure(JobConf job) {
    jobConf = job;
  }

  private String generateFilename() {
    return UUID.randomUUID().toString();
  }

  private enum Counters {

    DYNAMODB_ITEMS_READ, OUTPUT_FILES,
  }
}
