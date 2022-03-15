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

import com.amazonaws.services.dynamodbv2.model.TableDescription;
import java.util.Date;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.dynamodb.DynamoDBClient;
import org.apache.hadoop.dynamodb.DynamoDBConstants;
import org.apache.hadoop.dynamodb.DynamoDBUtil;
import org.apache.hadoop.dynamodb.exportformat.ExportManifestOutputFormat;
import org.apache.hadoop.dynamodb.read.DynamoDBInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DynamoDBExport extends Configured implements Tool {

  public static final Log log = LogFactory.getLog(DynamoDBExport.class);

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new DynamoDBExport(), args);
    System.exit(res);
  }

  public JobConf initJobConf(JobConf jobConf, Path outputPath, String tableName, Double readRatio,
      Integer totalSegments) {
    jobConf.setJobName("dynamodb-export");
    jobConf.setOutputKeyClass(Text.class);
    jobConf.setOutputValueClass(Text.class);
    jobConf.setMapperClass(ExportMapper.class);
    jobConf.setReducerClass(IdentityReducer.class);
    jobConf.setInputFormat(DynamoDBInputFormat.class);
    jobConf.setOutputFormat(ExportManifestOutputFormat.class);
    jobConf.setNumReduceTasks(1);
    FileOutputFormat.setOutputPath(jobConf, outputPath);

    setTableProperties(jobConf, tableName, readRatio, totalSegments);
    return jobConf;
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      printUsage("Not enough parameters");
      return -1;
    }

    Path outputPath = new Path(args[0]);
    String tableName = args[1];
    Double readRatio = null;
    if (args.length >= 3) {
      String val = args[2];
      try {
        readRatio = Double.parseDouble(val);
      } catch (Exception e) {
        printUsage("Could not parse read ratio (value: " + val + ")");
        return -1;
      }
    }
    Integer totalSegments = null;
    if (args.length >= 4) {
      String val = args[3];
      try {
        totalSegments = Integer.parseInt(val);
      } catch (Exception e) {
        printUsage("Could not parse segment count (value: " + val + ")");
        return -1;
      }
    }

    JobConf jobConf =
        initJobConf(new JobConf(getConf(), DynamoDBExport.class), outputPath, tableName, readRatio,
            totalSegments);

    Date startTime = new Date();
    System.out.println("Job started: " + startTime);
    JobClient.runJob(jobConf);
    Date endTime = new Date();
    System.out.println("Job ended: " + endTime);
    System.out.println("The job took " + (endTime.getTime() - startTime.getTime()) / 1000 + " "
        + "seconds.");
    System.out.println("Output path: " + outputPath);

    return 0;
  }

  private void setTableProperties(JobConf jobConf, String tableName, Double readRatio, Integer
      totalSegments) {
    jobConf.set(DynamoDBConstants.TABLE_NAME, tableName);
    jobConf.set(DynamoDBConstants.INPUT_TABLE_NAME, tableName);
    jobConf.set(DynamoDBConstants.OUTPUT_TABLE_NAME, tableName);

    DynamoDBClient client = new DynamoDBClient(jobConf);
    TableDescription description = client.describeTable(tableName);

    Long itemCount = description.getItemCount();
    Long tableSizeBytes = description.getTableSizeBytes();

    if (description.getBillingModeSummary() == null
            || description.getBillingModeSummary().getBillingMode()
        .equals(DynamoDBConstants.BILLING_MODE_PROVISIONED)) {
      jobConf.set(DynamoDBConstants.READ_THROUGHPUT,
          description.getProvisionedThroughput().getReadCapacityUnits().toString());
      jobConf.set(DynamoDBConstants.WRITE_THROUGHPUT,
          description.getProvisionedThroughput().getWriteCapacityUnits().toString());
    } else {
      // If not specified at the table level, set a hard coded value of 40,000
      jobConf.set(DynamoDBConstants.READ_THROUGHPUT,
          DynamoDBConstants.DEFAULT_CAPACITY_FOR_ON_DEMAND.toString());
      jobConf.set(DynamoDBConstants.WRITE_THROUGHPUT,
          DynamoDBConstants.DEFAULT_CAPACITY_FOR_ON_DEMAND.toString());
    }

    jobConf.set(DynamoDBConstants.ITEM_COUNT, itemCount.toString());
    jobConf.set(DynamoDBConstants.TABLE_SIZE_BYTES, tableSizeBytes.toString());

    Double averageItemSize = DynamoDBUtil.calculateAverageItemSize(description);
    jobConf.set(DynamoDBConstants.AVG_ITEM_SIZE, averageItemSize.toString());

    log.info("Read throughput:       " + jobConf.get(DynamoDBConstants.READ_THROUGHPUT));
    log.info("Write throughput:      " + jobConf.get(DynamoDBConstants.WRITE_THROUGHPUT));
    log.info("Item count:            " + itemCount);
    log.info("Table size:            " + tableSizeBytes);
    log.info("Average item size:     " + averageItemSize);

    // Optional properties
    if (readRatio != null) {
      jobConf.set(DynamoDBConstants.THROUGHPUT_READ_PERCENT, readRatio.toString());
      log.info("Throughput read ratio: " + readRatio);
    }

    if (totalSegments != null) {
      jobConf.set(DynamoDBConstants.SCAN_SEGMENTS, totalSegments.toString());
      log.info("Total segment count:   " + totalSegments);
    }
  }

  private void printUsage(String error) {
    if (error != null) {
      System.out.println("Error: " + error);
    }

    System.out.println("Usage: Export <path> <table-name> [<read-ratio>] [<total-segment-count>]");
    ToolRunner.printGenericCommandUsage(System.out);
  }
}
