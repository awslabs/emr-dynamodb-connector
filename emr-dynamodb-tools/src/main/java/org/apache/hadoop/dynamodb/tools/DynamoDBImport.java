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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.dynamodb.DynamoDBClient;
import org.apache.hadoop.dynamodb.DynamoDBConstants;
import org.apache.hadoop.dynamodb.DynamoDBItemWritable;
import org.apache.hadoop.dynamodb.importformat.ImportInputFormat;
import org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Date;

public class DynamoDBImport extends Configured implements Tool {

  public static final Log log = LogFactory.getLog(DynamoDBImport.class);

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new DynamoDBImport(), args);
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      printUsage("Not enough parameters");
      return -1;
    }

    JobConf jobConf = new JobConf(getConf(), DynamoDBImport.class);

    jobConf.setJobName("dynamodb-import");
    jobConf.setOutputKeyClass(Text.class);
    jobConf.setOutputValueClass(DynamoDBItemWritable.class);
    jobConf.setMapperClass(ImportMapper.class);
    jobConf.setReducerClass(IdentityReducer.class);
    jobConf.setInputFormat(ImportInputFormat.class);
    jobConf.setOutputFormat(DynamoDBOutputFormat.class);
    jobConf.setNumReduceTasks(0);
    FileInputFormat.setInputPaths(jobConf, new Path(args[0]));

    String tableName = args[1];
    Double writeRatio = null;
    if (args.length >= 3) {
      String val = args[2];
      try {
        writeRatio = Double.parseDouble(val);
      } catch (Exception e) {
        printUsage("Could not parse write ratio (value: " + val + ")");
        return -1;
      }
    }
    setTableProperties(jobConf, tableName, writeRatio);

    Date startTime = new Date();
    System.out.println("Job started: " + startTime);
    JobClient.runJob(jobConf);
    Date endTime = new Date();
    System.out.println("Job ended: " + endTime);
    System.out.println("The job took " + (endTime.getTime() - startTime.getTime()) / 1000 + " "
        + "seconds.");

    return 0;
  }

  private void setTableProperties(JobConf jobConf, String tableName, Double writeRatio) {
    jobConf.set(DynamoDBConstants.OUTPUT_TABLE_NAME, tableName);
    jobConf.set(DynamoDBConstants.INPUT_TABLE_NAME, tableName);
    jobConf.set(DynamoDBConstants.TABLE_NAME, tableName);

    DynamoDBClient client = new DynamoDBClient(jobConf);
    TableDescription description = client.describeTable(tableName);

    if (description.getBillingModeSummary() == null
        || description.getBillingModeSummary().getBillingMode()
        .equals(DynamoDBConstants.BILLING_MODE_PROVISIONED)) {
      jobConf.set(DynamoDBConstants.READ_THROUGHPUT,
          description.getProvisionedThroughput().getReadCapacityUnits().toString());
      jobConf.set(DynamoDBConstants.WRITE_THROUGHPUT,
          description.getProvisionedThroughput().getWriteCapacityUnits().toString());
    } else {
      jobConf.set(DynamoDBConstants.READ_THROUGHPUT,
          DynamoDBConstants.DEFAULT_CAPACITY_FOR_ON_DEMAND.toString());
      jobConf.set(DynamoDBConstants.WRITE_THROUGHPUT,
          DynamoDBConstants.DEFAULT_CAPACITY_FOR_ON_DEMAND.toString());
    }

    log.info("Read throughput:       " + jobConf.get(DynamoDBConstants.READ_THROUGHPUT));
    log.info("Write throughput:      " + jobConf.get(DynamoDBConstants.WRITE_THROUGHPUT));

    // Optional properties
    if (writeRatio != null) {
      jobConf.set(DynamoDBConstants.THROUGHPUT_WRITE_PERCENT, writeRatio.toString());
      log.info("Throughput write ratio: " + writeRatio);
    }
  }

  private void printUsage(String error) {
    if (error != null) {
      System.out.println("Error: " + error);
    }

    System.out.println("Usage: Import <path> <table-name> [<write-ratio>]");
    ToolRunner.printGenericCommandUsage(System.out);
  }
}
