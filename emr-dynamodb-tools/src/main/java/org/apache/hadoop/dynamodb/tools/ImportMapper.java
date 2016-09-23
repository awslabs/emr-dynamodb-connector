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

import org.apache.hadoop.dynamodb.DynamoDBItemWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class ImportMapper extends MapReduceBase implements Mapper<NullWritable,
    DynamoDBItemWritable, Text, DynamoDBItemWritable> {

  private static final Text outputKey = new Text();

  @Override
  public void map(NullWritable key, DynamoDBItemWritable value, OutputCollector<Text,
      DynamoDBItemWritable> output, Reporter reporter) throws IOException {
    output.collect(outputKey, value);
    reporter.incrCounter(Counters.DYNAMODB_ITEMS_READ_FROM_BACKUP, 1);
  }

  private enum Counters {

    DYNAMODB_ITEMS_READ_FROM_BACKUP,
  }
}
