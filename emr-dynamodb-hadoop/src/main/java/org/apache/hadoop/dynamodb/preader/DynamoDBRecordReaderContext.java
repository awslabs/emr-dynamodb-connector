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

package org.apache.hadoop.dynamodb.preader;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import org.apache.hadoop.dynamodb.DynamoDBClient;
import org.apache.hadoop.dynamodb.split.DynamoDBSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

import java.util.Collection;
import java.util.Map;

public class DynamoDBRecordReaderContext {

  private DynamoDBSplit split;
  private JobConf conf;
  private Reporter reporter;
  private DynamoDBClient client;
  private Collection<String> attributes;
  private double averageItemSize;
  private PageResultMultiplexer<Map<String, AttributeValue>> pageMux;

  public PageResultMultiplexer<Map<String, AttributeValue>> getPageResultMultiplexer() {
    return pageMux;
  }

  public void setPageResultMultiplexer(PageResultMultiplexer<Map<String, AttributeValue>> pageMux) {
    this.pageMux = pageMux;
  }

  public DynamoDBSplit getSplit() {
    return split;
  }

  public void setSplit(InputSplit split) {
    this.split = (DynamoDBSplit) split;
  }

  public double getAverageItemSize() {
    return averageItemSize;
  }

  public void setAverageItemSize(double averageItemSize) {
    this.averageItemSize = averageItemSize;
  }

  public JobConf getConf() {
    return conf;
  }

  public void setConf(JobConf conf) {
    this.conf = conf;
  }

  public Reporter getReporter() {
    return reporter;
  }

  public void setReporter(Reporter reporter) {
    this.reporter = reporter;
  }

  public DynamoDBClient getClient() {
    return client;
  }

  public void setClient(DynamoDBClient client) {
    this.client = client;
  }

  public Collection<String> getAttributes() {
    return attributes;
  }

  public void setAttributes(Collection<String> attributes) {
    this.attributes = attributes;
  }
}
