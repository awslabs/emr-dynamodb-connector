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

import com.google.gson.Gson;

import org.apache.hadoop.dynamodb.DynamoDBUtil;

public class ExportFileHeader {

  public final String name = "DynamoDB-export";
  public final int version;

  public ExportFileHeader(int version) {
    this.version = version;
  }

  public String writeStream() {
    Gson gson = DynamoDBUtil.getGson();
    return gson.toJson(this);
  }

  @Override
  public String toString() {
    return writeStream();
  }

}
