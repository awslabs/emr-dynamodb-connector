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

package org.apache.hadoop.dynamodb.filter;

import com.amazonaws.services.dynamodbv2.model.Condition;
import org.apache.hadoop.dynamodb.util.ObjectMapperUtil;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class DynamoDBQueryFilter implements Serializable, Writable {

  private Map<String, Condition> keyConditions = new HashMap<>();
  private Map<String, Condition> scanFilter = new HashMap<>();

  public Map<String, Condition> getKeyConditions() {
    return keyConditions;
  }

  public DynamoDBQueryFilter withKeyCondition(String key, Condition condition) {
    keyConditions.put(key, condition);
    return this;
  }

  public DynamoDBQueryFilter withScanFilter(String key, Condition condition) {
    scanFilter.put(key, condition);
    return this;
  }

  public void addKeyCondition(DynamoDBFilter filter) {
    this.keyConditions.put(filter.getColumnName(), filter.getDynamoDBCondition());
  }

  public Map<String, Condition> getScanFilter() {
    return scanFilter;
  }

  public void addScanFilter(DynamoDBFilter filter) {
    this.scanFilter.put(filter.getColumnName(), filter.getDynamoDBCondition());
  }

  public boolean isQuery() {
    return keyConditions.size() > 0;
  }

  public boolean isScan() {
    return scanFilter.size() > 0;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(keyConditions.size());
    for (Map.Entry<String, Condition> entry : keyConditions.entrySet()) {
      dataOutput.writeUTF(entry.getKey());
      dataOutput.writeUTF(ObjectMapperUtil.createDefaultMapper().writeValueAsString(entry.getValue()));
    }

    dataOutput.writeInt(scanFilter.size());
    for (Map.Entry<String, Condition> entry : scanFilter.entrySet()) {
      dataOutput.writeUTF(entry.getKey());
      dataOutput.writeUTF(ObjectMapperUtil.createDefaultMapper().writeValueAsString(entry.getValue()));
    }
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    keyConditions = readConditions(dataInput);
    scanFilter = readConditions(dataInput);
  }

  public Map<String, Condition> readConditions(DataInput dataInput) throws IOException {
    Map<String, Condition> conditionMap = new HashMap<>();
    int size = dataInput.readInt();
    for (int i = 0; i < size; ++i) {
      String key = dataInput.readUTF();
      String value = dataInput.readUTF();
      Condition condition = ObjectMapperUtil.createDefaultMapper().readValue(value, Condition.class);
      conditionMap.put(key, condition);
    }

    return conditionMap;
  }
}
