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

package org.apache.hadoop.hive.dynamodb.type;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import org.apache.hadoop.dynamodb.type.DynamoDBBinaryType;
import org.apache.hadoop.hive.dynamodb.util.DynamoDBDataParser;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.nio.ByteBuffer;

public class HiveDynamoDBBinaryType extends DynamoDBBinaryType implements HiveDynamoDBType {

  private final DynamoDBDataParser parser = new DynamoDBDataParser();

  @Override
  public AttributeValue getDynamoDBData(Object data, ObjectInspector objectInspector) {
    ByteBuffer value = parser.getByteBuffer(data, objectInspector);
    return new AttributeValue().withB(value);
  }

  @Override
  public Object getHiveData(AttributeValue data, String hiveType) {
    if (data == null) {
      return null;
    }

    if (data.getB() == null) {
      return null;
    }

    return data.getB().array();
  }
}
