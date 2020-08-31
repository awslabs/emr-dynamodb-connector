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

package org.apache.hadoop.dynamodb.key;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import java.nio.ByteBuffer;
import org.apache.hadoop.dynamodb.DynamoDBUtil;

public class DynamoDBBinaryKey extends AbstractDynamoDBKey {

  private final ByteBuffer byteBuffer;

  public DynamoDBBinaryKey(String base64EncodedKey) {
    super(base64EncodedKey);
    this.byteBuffer = ByteBuffer.wrap(DynamoDBUtil.base64DecodeString(base64EncodedKey));
  }

  @Override
  public int compareValue(AttributeValue attribute) {
    return byteBuffer.compareTo(attribute.getB());
  }
}
