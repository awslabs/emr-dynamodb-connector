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

import java.nio.ByteBuffer;
import org.apache.hadoop.dynamodb.type.DynamoDBBinaryType;
import org.apache.hadoop.hive.dynamodb.util.DynamoDBDataParser;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class HiveDynamoDBBinaryType extends DynamoDBBinaryType implements HiveDynamoDBType {

  @Override
  public AttributeValue getDynamoDBData(Object data, ObjectInspector objectInspector,
      boolean nullSerialization) {
    ByteBuffer value = DynamoDBDataParser.getByteBuffer(data, objectInspector);
    return value == null
        ? DynamoDBDataParser.getNullAttribute(nullSerialization)
        : AttributeValue.fromB(SdkBytes.fromByteBuffer(value));
  }

  @Override
  public TypeInfo getSupportedHiveType() {
    return TypeInfoFactory.binaryTypeInfo;
  }

  @Override
  public boolean supportsHiveType(TypeInfo typeInfo) {
    return typeInfo.equals(getSupportedHiveType());
  }

  @Override
  public Object getHiveData(AttributeValue data, ObjectInspector objectInspector) {
    return data.b() == null ? null : data.b().asByteArray();
  }
}
