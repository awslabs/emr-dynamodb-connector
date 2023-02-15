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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.dynamodb.type.DynamoDBBinarySetType;
import org.apache.hadoop.hive.dynamodb.util.DynamoDBDataParser;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class HiveDynamoDBBinarySetType extends DynamoDBBinarySetType implements HiveDynamoDBType {

  @Override
  public AttributeValue getDynamoDBData(Object data, ObjectInspector objectInspector,
                                        boolean nullSerialization) {
    List<ByteBuffer> values =
        DynamoDBDataParser.getByteBuffers(data, objectInspector, getDynamoDBType());
    return (values == null || values.isEmpty())
        ? DynamoDBDataParser.getNullAttribute(nullSerialization)
        : AttributeValue.fromBs(values.stream()
        .map(byteBuffer -> SdkBytes.fromByteBuffer(byteBuffer))
        .collect(Collectors.toList()));
  }

  @Override
  public TypeInfo getSupportedHiveType() {
    return TypeInfoFactory.getListTypeInfo(TypeInfoFactory.binaryTypeInfo);
  }

  @Override
  public boolean supportsHiveType(TypeInfo typeInfo) {
    return typeInfo.equals(getSupportedHiveType());
  }

  @Override
  public Object getHiveData(AttributeValue data, ObjectInspector objectInspector) {
    List<SdkBytes> sdkBytesList = data.bs();

    if (sdkBytesList == null || sdkBytesList.isEmpty()) {
      return null;
    }

    List<byte[]> byteArrays = new ArrayList<>(sdkBytesList.size());
    for (SdkBytes sdkBytes : sdkBytesList) {
      byteArrays.add(Arrays.copyOf(sdkBytes.asByteArray(), sdkBytes.asByteArray().length));
    }

    return byteArrays;
  }

}
