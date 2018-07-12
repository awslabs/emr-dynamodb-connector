/**
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file
 * except in compliance with the License. A copy of the License is located at
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "LICENSE.TXT" file accompanying this file. This file is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under the License.
 */

package org.apache.hadoop.dynamodb.type;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import org.apache.hadoop.dynamodb.key.DynamoDBKey;
import org.apache.hadoop.dynamodb.type.DynamoDBType;

public class DynamoDBListType implements DynamoDBType {

  @Override
  public AttributeValue getAttributeValue(String... values) {
    AttributeValue av = new AttributeValue();
    for (String ele : values) {
      av.withL(new AttributeValue(ele));
    }
    return av;
  }

  @Override
  public String getDynamoDBType() {
    return "L";
  }

  @Override
  public DynamoDBKey getKey(String key) {
    throw new RuntimeException("Unexpected type " + getDynamoDBType());
  }

}

