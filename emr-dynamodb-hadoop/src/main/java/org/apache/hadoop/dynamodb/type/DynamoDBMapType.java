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

import org.apache.hadoop.dynamodb.key.DynamoDBKey;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class DynamoDBMapType implements DynamoDBType {

  @Override
  public AttributeValue getAttributeValue(String... values) {
    System.out.println("values:");
    System.out.println(values);
    System.out.println("end values");
    return AttributeValue.builder().build();
  }

  @Override
  public String getDynamoDBType() {
    return DynamoDBTypeConstants.MAP;
  }

  @Override
  public DynamoDBKey getKey(String key) {
    throw new RuntimeException("Unexpected type " + getDynamoDBType());
  }

}
