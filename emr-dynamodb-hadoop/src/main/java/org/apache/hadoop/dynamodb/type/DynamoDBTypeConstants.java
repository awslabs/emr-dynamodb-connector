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

public final class DynamoDBTypeConstants {
  public static final String BINARY_SET = "BS";
  public static final String BINARY = "B";
  public static final String BOOLEAN = "BOOL";
  public static final String ITEM = "ITEM";
  public static final String LIST = "L";
  public static final String MAP = "M";
  public static final String NUMBER_SET = "NS";
  public static final String NUMBER = "N";
  public static final String STRING_SET = "SS";
  public static final String STRING = "S";

  private DynamoDBTypeConstants() {
  }
}
