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

package org.apache.hadoop.dynamodb.type;

/**
 * We define a new DynamoDB item type to leverage the existing convention for inspecting to map a
 * DynamoDB item to hive and vice versa.
 *
 * Note that most methods in the interface are not useful to us as they were created individual
 * attributes and not a whole DynamoDB item.
 *
 * DynamoDBItemType.
 */
public interface DynamoDBItemType extends DynamoDBType {
}
