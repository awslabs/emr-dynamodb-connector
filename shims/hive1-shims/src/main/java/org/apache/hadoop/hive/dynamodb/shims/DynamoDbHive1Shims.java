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

package org.apache.hadoop.hive.dynamodb.shims;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.SerDeException;

import java.util.Properties;

final class DynamoDbHive1Shims implements DynamoDbHiveShims {

  private static final String HIVE_1_VERSION = "1.0.0";

  static boolean supportsVersion(String version) {
    return version.startsWith(HIVE_1_VERSION);
  }

  @Override
  public ExprNodeDesc deserializeExpression(String serializedFilterExpr) {
    return Utilities.deserializeExpression(serializedFilterExpr);
  }

  @Override
  public ExprNodeGenericFuncDesc getIndexExpression(IndexSearchCondition condition) {
    return condition.getComparisonExpr();
  }

  @Override
  public SerDeParametersShim getSerDeParametersShim(Configuration configuration,
      Properties properties, String serDeName) throws SerDeException {
    return new Hive1SerDeParametersShim(configuration, properties, serDeName);
  }

}
