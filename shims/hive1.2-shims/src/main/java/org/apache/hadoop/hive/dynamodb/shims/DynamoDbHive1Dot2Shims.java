package org.apache.hadoop.hive.dynamodb.shims;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.SerDeException;

import java.util.Properties;

class DynamoDbHive1Dot2Shims implements DynamoDbHiveShims {

  private static final String HIVE_1_2_VERSION = "1.2";
  DynamoDbHiveShims hive1Shims;

  DynamoDbHive1Dot2Shims() {
    this.hive1Shims = new DynamoDbHive1Shims();
  }

  @Override
  public ExprNodeDesc deserializeExpression(String serializedFilterExpr) {
    return hive1Shims.deserializeExpression(serializedFilterExpr);
  }

  @Override
  public ExprNodeGenericFuncDesc getIndexExpression(IndexSearchCondition condition) {
    return condition.getComparisonExpr();
  }

  /**
   * Hive 1.2.x and Hive 2.x share behavior for SerDeParameters class initialization.
   */
  @Override
  public SerDeParametersShim getSerDeParametersShim(Configuration configuration,
      Properties properties, String serDeName) throws SerDeException {
    return new Hive2SerDeParametersShim(configuration, properties, serDeName);
  }

  static boolean supportsVersion(String version) {
    return version.startsWith(HIVE_1_2_VERSION);
  }
}
