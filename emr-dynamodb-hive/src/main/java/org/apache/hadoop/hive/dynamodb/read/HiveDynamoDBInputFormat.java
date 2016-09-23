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

package org.apache.hadoop.hive.dynamodb.read;

import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.dynamodb.DynamoDBClient;
import org.apache.hadoop.dynamodb.DynamoDBConstants;
import org.apache.hadoop.dynamodb.DynamoDBItemWritable;
import org.apache.hadoop.dynamodb.filter.DynamoDBQueryFilter;
import org.apache.hadoop.dynamodb.preader.DynamoDBRecordReaderContext;
import org.apache.hadoop.dynamodb.read.DefaultDynamoDBRecordReader;
import org.apache.hadoop.dynamodb.read.DynamoDBInputFormat;
import org.apache.hadoop.dynamodb.split.DynamoDBSplit;
import org.apache.hadoop.dynamodb.split.DynamoDBSplitGenerator;
import org.apache.hadoop.hive.dynamodb.filter.DynamoDBFilterPushdown;
import org.apache.hadoop.hive.dynamodb.shims.ShimsLoader;
import org.apache.hadoop.hive.dynamodb.split.HiveDynamoDBSplitGenerator;
import org.apache.hadoop.hive.dynamodb.util.HiveDynamoDBUtil;
import org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class HiveDynamoDBInputFormat extends DynamoDBInputFormat {

  private static final Log log = LogFactory.getLog(HiveDynamoDBInputFormat.class);

  /**
   * Instantiates a new predicate analyzer suitable for determining how to push a filter down into
   * the HBase scan, based on the rules for what kinds of pushdown we currently support.
   *
   * @param keyColumnName name of the Hive column mapped to the HBase row key
   * @return preconfigured predicate analyzer
   */
  static IndexPredicateAnalyzer newIndexPredicateAnalyzer(String keyColumnName) {
    IndexPredicateAnalyzer analyzer = new IndexPredicateAnalyzer();

    // for now, we only support equality comparisons
    analyzer.addComparisonOp("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual");

    // and only on the key column
    analyzer.clearAllowedColumnNames();
    analyzer.allowColumnName(keyColumnName);

    return analyzer;
  }

  @Override
  public RecordReader<Text, DynamoDBItemWritable> getRecordReader(InputSplit split, JobConf conf,
      Reporter reporter) throws
      IOException {
    reporter.progress();

    Map<String, String> columnMapping =
        HiveDynamoDBUtil.fromJsonString(conf.get(DynamoDBConstants.DYNAMODB_COLUMN_MAPPING));
    Map<String, String> hiveTypeMapping = HiveDynamoDBUtil.extractHiveTypeMapping(conf);
    DynamoDBQueryFilter queryFilter = getQueryFilter(conf, columnMapping, hiveTypeMapping);
    DynamoDBSplit bbSplit = (DynamoDBSplit) split;
    bbSplit.setDynamoDBFilterPushdown(queryFilter);

    Collection<String> attributes = (columnMapping == null ? null : columnMapping.values());
    DynamoDBRecordReaderContext context = buildHiveDynamoDBRecordReaderContext(bbSplit, conf,
        reporter, attributes);
    return new DefaultDynamoDBRecordReader(context);
  }

  @Override
  protected int getNumSegments(int tableNormalizedReadThroughput, int
      tableNormalizedWriteThroughput, long currentTableSizeBytes, JobConf conf) throws IOException {
    if (isQuery(conf)) {
      log.info("Defaulting to 1 segment because there are key conditions");
      return 1;
    } else {
      return super.getNumSegments(tableNormalizedReadThroughput, tableNormalizedWriteThroughput,
          currentTableSizeBytes, conf);
    }
  }

  @Override
  protected int getNumMappers(int maxClusterMapTasks, int configuredReadThroughput, JobConf conf)
      throws IOException {
    if (isQuery(conf)) {
      log.info("Defaulting to 1 mapper because there are key conditions");
      return 1;
    } else {
      return super.getNumMappers(maxClusterMapTasks, configuredReadThroughput, conf);
    }
  }

  @Override
  protected DynamoDBSplitGenerator getSplitGenerator() {
    return new HiveDynamoDBSplitGenerator();
  }

  private DynamoDBRecordReaderContext buildHiveDynamoDBRecordReaderContext(InputSplit split,
      JobConf conf, Reporter reporter, Collection<String> attributes) {
    DynamoDBRecordReaderContext context = super.buildDynamoDBRecordReaderContext(split, conf,
        reporter);
    context.setAttributes(attributes);
    return context;
  }

  private boolean isQuery(JobConf conf) throws IOException {
    Map<String, String> hiveDynamoDBMapping =
        HiveDynamoDBUtil.fromJsonString(conf.get(DynamoDBConstants.DYNAMODB_COLUMN_MAPPING));
    Map<String, String> hiveTypeMapping = HiveDynamoDBUtil.extractHiveTypeMapping(conf);
    DynamoDBQueryFilter filter = getQueryFilter(conf, hiveDynamoDBMapping, hiveTypeMapping);

    return filter.getKeyConditions().size() >= 1;
  }

  private DynamoDBQueryFilter getQueryFilter(JobConf conf, Map<String, String>
      hiveDynamoDBMapping, Map<String, String> hiveTypeMapping) throws IOException {
    if (hiveDynamoDBMapping == null) {
      /*
       * Column mapping may be null when user has mapped a DynamoDB item
       * onto a single hive map<string, string> column.
       */
      return new DynamoDBQueryFilter();
    }

    DynamoDBClient client = new DynamoDBClient(conf);
    String filterExprSerialized = conf.get(TableScanDesc.FILTER_EXPR_CONF_STR);
    if (filterExprSerialized == null) {
      return new DynamoDBQueryFilter();
    }
    ExprNodeDesc filterExpr =
        ShimsLoader.getHiveShims().deserializeExpression(filterExprSerialized);

    DynamoDBFilterPushdown pushdown = new DynamoDBFilterPushdown();
    List<KeySchemaElement> schema =
        client.describeTable(conf.get(DynamoDBConstants.TABLE_NAME)).getKeySchema();
    DynamoDBQueryFilter queryFilter = pushdown.predicateToDynamoDBFilter(
        schema, hiveDynamoDBMapping, hiveTypeMapping, filterExpr);
    return queryFilter;
  }

}
