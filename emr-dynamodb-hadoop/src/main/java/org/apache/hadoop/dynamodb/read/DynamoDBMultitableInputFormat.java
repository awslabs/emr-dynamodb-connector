package org.apache.hadoop.dynamodb.read;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.dynamodb.DynamoDBConstants;
import org.apache.hadoop.dynamodb.DynamoDBItemWritable;
import org.apache.hadoop.dynamodb.filter.DynamoDBQueryFilter;
import org.apache.hadoop.dynamodb.preader.DynamoDBRecordReaderContext;
import org.apache.hadoop.dynamodb.util.SerializeUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by huy on 2017. 1. 23..
 */
public class DynamoDBMultitableInputFormat extends AbstractDynamoDBInputFormat<Text, DynamoDBItemWritable> {
  private static final Log log = LogFactory.getLog(DynamoDBMultitableInputFormat.class);

  @Override
  public RecordReader<Text, DynamoDBItemWritable> getRecordReader(InputSplit split, JobConf conf,
                                                                  Reporter reporter) throws IOException {
    DynamoDBRecordReaderContext context = buildDynamoDBRecordReaderContext(split, conf, reporter);
    return new DefaultDynamoDBRecordReader(context);
  }

  @Override
  protected int getNumSegments(int tableNormalizedReadThroughput, int
      tableNormalizedWriteThroughput, long currentTableSizeBytes, JobConf conf) throws IOException {
    return getFilterList(conf).size();
  }

  @Override
  protected int getNumMappers(int maxClusterMapTasks, int configuredReadThroughput, JobConf conf)
      throws IOException {
    return getFilterList(conf).size();
  }

  @Override
  public InputSplit[] getSplits(JobConf conf, int desiredSplits) throws IOException {
    int numSegments = getNumSegments(0, 0, 0, conf);
    int numMappers = getNumMappers(0, 0, conf);
    log.info("Using " + numSegments + " segments across " + numMappers + " mappers");

    return getSplitGenerator().generateSplits(numMappers, numSegments, conf, new ArrayList<>(getFilterList(conf)));
  }

  protected List<DynamoDBQueryFilter> getFilterList(JobConf conf) {

    try {
      List<DynamoDBQueryFilter> filterList = (List) SerializeUtil.fromString(conf.get(DynamoDBConstants.DYNAMODB_MULTIPLE_QUERY));
      if (filterList.isEmpty()) {
        throw new IllegalArgumentException(this.getClass().getCanonicalName() + " must need to 1 more filters.");
      }
      return filterList;
    } catch (ClassNotFoundException | IOException e) {
      log.error(e);
      throw new IllegalArgumentException(this.getClass().getCanonicalName() + " can't load multi table filters.");
    }
  }
}
