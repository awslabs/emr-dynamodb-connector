package org.apache.hadoop.dynamodb.read;

import org.apache.hadoop.dynamodb.DynamoDBConstants;
import org.apache.hadoop.dynamodb.split.DynamoDBSplit;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class DynamoDBInputFormatTest {

  @Test
  public void testProvisionedTable() throws IOException {
    JobConf conf = makeJobConf(
        234 * 1024L * 1024L, // 234 MB
        Optional.of(25), // 25 provisioned RCUs
        Optional.empty() // No configured scan segments
    );
    checkSplits(conf, 2);
  }

  @Test
  public void testProvisionedTableLowRCU() throws IOException {
    JobConf conf = makeJobConf(
        50 * 1024L * 1024L * 1024L, // 50 GB
        Optional.of(1), // 1 provisioned RCU
        Optional.empty() // No configured scan segments
    );
    checkSplits(conf, 20); // Number of partitions is low because of the table low RCUs
  }

  @Test
  public void testConfiguredScanSegments() throws IOException {
    JobConf conf = makeJobConf(
        234 * 1024L * 1024L, // 234 MB
        Optional.of(25), // 25 provisioned RCUs
        Optional.of(5)   // Explicit configuration for total scan segments
    );
    checkSplits(conf, 5);
  }

  @Test
  public void testOnDemandTable() throws IOException {
    JobConf conf = makeJobConf(
        234 * 1024L * 1024L, // 234 MB
        Optional.empty(), // On-demand billing mode
        Optional.empty()  // No configured scan segments
    );
    checkSplits(conf, 2);
  }

  private JobConf makeJobConf(
      long tableSizeBytes,
      Optional<Integer> provisionedReadCapacityUnits,
      Optional<Integer> configuredScanSegments
  ) {
    JobConf conf = new JobConf();
    // Set table size
    conf.set(DynamoDBConstants.TABLE_SIZE_BYTES, Long.toString(tableSizeBytes));
    // Set read throughput
    provisionedReadCapacityUnits.ifPresent(rcu ->
        conf.set(
            DynamoDBConstants.READ_THROUGHPUT,
            Long.toString((long)(rcu * DynamoDBConstants.BYTES_PER_READ_CAPACITY_UNIT))
        )
    );
    if (!provisionedReadCapacityUnits.isPresent()) {
      conf.set(
          DynamoDBConstants.READ_THROUGHPUT,
          Long.toString((long)(DynamoDBConstants.DEFAULT_CAPACITY_FOR_ON_DEMAND * DynamoDBConstants.BYTES_PER_READ_CAPACITY_UNIT))
      );
    }
    // Scan segments
    configuredScanSegments.ifPresent(scanSegments ->
        conf.set(DynamoDBConstants.SCAN_SEGMENTS, scanSegments.toString())
    );
    return conf;
  }

  private void checkSplits(JobConf conf, int expectedSplitNumber) throws IOException {
    DynamoDBInputFormat inputFormat = new DynamoDBInputFormat();
    List<DynamoDBSplit> splits =
        Arrays.stream(inputFormat.getSplits(conf, 1))
            .map(split -> (DynamoDBSplit) split)
            .collect(Collectors.toList());

    assertEquals(expectedSplitNumber, splits.size());
    // By default, each partition is assigned exactly one scan segment
    splits.forEach(split -> assertEquals(1, split.getSegments().size()));
  }

}
