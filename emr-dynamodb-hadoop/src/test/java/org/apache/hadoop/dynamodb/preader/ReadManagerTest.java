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

package org.apache.hadoop.dynamodb.preader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.dynamodb.DynamoDBConstants;
import org.apache.hadoop.dynamodb.preader.RateController.RequestLimit;
import org.apache.hadoop.dynamodb.split.DynamoDBSegmentsSplit;
import org.apache.hadoop.dynamodb.split.DynamoDBSplit;
import org.apache.hadoop.dynamodb.util.AbstractTimeSource;
import org.apache.hadoop.dynamodb.util.MockTimeSource;
import org.junit.Before;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.FromDataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

@RunWith(Theories.class)
public class ReadManagerTest {

  @DataPoints("rates")
  public static final double[] rates = {10.0, 50.0, 100.0, 1000.0, 10000.0};
  @DataPoints("sizes")
  public static final double sizes[] = {50, 100, 200, 4000, 4100, 9000, 20000, 40000, 60000};
  @DataPoints("windows")
  public static final int windows[] = {1, 5, 10, 100};
  private static final Random rnd = new Random();
  private DynamoDBRecordReaderContext dummyContext;

  @Before
  public void setup() {
    dummyContext = new DynamoDBRecordReaderContext();
    List<Integer> segments = Collections.singletonList(1);

    DynamoDBSplit splits = new DynamoDBSegmentsSplit(null /* path */, 1 /* approxItemCount */, 1
        /* splitId */, segments, 1 /* totalSegments */, null /* filterPushdown */);
    dummyContext.setSplit(splits);
  }

  @Theory
  public void testHappyCase(@FromDataPoints("rates") double rate, @FromDataPoints("sizes") double
      itemSize, @FromDataPoints("windows") int windowSize) {

    final int DURATION_SEC = 900; // 15 min
    final int ADVANCE_BY_MILLIS = 50;

    MockTimeSource time = new MockTimeSource();
    RateController rateCtr = new RateController(time, rate, windowSize, itemSize);
    MockReadManager mgr = new MockReadManager(rateCtr, time, dummyContext);

    // Consume until we get a zero response
    while (time.getTimeSinceMs(0) <= DURATION_SEC * 1000) {
      mgr.tick();
      time.advanceByMillis(ADVANCE_BY_MILLIS);
    }

    // Look at the last 5 evaluations, their average should be within 10% of
    // the target rate
    final int samples = 5;

    assertTrue("not enough samples", mgr.evalAvgRps.size() >= samples);

    double rateSum = 0;
    for (int i = 0; i < samples; i++) {
      rateSum += mgr.evalAvgRps.pop();
    }

    double rateAvg = rateSum / samples;
    double delta = Math.abs((rate - rateAvg) / rate);

    // Allow 15% fuzz, note that we allow the rate limiter to go 10% over
    assertEquals("target=" + rate + ", actual=" + rateAvg + ", delta=" + delta + " when " +
        "itemSize=" + itemSize + ", windowSize=" + windowSize, 0, delta, 0.15);
  }

  /**
   * Implement a dummy worker, it's states are IDLE, READING, SLEEPING.
   */
  private static class MockReadWorker extends ReadWorker {

    private static final int SLEEP_TIME_MS = 50;
    private static final int SLEEP_FUZZ = 10;

    private static final int READ_TIME_MS = 40;
    private static final int READ_FUZZ = 20;
    private STATE state = STATE.IDLE;
    private long nextActTime = 0;
    private RateController.RequestLimit lastReadLimit;

    public MockReadWorker(AbstractReadManager mgr) {
      super(mgr, null /* reporter */);
      state = STATE.IDLE;
    }

    public void tick(AbstractTimeSource time) {
      if (time.getNanoTime() < nextActTime) {
        return;
      }

      if (!alive && state != STATE.DONE) {
        nextActTime = Long.MAX_VALUE;
        state = STATE.DONE;
        return;
      }

      switch (state) {
        case DONE:
          assertTrue("invalid state", false);
        case READING:
          // report last read's stats
          report();
          // fall through
        case IDLE:
        case SLEEPING:
          break;
        default:
          assertTrue("unhandled state", false);
      }

      // Find the limit for the next request
      RateController.RequestLimit lim = readMgr.rateController.getNextRequestLimit();
      if (lim == RateController.RequestLimit.ZERO) {
        sleep();
        return;
      }

      read(lim);
    }

    private void read(RequestLimit lim) {
      state = STATE.READING;
      nextActTime += (READ_TIME_MS + (int) (READ_FUZZ * rnd.nextDouble())) * MockTimeSource
          .NANOSECONDS_IN_MILLISECOND;
      lastReadLimit = lim;
    }

    /**
     * Didn't get tokens, sleep for some time
     */
    private void sleep() {
      state = STATE.SLEEPING;
      this.nextActTime += (SLEEP_TIME_MS + (int) (SLEEP_FUZZ * rnd.nextDouble())) *
          MockTimeSource.NANOSECONDS_IN_MILLISECOND;
    }

    private void report() {
      int retries = 0;
      int bytes = (int) (lastReadLimit.items * readMgr.rateController.getAvgItemSize());
      double consumedRcu = Math.ceil(bytes / DynamoDBConstants.BYTES_PER_READ_CAPACITY_UNIT /
          DynamoDBConstants.READ_EVENTUALLY_TO_STRONGLY_CONSISTENT_FACTOR);
      readMgr.report(lastReadLimit.readCapacityUnits, consumedRcu, lastReadLimit.items, retries);

    }

    private enum STATE {

      IDLE, READING, SLEEPING, DONE
    }
  }

  private static class MockReadManager extends AbstractReadManager {

    /**
     * Hook into the ReadManager, override worker add/removal and reporting
     */
    private final LinkedList<Double> evalAvgRps = new LinkedList<>();

    public MockReadManager(RateController rateController, AbstractTimeSource time,
        DynamoDBRecordReaderContext context) {
      super(rateController, time, context);
    }

    public void tick() {
      List<ReadWorker> cpy = new ArrayList<>(workers);
      for (ReadWorker w : cpy) {
        MockReadWorker mw = (MockReadWorker) w;
        mw.tick(time);
      }
    }

    @Override
    protected synchronized void addWorker() {
      ReadWorker worker = new MockReadWorker(this);
      // Don't start the thread, just add it to the list
      workers.offer(worker);
    }

    @Override
    protected void recordEvaluationStats(int reportCnt, double rcuPerRequest, double rcuPerSecond) {
      evalAvgRps.push(rcuPerSecond);
      log.info("Evaluating rcuPerRequest=" + rcuPerRequest + ", rcuPerSecond=" + rcuPerSecond +
          ", reportCnt=" + reportCnt + ", workers=" + workers.size());
    }

    @Override
    protected void initializeReadRequests() {
    }
  }
}
