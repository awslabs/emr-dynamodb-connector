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

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.dynamodb.DynamoDBConstants;
import org.apache.hadoop.dynamodb.util.AbstractTimeSource;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The ReadManager is responsible for deciding the required number of ReadWorkers to achieve the
 * target throughput rate. It will keep track of RCUs achieved and increase or decrease the worker
 * count as necessary.
 */
public abstract class AbstractReadManager {

  protected static final Log log = LogFactory.getLog(AbstractReadManager.class);

  // This defines the lower bound of what we try to stay within. Not the same
  // as RateController.MIN_RCU_PER_REQ which defines the absolute smallest
  // request we are willing to make.
  private static final int MIN_RCU_PER_REQ = 2;

  private static final int MIN_WORKER_COUNT = 1;
  private static final int MAX_WORKER_COUNT = 30;
  private static final int INITIAL_WORKER_COUNT = MIN_WORKER_COUNT;
  private static final int EVALUATION_FREQ_MS = DynamoDBConstants.RATE_CONTROLLER_WINDOW_SIZE_SEC
      * 1000;
  protected final DynamoDBRecordReaderContext context;
  protected final RateController rateController;
  protected final AbstractTimeSource time;
  // In the query case, there is only one read quest.
  protected final Deque<AbstractRecordReadRequest> readRequestQueue = new ConcurrentLinkedDeque<>();
  protected final AtomicInteger segmentsRemaining = new AtomicInteger(0);
  protected final Queue<ReadWorker> workers = new ArrayBlockingQueue<>(MAX_WORKER_COUNT);
  private final List<Report> reportedStats = new ArrayList<>();
  private final Object reportStatsLock = new Object();
  private final PageResultMultiplexer<Map<String, AttributeValue>> pageMux;
  private long lastEvaluatedTimeNano;

  public AbstractReadManager(RateController rateController, AbstractTimeSource time,
      DynamoDBRecordReaderContext context) {
    this.context = context;
    this.rateController = rateController;
    this.time = time;
    this.lastEvaluatedTimeNano = time.getNanoTime();
    this.pageMux = context.getPageResultMultiplexer();

    initializeReadRequests();

    for (int i = 0; i < INITIAL_WORKER_COUNT; i++) {
      addWorker();
    }
  }

  public void enqueueReadRequestToTail(AbstractRecordReadRequest req) {
    readRequestQueue.addLast(req);
  }

  public void enqueueReadRequestToHead(AbstractRecordReadRequest req) {
    readRequestQueue.addFirst(req);
  }

  public AbstractRecordReadRequest dequeueReadRequest() {
    return readRequestQueue.poll();
  }

  /**
   * @param permittedReadUnits How many RCU the worker was allocated by the rate controller
   * @param consumedReadUnits  How many RCU the worker actually consumed
   * @param items              Items returned
   * @param retries            Number of throttles
   */
  public void report(double permittedReadUnits, double consumedReadUnits, int items, int retries) {
    rateController.adjust(permittedReadUnits, consumedReadUnits, items);

    boolean addWorker = false;
    boolean removeWorker = false;

    synchronized (reportStatsLock) {
      reportedStats.add(new Report(consumedReadUnits, items, retries));

      long deltaMs = time.getTimeSinceMs(lastEvaluatedTimeNano);
      if (deltaMs < EVALUATION_FREQ_MS) {
        return;
      }
      int reportCount = reportedStats.size();
      if (reportCount == 0) {
        return;
      }

      // Compute statistics
      Report sum = getReportedSum();
      double rcuPerRequest = sum.readUnits / reportCount;
      double rcuPerSecond = (sum.readUnits * 1000) / deltaMs;
      recordEvaluationStats(reportCount, rcuPerRequest, rcuPerSecond);

      // Remove a worker if we're achieving our throughput with very low
      // iops requests. There's benefit in doing slightly larger requests.
      if (rcuPerRequest < MIN_RCU_PER_REQ && rcuPerSecond * 1.1 > rateController.getTargetRate()) {
        removeWorker = true;
      } else if (rcuPerSecond * 1.1 <= rateController.getTargetRate()) {
        if (sum.retries > 0) {
          log.warn("Not achieving throughput, but not adding workers due to retries (throttles or"
              + " 500s) (cnt=" + sum.retries + ")");
          // Add a worker if we're not achieving our throughput and getting no throttles/retries.
        } else {
          addWorker = true;
        }
      }
      reportedStats.clear();
      lastEvaluatedTimeNano = time.getNanoTime();
    }

    if (removeWorker) {
      log.info("Removing a worker");
      removeWorker();
    } else if (addWorker) {
      log.info("Adding a worker");
      addWorker();
    }
  }

  /**
   * Called by read worker/read request once a segment has been fully read.
   *
   * @param segment the segment to be marked as completed
   */
  void markSegmentComplete(int segment) {
    int remaining = segmentsRemaining.decrementAndGet();

    log.info("Segment " + segment + " complete. Remaining segments: " + remaining);

    if (remaining == 0) {
      // signal the multiplexer that it should start draining pages.
      pageMux.setDraining(true);

      // signal read manager that we're done
      shutdown();
    }
  }

  /**
   * Signal workers to stop work. Note that this does not block and wait for them to complete. If
   * they have pending work they may still push work to the multiplexer.
   */
  public void shutdown() {
    if (segmentsRemaining.get() > 0) {
      log.warn("Shutting down ReadManager while there are segments remaining.");
    } else {
      log.info("Shutting down record reader, no segments remaining.");
    }

    while (workers.size() > 0) {
      removeWorker(true /* force */);
    }
  }

  protected abstract void initializeReadRequests();

  protected void recordEvaluationStats(int reportCnt, double rcuPerRequest, double rcuPerSecond) {
    log.info("Evaluating rcuPerRequest=" + rcuPerRequest + ", rcuPerSecond=" + rcuPerSecond + ", "
        + "reportCnt=" + reportCnt + ", workers=" + workers.size());
  }

  protected void addWorker() {
    ReadWorker worker = new ReadWorker(this, context.getReporter());
    if (workers.offer(worker)) {
      worker.start();
    } else {
      log.info("Can't increase worker count, already at max worker count");
    }
  }

  protected void removeWorker() {
    removeWorker(false /* force */);
  }

  private void removeWorker(boolean force) {
    if (!force && workers.size() <= MIN_WORKER_COUNT) {
      log.info("Can't reduce worker count, already at min worker count");
    } else {
      ReadWorker worker = workers.poll();
      if (worker != null) {
        worker.setAlive(false);
      }
    }
  }

  private Report getReportedSum() {
    double readUnits = 0;
    int items = 0;
    int retries = 0;

    for (Report r : reportedStats) {
      readUnits += r.readUnits;
      items += r.items;
      retries += r.retries;
    }
    return new Report(readUnits, items, retries);
  }

  private static class Report {

    public final double readUnits;
    public final int items;
    public final int retries;

    public Report(double readUnits, int items, int retries) {
      this.readUnits = readUnits;
      this.items = items;
      this.retries = retries;
    }
  }

}
