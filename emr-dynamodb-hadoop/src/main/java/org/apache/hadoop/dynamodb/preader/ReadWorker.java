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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.dynamodb.preader.RateController.RequestLimit;
import org.apache.hadoop.mapred.Reporter;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class ReadWorker extends Thread {

  private static final int SLEEP_TIME_MS = 80;
  private static final int SLEEP_JITTER_MS = 40;

  private static final Log log = LogFactory.getLog(ReadWorker.class);
  private static final AtomicInteger workerId = new AtomicInteger();
  protected final AbstractReadManager readMgr;
  private final Reporter reporter;
  private final Random rnd = new Random();
  protected volatile boolean alive = true;

  public ReadWorker(AbstractReadManager mgr, Reporter reporter) {
    super("ReadWorker-" + workerId.incrementAndGet());
    this.readMgr = mgr;
    this.reporter = reporter;
  }

  @Override
  public void run() {
    while (alive) {
      try {
        runInternal();
      } catch (InterruptedException e) {
        Thread.interrupted();
      } catch (Throwable e) {
        log.error("Unknown exception thrown!", e);
      }
    }

    log.info("Worker shutting down, no longer alive");
  }

  public void setAlive(boolean alive) {
    this.alive = alive;
  }

  private void runInternal() throws Throwable {
    // We're alive, even though we might not get tokens or find work below
    if (reporter != null) {
      reporter.progress();
    }

    AbstractRecordReadRequest req = readMgr.dequeueReadRequest();
    if (req == null) {
      log.info("Worker found read request queue empty, sleeping.");
      Thread.sleep(getSleepTime());
      return;
    }

    // Find the limit for the next request
    RequestLimit lim = readMgr.rateController.getNextRequestLimit();
    if (lim == RateController.RequestLimit.ZERO) {
      log.info("No read token from rate controller. Putting the request back");
      readMgr.enqueueReadRequestToHead(req);
      Thread.sleep(getSleepTime());
      return;
    }

    // Trigger the read request
    req.read(lim);
  }

  private long getSleepTime() {
    int base = SLEEP_TIME_MS - SLEEP_JITTER_MS;
    int jitter = rnd.nextInt(SLEEP_JITTER_MS * 2);
    return base + jitter;
  }

}
