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

package org.apache.hadoop.dynamodb.exportformat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.dynamodb.util.AbstractTimeSource;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * This class is responsible for asynchronously closing export file record writers, flushing the
 * files to disk/s3/etc. If an exception is thrown while closing a file, the exception is caught and
 * thrown in the subsequent close or sync call.
 */
public class ExportFileFlusher {

  static final int FILE_FLUSHER_POOL_SIZE = 5;

  private static final Log log = LogFactory.getLog(ExportFileFlusher.class);
  private final ExecutorService closePool = new ThreadPoolExecutor(FILE_FLUSHER_POOL_SIZE,
      FILE_FLUSHER_POOL_SIZE, 1L, TimeUnit.MINUTES, new SynchronousQueue<Runnable>(), new
      ThreadPoolExecutor.CallerRunsPolicy());
  private final AbstractTimeSource time;
  private volatile Throwable exception = null;

  public ExportFileFlusher(AbstractTimeSource time) {
    this.time = time;
  }

  @SuppressWarnings("rawtypes")
  public void close(final RecordWriter recordWriter, final Reporter reporter) throws IOException {
    throwCaughtException();

    closePool.execute(new Runnable() {
      @Override
      public void run() {
        try {
          long start = time.getNanoTime();
          recordWriter.close(reporter);
          long duration = time.getTimeSinceMs(start);
          log.info("Flushed file in " + (duration / 1000.0) + " seconds.");
        } catch (Throwable e) {
          log.error("Exeption caught while closing stream. This exception will be thrown later.",
              e);
          exception = e;
        }

      }
    });
  }

  public void sync() throws IOException {
    log.info("Waiting for all output files to properly close.");
    closePool.shutdown();

    try {
      closePool.awaitTermination(30, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      log.info("Thread interrupted while awaiting pool termination");
    }

    if (!closePool.isTerminated()) {
      log.fatal("Could not properly drain file closes");
      throw new RuntimeException("Could not properly drain file closes");
    }

    throwCaughtException();
  }

  private void throwCaughtException() throws IOException {
    if (exception != null) {
      if (exception instanceof IOException) {
        throw (IOException) exception;
      } else {
        throw new RuntimeException("Uncaught exception while closing previous stream", exception);
      }
    }
  }
}
