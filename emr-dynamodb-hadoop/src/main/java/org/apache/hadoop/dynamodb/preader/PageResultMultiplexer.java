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

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A multiplexer that interleaves items from multiple scan/query page results.
 *
 * The next() method will walk through pages in the buffer and return a single item a time. next()
 * will block if the number of pages is smaller than batchSize.
 */
public class PageResultMultiplexer<V> {

  private static final Log log = LogFactory.getLog(PageResultMultiplexer.class);

  private final int batchSize;
  private final int capacity;
  private final BlockingQueue<PageResults<V>> pages;
  private final AtomicInteger pageCount = new AtomicInteger();
  private final Object removeItemLock = new Object();

  private long itemsReturned = 0;
  private volatile boolean draining = false;
  private volatile Iterator<PageResults<V>> pageIterator;

  public PageResultMultiplexer(int batchSize, int capacity) {
    this.batchSize = batchSize;
    this.capacity = capacity;
    this.pages = new LinkedBlockingQueue<>(capacity);
    this.pageIterator = pages.iterator();
  }

  public boolean addPageResults(PageResults<V> page) {
    // Order: add page, increment counter
    boolean admitted = pages.offer(page);
    if (!admitted) {
      try {
        log.info("Blocking on page add, mux full.");
        pages.put(page);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.error("Page add was interrupted");
        return false;
      }
    }

    pageCount.incrementAndGet();
    log.info("Added a page. Page count: " + pageCount.get());

    return true;
  }

  public V next() throws IOException {
    if (itemsReturned % 10000 == 0) {
      log.info("Pagemux stats: items=" + itemsReturned + ", pages=" + pageCount.get() + ", cap="
          + capacity);
    }

    synchronized (removeItemLock) {
      // Now, this could block for a while as we wait for enough pages to
      // multiplex. As long as we're really doing work in the background (
      // scans/queries), then the dynamodb client/retrier will tick the
      // reporter to signal liveness.
      waitForMuxCondition();

      // Loop until we find the queue in draining mode and empty
      while (!(draining && pageCount.get() == 0)) {
        PageResults<V> nextPage;
        try {
          nextPage = pageIterator.next();
        } catch (NoSuchElementException e) {
          pageIterator = pages.iterator();
          continue;
        }

        if (nextPage.exception != null) {
          throw new IOException(nextPage.exception);
        }

        V nextItem = nextPage.next();

        // Remove the page if we've emptied it
        if (!nextPage.hasMore()) {
          // Order: lower counter, remove page
          pageCount.decrementAndGet();
          pageIterator.remove();
        }

        if (nextItem != null) {
          itemsReturned++;
          return nextItem;
        }
      }
    }

    return null;
  }

  public void setDraining(boolean draining) {
    this.draining = draining;
  }

  private void waitForMuxCondition() {
    while (pageCount.get() < batchSize && !draining) {
      try {
        log.info("Sleeping on consumption condition, pagecount = " + pageCount.get());
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
