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

package org.apache.hadoop.dynamodb.read;

import org.apache.hadoop.dynamodb.preader.PageResultMultiplexer;
import org.apache.hadoop.dynamodb.preader.PageResults;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class PageResultMultiplexerTest {

  public static final int DEFAULT_CAPACITY = 1000;

  /*
   * Make sure basic multiplexing works, pages of even sizes - no blocking.
   */
  @Test(timeout = 2000)
  public void basicMultiplexingTest() throws Exception {
    final int BATCH_SIZE = 5;
    PageResultMultiplexer<Integer> mux = new PageResultMultiplexer<>(BATCH_SIZE, DEFAULT_CAPACITY);

    for (int p = 0; p < BATCH_SIZE; p++) {
      mux.addPageResults(new PageResults<>(Arrays.asList(p, p, p), null));
    }
    mux.setDraining(true);

    Integer[] expectedOrder = {0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4};
    Assert.assertArrayEquals(expectedOrder, muxToList(mux).toArray());
  }

  /*
   * Multiplex over pages of different sizes
   */
  @Test(timeout = 2000)
  public void differentSizedPages() throws Exception {
    final int BATCH_SIZE = 5;
    PageResultMultiplexer<Integer> mux = new PageResultMultiplexer<>(BATCH_SIZE, DEFAULT_CAPACITY);

    mux.addPageResults(new PageResults<>(Arrays.asList(1, 1, 1), null));
    mux.addPageResults(new PageResults<>(Arrays.asList(2), null));
    mux.addPageResults(new PageResults<>(Arrays.asList(3, 3, 3, 3), null));
    mux.addPageResults(new PageResults<>(Arrays.asList(4, 4), null));
    mux.setDraining(true);

    Integer[] expectedOrder = {1, 2, 3, 4, 1, 3, 4, 1, 3, 3};
    Assert.assertArrayEquals(expectedOrder, muxToList(mux).toArray());
  }

  /*
   * Test that next() blocks until it has enough pages in the multiplexer, but then fully drains
   * once in "drain mode".
   */
  @Test(timeout = 2000)
  public void testRequiredBatch() throws InterruptedException {
    final int BATCH_SIZE = 3;

    PageResultMultiplexer<Integer> mux = new PageResultMultiplexer<>(BATCH_SIZE, DEFAULT_CAPACITY);
    MuxConsumer consumer = new MuxConsumer(mux);
    consumer.start();

    mux.addPageResults(new PageResults<>(Arrays.asList(1, 1, 1), null));
    mux.addPageResults(new PageResults<>(Arrays.asList(2, 2), null));

    // Nothing has been consumed because we haven't reached the max batch
    // size yet
    Assert.assertEquals(0, consumer.out.size());

    // Add one more page to make #pages >= BATCH_SIZE to kick off consuming
    // Expect consuming 5 items.
    consumer.jobTrackLatch = new CountDownLatch(5);
    mux.addPageResults(new PageResults<>(Arrays.asList(3, 3, 3), null));
    consumer.jobTrackLatch.await();
    Assert.assertArrayEquals(new Integer[]{1, 2, 3, 1, 2}, consumer.out.toArray());

    // Kick off drain mode. Remaining items will be fetched
    mux.setDraining(true);
    consumer.jobFinishLatch.await();
    Assert.assertArrayEquals(new Integer[]{1, 2, 3, 1, 2, 3, 1, 3}, consumer.out.toArray());
  }

  /*
   * Test that addPageResults() blocks until there is free capacity.
   */
  @Test(timeout = 2000)
  public void testMaximumCapacity() throws InterruptedException, IOException {
    final int BATCH_SIZE = 1;
    final int CAPACITY = 5;
    final int PAGE_COUNT = 8;

    PageResultMultiplexer<Integer> mux = new PageResultMultiplexer<>(BATCH_SIZE, CAPACITY);
    MuxProducer producer = new MuxProducer(mux, PAGE_COUNT);

    // Only CAPACITY pages will be added
    producer.jobTrackLatch = new CountDownLatch(CAPACITY);
    producer.start();
    producer.jobTrackLatch.await();
    Assert.assertEquals(CAPACITY, producer.idx);

    // Remove CAPACITY items from the queue, which effectively removes
    // CAPACITY pages because one page has only one item
    for (int i = 0; i < CAPACITY; i++) {
      Assert.assertNotNull(mux.next());
    }

    // Producer is now able to add (PAGE_COUNT - CAPACITY) items to mux
    producer.jobFinishLatch.await();
    Assert.assertEquals(PAGE_COUNT, producer.idx);

    // Set the mux in drain mode to consume the remaining items
    mux.setDraining(true);
    for (int i = 0; i < PAGE_COUNT - CAPACITY; i++) {
      Assert.assertNotNull(mux.next());
    }
    Assert.assertNull(mux.next());
  }

  @Test(timeout = 100000)
  public void testWithMultipleThreads() throws InterruptedException, IOException {
    final int BATCH_SIZE = 10;
    final int CAPACITY = 1000;
    final int PRODUCERS = 100;
    final int CONSUMERS = 20;

    PageResultMultiplexer<Integer> mux = new PageResultMultiplexer<>(BATCH_SIZE, CAPACITY);
    List<MuxProducer> producers = new ArrayList<>(PRODUCERS);
    List<MuxConsumer> consumers = new ArrayList<>(CONSUMERS);

    // Start consumers
    for (int i = 0; i < CONSUMERS; i++) {
      MuxConsumer t = new MuxConsumer(mux);
      t.start();
      consumers.add(t);
    }

    // Assert that all consumers are alive before starting producers
    for (MuxConsumer consumer : consumers) {
      Assert.assertTrue(consumer.isAlive());
    }

    // Start producers
    for (int i = 0; i < PRODUCERS; i++) {
      MuxProducer t = new MuxProducer(mux, Integer.MAX_VALUE);
      t.start();
      producers.add(t);
    }

    // Let them run a second
    Thread.sleep(1000);

    // Assert that everyone's alive
    for (MuxConsumer consumer : consumers) {
      Assert.assertTrue(consumer.isAlive());
    }
    for (MuxProducer producer : producers) {
      Assert.assertTrue(producer.isAlive());
    }

    // Shut producers down.
    for (MuxProducer producer : producers) {
      producer.alive = false;
      producer.join();
      producer.jobFinishLatch.await();
    }

    // Set the mux in drain mode and wait on the consumers
    mux.setDraining(true);
    for (MuxConsumer consumer : consumers) {
      consumer.join();
      consumer.jobFinishLatch.await();
    }

    // success if not timing out
  }

  /**
   * Helper function, fully consume multiplexer output into a list (blocking).
   */
  private List<Integer> muxToList(PageResultMultiplexer<Integer> mux) throws
      InterruptedException, IOException {
    Integer n;
    List<Integer> list = new ArrayList<>();

    while (true) {
      if ((n = mux.next()) == null) {
        break;
      }
      list.add(n);
    }

    return list;
  }

  /*
   * Helper class, thread that consumes from a multiplexer and appends to a list for inspection.
   */
  private static class MuxConsumer extends Thread {

    private final PageResultMultiplexer<Integer> mux;
    private final CountDownLatch jobFinishLatch = new CountDownLatch(1);
    private final List<Integer> out = new ArrayList<>();

    private CountDownLatch jobTrackLatch;

    public MuxConsumer(PageResultMultiplexer<Integer> mux) {
      this.mux = mux;
    }

    @Override
    public void run() {
      try {
        Integer n;
        while ((n = mux.next()) != null) {
          out.add(n);
          if (jobTrackLatch != null) {
            jobTrackLatch.countDown();
          }
        }
        jobFinishLatch.countDown();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static class MuxProducer extends Thread {

    private final PageResultMultiplexer<Integer> mux;
    private final int cnt;
    private final CountDownLatch jobFinishLatch = new CountDownLatch(1);

    private volatile int idx;
    private volatile boolean alive = true;
    private CountDownLatch jobTrackLatch;

    public MuxProducer(PageResultMultiplexer<Integer> mux, int cnt) {
      this.mux = mux;
      this.cnt = cnt;
    }

    @Override
    public void run() {
      for (idx = 0; idx < cnt && alive; ) {
        mux.addPageResults(new PageResults<>(Arrays.asList(idx), null /* lastEvalKey */));
        idx++;
        if (jobTrackLatch != null) {
          jobTrackLatch.countDown();
        }
      }
      jobFinishLatch.countDown();
    }
  }

}
