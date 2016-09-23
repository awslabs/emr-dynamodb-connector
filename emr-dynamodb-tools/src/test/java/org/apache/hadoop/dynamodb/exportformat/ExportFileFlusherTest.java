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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import org.apache.hadoop.dynamodb.util.AbstractTimeSource;
import org.apache.hadoop.dynamodb.util.TimeSource;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@RunWith(MockitoJUnitRunner.class)
public class ExportFileFlusherTest {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();
  private AtomicInteger closedFiles;
  private Lock flushLock;
  private AbstractTimeSource time;
  private ExportFileFlusher flusher;
  @Mock
  private RecordWriter<Integer, Integer> recordWriter;

  @Before
  public void setup() {
    closedFiles = new AtomicInteger();
    flushLock = new ReentrantLock();
    time = new TimeSource();
    flusher = new ExportFileFlusher(time);
  }

  @Test
  public void close_whenOneFile_thenFileClosed() throws IOException {
    flusher.close(recordWriter, Reporter.NULL);
    flusher.sync();

    verify(recordWriter).close(Reporter.NULL);
  }

  @Test
  public void close_whenPoolCapacityExceeded_theeAllFilesClosed() throws IOException,
      InterruptedException {

    // Grab the flush lock, this means that only I can flush files.
    flushLock.lock();

    // Enqueue more than the thread pool can handle. This will enqueue
    // pool-size tasks and then run the remainder on this current thread
    // since we're using the CallerRuns policy.
    for (int i = 0; i < ExportFileFlusher.FILE_FLUSHER_POOL_SIZE * 2; i++) {
      RecordWriter<Integer, Integer> rw = mock(RecordWriter.class);

      doAnswer(new Answer() {
        @Override
        public Object answer(InvocationOnMock invocation) throws Throwable {
          flushLock.lock();
          try {
            closedFiles.incrementAndGet();
          } finally {
            flushLock.unlock();
          }
          return null;
        }
      }).when(rw).close(Reporter.NULL);
      flusher.close(rw, Reporter.NULL);
    }

    // Half should have been closed, the other half should still be open.
    Thread.sleep(250); // synchronize by sleep!
    assertEquals(ExportFileFlusher.FILE_FLUSHER_POOL_SIZE, closedFiles.get());

    // Release the flush lock, sync the flusher
    flushLock.unlock();
    flusher.sync();

    assertEquals(ExportFileFlusher.FILE_FLUSHER_POOL_SIZE * 2, closedFiles.get());
  }

  @Test
  public void close_whenIOE_thenConsecutiveCloseCallFails() throws IOException,
      InterruptedException {
    doThrow(new IOException()).when(recordWriter).close(Reporter.NULL);
    flusher.close(recordWriter, Reporter.NULL);
    expectedException.expect(IOException.class);
    verify(recordWriter, timeout(250).times(1)).close(Reporter.NULL);
    flusher.close(mock(RecordWriter.class), Reporter.NULL);
  }

  @Test
  public void close_whenRTE_thenConsecutiveCloseCallFails() throws IOException,
      InterruptedException {
    doThrow(new RuntimeException()).when(recordWriter).close(Reporter.NULL);
    flusher.close(recordWriter, Reporter.NULL);
    expectedException.expect(RuntimeException.class);
    verify(recordWriter, timeout(250).times(1)).close(Reporter.NULL);
    flusher.close(mock(RecordWriter.class), Reporter.NULL);
  }

  @Test
  public void close_whenIOE_thenConsecutiveSyncCallFails() throws IOException,
      InterruptedException {
    doThrow(new IOException()).when(recordWriter).close(Reporter.NULL);
    flusher.close(recordWriter, Reporter.NULL);
    expectedException.expect(IOException.class);
    flusher.sync();
  }

  @Test
  public void close_whenRTE_thenConsecutiveSyncCallFails() throws IOException,
      InterruptedException {
    doThrow(new RuntimeException()).when(recordWriter).close(Reporter.NULL);
    flusher.close(recordWriter, Reporter.NULL);
    expectedException.expect(RuntimeException.class);
    flusher.sync();
  }
}
