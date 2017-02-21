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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.dynamodb.DynamoDBItemWritable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.File;
import java.io.IOException;

@RunWith(MockitoJUnitRunner.class)
public class ExportOutputFormatTest {

  @Rule
  public TemporaryFolder tempDir = new TemporaryFolder();
  @Mock
  private FileSystem mockFileSystem;
  @Mock
  private Progressable mockProgressable;
  private String EXPECTED_FILENAME = "foo";
  private JobConf conf = new JobConf();

  @Before
  public void setup() {
    conf.set("mapred.output.dir", tempDir.getRoot().getPath());
  }

  @Test
  public void testGetRecordWriter() throws IOException {
    ExportOutputFormat outputFormat = new ExportOutputFormat();

    RecordWriter<NullWritable, DynamoDBItemWritable> recordWriter
        = outputFormat.getRecordWriter(mockFileSystem, conf, EXPECTED_FILENAME, mockProgressable);

    assertNotNull(recordWriter);
    String expectedFilePath =
        tempDir.getRoot().getAbsolutePath() + Path.SEPARATOR + EXPECTED_FILENAME;
    assertTrue(new File(expectedFilePath).exists());
  }

  @Test
  public void testGetRecordWriterWithCompression() throws IOException {
    ExportOutputFormat.setCompressOutput(conf, true);
    ExportOutputFormat.setOutputCompressorClass(conf, GzipCodec.class);

    ExportOutputFormat outputFormat = new ExportOutputFormat();

    RecordWriter<NullWritable, DynamoDBItemWritable> recordWriter
        = outputFormat.getRecordWriter(mockFileSystem, conf, EXPECTED_FILENAME, mockProgressable);

    assertNotNull(recordWriter);
    String expectedFilePath =
        tempDir.getRoot().getAbsolutePath() + Path.SEPARATOR + EXPECTED_FILENAME + ".gz";
    assertTrue(new File(expectedFilePath).exists());
  }
}
