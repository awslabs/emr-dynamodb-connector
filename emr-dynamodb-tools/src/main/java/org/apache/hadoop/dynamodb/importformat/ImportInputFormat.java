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

package org.apache.hadoop.dynamodb.importformat;

import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.dynamodb.DynamoDBConstants;
import org.apache.hadoop.dynamodb.DynamoDBItemWritable;
import org.apache.hadoop.dynamodb.DynamoDBUtil;
import org.apache.hadoop.dynamodb.exportformat.ExportManifestEntry;
import org.apache.hadoop.dynamodb.exportformat.ExportManifestOutputFormat;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.apache.hadoop.mapreduce.security.TokenCache;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ImportInputFormat extends FileInputFormat<NullWritable, DynamoDBItemWritable> {

  private static final Log log = LogFactory.getLog(ImportInputFormat.class);

  // This is based on our observation that EMR can run 100,000 map tasks
  // without melting down. This is not a hard limit.
  private static final int MAX_NUM_SPLITS = 100000;

  private static final String VERSION_JSON_KEY = "version";
  private static final String ENTRIES_JSON_KEY = "entries";

  /**
   * {@inheritDoc}
   *
   * More exactly, the returned array type is CombineFileSplit[]. See { {@link
   * #readEntries(JsonReader, JobConf)}.
   */
  @Override
  public InputSplit[] getSplits(JobConf job, int numSplitsHint) throws IOException {
    List<InputSplit> splits = getSplitsFromManifest(job);
    if (splits == null) {
      /*
       * In the case of no manifest file, we fall back to the built-in
       * FileInputFormat.getSplits() to generate one split for each S3
       * file. Note that our record reader doesn't support byte offsets
       * into S3 files, so we need to override isSplitable(FileSystem,
       * Path) to always return false.
       */
      return super.getSplits(job, numSplitsHint);
    }

    log.info("The actual number of generated splits: " + splits.size());

    return splits.toArray(new InputSplit[splits.size()]);
  }

  @Override
  public RecordReader<NullWritable, DynamoDBItemWritable> getRecordReader(InputSplit genericSplit,
      JobConf job, Reporter reporter) throws IOException {
    return ImportRecordReaderFactory.getRecordReader(genericSplit, job, reporter);
  }

  @Override
  protected boolean isSplitable(FileSystem fs, Path filename) {
    return false;
  }

  private List<InputSplit> getSplitsFromManifest(JobConf job) throws IOException {
    Path[] dirs = getInputPaths(job);
    if (dirs.length == 0) {
      throw new IOException("No input path specified in job");
    } else if (dirs.length > 1) {
      throw new IOException("Will only look for manifests in a single input directory (" + dirs
          .length + " directories provided).");
    }
    TokenCache.obtainTokensForNamenodes(job.getCredentials(), dirs, job);

    Path dir = dirs[0];

    FileSystem fs = dir.getFileSystem(job);
    if (!fs.getFileStatus(dir).isDirectory()) {
      throw new IOException("Input path not a directory: " + dir);
    }

    Path manifestPath = new Path(dir, ExportManifestOutputFormat.MANIFEST_FILENAME);
    if (!fs.isFile(manifestPath)) {
      return null;
    }

    return parseManifest(fs, manifestPath, job);
  }

  // @formatter:off

  /**
   * An example manifest file looks like
   *
   * {"name":"DynamoDB-export","version":3, "entries":[
   * {"url":"s3://path/to/object/92dd1414-a049-4c68-88fb-a23acd44907e","mandatory":true},
   * {"url":"s3://path/to/object/ba3f3535-7aa1-4f97-a530-e72938bf4b76","mandatory":true} ]}
   */
  // @formatter:on
  private List<InputSplit> parseManifest(FileSystem fs, Path manifestPath, JobConf job) throws
      IOException {
    List<InputSplit> splits = null;

    FSDataInputStream fp = fs.open(manifestPath);
    JsonReader reader = new JsonReader(new InputStreamReader(fp, Charsets.UTF_8));

    reader.beginObject();
    while (reader.hasNext()) {
      String name = reader.nextName();
      switch (name) {
        case VERSION_JSON_KEY:
          job.set(DynamoDBConstants.EXPORT_FORMAT_VERSION, String.valueOf(reader.nextInt()));
          break;
        case ENTRIES_JSON_KEY:
          splits = readEntries(reader, job);
          break;
        default:
          log.info("Skipping a JSON key in the manifest file: " + name);
          reader.skipValue();
          break;
      }
    }
    reader.endObject();

    if (splits == null) {
      return Collections.emptyList();
    }
    return splits;
  }

  /**
   * This method retrieves the URLs of all S3 files and generates input splits by combining
   * multiple S3 URLs into one split.
   *
   * @return a list of input splits. The length of this list may not be exactly the same as
   * <code>numSplits</code>. For example, if numSplits is larger than MAX_NUM_SPLITS or the number
   * of S3 files, then numSplits is ignored. Furthermore, not all input splits contain the same
   * number of S3 files. For example, with five S3 files {s1, s2, s3, s4, s5} and numSplits = 3,
   * this method returns a list of three input splits: {s1, s2}, {s3, s4} and {s5}.
   */
  private List<InputSplit> readEntries(JsonReader reader, JobConf job) throws IOException {
    List<Path> paths = new ArrayList<Path>();
    Gson gson = DynamoDBUtil.getGson();

    reader.beginArray();
    while (reader.hasNext()) {
      ExportManifestEntry entry = gson.fromJson(reader, ExportManifestEntry.class);
      paths.add(new Path(entry.url));
    }
    reader.endArray();
    log.info("Number of S3 files: " + paths.size());

    if (paths.size() == 0) {
      return Collections.emptyList();
    }

    int filesPerSplit = (int) Math.ceil((double) (paths.size()) / Math.min(MAX_NUM_SPLITS, paths
        .size()));
    int numSplits = (int) Math.ceil((double) (paths.size()) / filesPerSplit);

    long[] fileMaxLengths = new long[filesPerSplit];
    Arrays.fill(fileMaxLengths, Long.MAX_VALUE / filesPerSplit);

    long[] fileStarts = new long[filesPerSplit];
    Arrays.fill(fileStarts, 0);

    List<InputSplit> splits = new ArrayList<InputSplit>(numSplits);
    for (int i = 0; i < numSplits; i++) {
      int start = filesPerSplit * i;
      int end = filesPerSplit * (i + 1);
      if (i == (numSplits - 1)) {
        end = paths.size();
      }
      Path[] pathsInOneSplit = paths.subList(start, end).toArray(new Path[end - start]);
      CombineFileSplit combineFileSplit = new CombineFileSplit(job, pathsInOneSplit, fileStarts,
          fileMaxLengths, new String[0]);
      splits.add(combineFileSplit);
    }

    return splits;
  }

}
