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

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.gson.Gson;

import org.apache.hadoop.dynamodb.DynamoDBUtil;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class ExportManifestEntryTest {
  private String url;
  private ExportManifestEntry entry;
  private Gson gson;

  @Before
  public void setup() {
    url = "s3://my-bucket/is-half-full";
    entry = new ExportManifestEntry(url);
    gson = DynamoDBUtil.getGson();
  }


  @Test
  public void testHappyCase() throws IOException {
    String json = gson.toJson(entry, ExportManifestEntry.class);

    // Assert that we have our basic fields there
    assertThat(json, containsString("url"));
    assertThat(json, containsString("mandatory"));
    assertThat(json, containsString(url));

    final ExportManifestEntry deserialized = gson.fromJson(json, ExportManifestEntry.class);
    assertEquals(entry, deserialized);
  }

  /*
   * Make sure we don't blow up reading a future entry with fields we don't know about
   */
  @Test
  public void testExtraFieldsInSerializedFormat() throws IOException {
    final String JSON = "{\"url\":\"" + url + "\",\"mandatory\":true,\"extra\":true}";
    final ExportManifestEntry deserialized = gson.fromJson(JSON, ExportManifestEntry.class);
    assertEquals(entry, deserialized);
  }


  /*
   * Should read a missing mandatory attribute as true.
   *
   * Unfortunately there isn't a way to define a required attribute in GSON,
   * so we'll have to do our own verification on url, for example (no tests
   * for that here).
   */
  @Test
  public void testMissingMandatory() throws IOException {
    final String JSON = "{\"url\":\"" + url + "\"}";
    final ExportManifestEntry deserialized = gson.fromJson(JSON, ExportManifestEntry.class);

    assertEquals(entry, deserialized);
    assertTrue(deserialized.mandatory);
  }

}
