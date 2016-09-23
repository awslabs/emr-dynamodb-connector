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

import com.google.gson.Gson;

import org.apache.hadoop.dynamodb.DynamoDBUtil;

public class ExportManifestEntry {

  public final String url;
  public final boolean mandatory = true;

  public ExportManifestEntry(String url) {
    if (url == null) {
      throw new RuntimeException("Url is required");
    }
    this.url = url;
  }

  public String writeStream() {
    Gson gson = DynamoDBUtil.getGson();
    return gson.toJson(this);
  }

  @Override
  public String toString() {
    return writeStream();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (mandatory ? 1231 : 1237);
    result = prime * result + ((url == null) ? 0 : url.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    ExportManifestEntry other = (ExportManifestEntry) obj;
    if (mandatory != other.mandatory) {
      return false;
    }
    if (url == null) {
      if (other.url != null) {
        return false;
      }
    } else if (!url.equals(other.url)) {
      return false;
    }
    return true;
  }

}
