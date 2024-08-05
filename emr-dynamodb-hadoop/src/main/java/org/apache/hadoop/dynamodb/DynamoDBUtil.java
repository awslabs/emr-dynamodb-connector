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

package org.apache.hadoop.dynamodb;

import static org.apache.hadoop.dynamodb.DynamoDBConstants.DEFAULT_MAX_ITEMS_PER_BATCH;
import static org.apache.hadoop.dynamodb.DynamoDBConstants.DEFAULT_SEGMENT_SPLIT_SIZE;
import static org.apache.hadoop.dynamodb.DynamoDBConstants.MAX_ITEMS_PER_BATCH;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.dynamodb.util.ClusterTopologyNodeCapacityProvider;
import org.apache.hadoop.dynamodb.util.DynamoDBReflectionUtils;
import org.apache.hadoop.dynamodb.util.NodeCapacityProvider;
import org.apache.hadoop.dynamodb.util.RoundRobinYarnContainerAllocator;
import org.apache.hadoop.dynamodb.util.TaskCalculator;
import org.apache.hadoop.dynamodb.util.YarnContainerAllocator;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.internal.util.EC2MetadataUtils;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

public final class DynamoDBUtil {

  public static final String CHARACTER_ENCODING = "UTF-8";
  private static final Log log = LogFactory.getLog(DynamoDBUtil.class);
  private static final Gson gson;

  static {
    GsonBuilder gsonBuilder = new GsonBuilder();
    /* We hand serialize/deserialize ByteBuffer objects. */
    gsonBuilder.registerTypeAdapter(ByteBuffer.class, new ByteBufferSerializer());
    gsonBuilder.registerTypeAdapter(ByteBuffer.class, new ByteBufferDeserializer());
    gsonBuilder.registerTypeAdapter(SdkBytes.class, new SdkBytesSerializer());
    gsonBuilder.registerTypeAdapter(SdkBytes.class, new SdkBytesDeserializer());
    gsonBuilder.registerTypeAdapter(AttributeValue.class, new AttributeValueSerializer());
    gsonBuilder.registerTypeAdapter(AttributeValue.class, new AttributeValueDeserializer());

    gson = gsonBuilder.disableHtmlEscaping().create();
  }

  public static Double calculateAverageItemSize(TableDescription description) {
    if (description.itemCount() != 0) {
      return ((double) description.tableSizeBytes()) / ((double) description.itemCount());
    }
    return 0.0;
  }

  /**
   * base64 encode a byte array using org.apache.commons.codec.binary.Base64
   *
   * @param bytes bytes to encode
   * @return base64 encoded representation of the provided byte array
   */
  public static String base64EncodeByteArray(byte[] bytes) {
    try {
      byte[] encodeBase64 = Base64.encodeBase64(bytes);
      return new String(encodeBase64, "UTF-8");
    } catch (Exception e) {
      throw new RuntimeException("Exception while encoding bytes: " + Arrays.toString(bytes));
    }
  }

  /**
   * base64 decode a base64String using org.apache.commons.codec.binary.Base64
   *
   * @param base64String string to base64 decode
   * @return byte array representing the decoded base64 string
   */
  public static byte[] base64DecodeString(String base64String) {
    try {
      return Base64.decodeBase64(base64String.getBytes("UTF-8"));
    } catch (Exception e) {
      throw new RuntimeException("Exception while decoding " + base64String);
    }
  }

  /**
   * Converts a base64 encoded key into a ByteBuffer
   *
   * @param base64EncodedKey base64 encoded key to be converted
   * @return {@link ByteBuffer} representation of the provided base64 encoded key string
   */
  public static ByteBuffer base64StringToByteBuffer(String base64EncodedKey) {
    return ByteBuffer.wrap(base64DecodeString(base64EncodedKey));
  }

  /**
   * Converts a given list of base64EncodedKeys to a List of ByteBuffers
   *
   * @param base64EncodedKeys base64 encoded key(s) to be converted
   * @return List of {@link ByteBuffer}s representing the provided base64EncodedKeys
   */
  public static List<ByteBuffer> base64StringToByteBuffer(String... base64EncodedKeys) {
    List<ByteBuffer> byteBuffers = new ArrayList<>(base64EncodedKeys.length);
    for (String base64EncodedKey : base64EncodedKeys) {
      byteBuffers.add(base64StringToByteBuffer(base64EncodedKey));
    }
    return byteBuffers;
  }

  /**
   * Get a Gson reference with custom ByteBuffer serializer/deserializer.
   *
   * @return Gson reference with custom ByteBuffer serializer/deserializer
   */
  public static Gson getGson() {
    return gson;
  }

  static int getItemSizeBytes(Map<String, AttributeValue> item) {
    try {
      int itemSize = 0;
      for (Entry<String, AttributeValue> entry : item.entrySet()) {
        itemSize += entry.getKey() != null ? entry.getKey().getBytes(CHARACTER_ENCODING).length : 0;
        itemSize += entry.getValue() != null ? getAttributeSizeBytes(entry.getValue()) : 0;
      }
      return itemSize;
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  public static void verifyInterval(long intervalBeginTime, long intervalLength) {
    long interval = intervalBeginTime + intervalLength;
    long currentDateTime = new DateTime(DateTimeZone.UTC).getMillis();
    if (currentDateTime < interval) {
      try {
        Thread.sleep(interval - currentDateTime);
      } catch (InterruptedException e) {
        throw new RuntimeException("Interrupted while waiting ", e);
      }
    }
  }

  private static int getAttributeSizeBytes(AttributeValue att) throws UnsupportedEncodingException {
    int byteSize = 0;
    if (att.n() != null) {
      byteSize += att.n().getBytes(CHARACTER_ENCODING).length;
    } else if (att.s() != null) {
      byteSize += att.s().getBytes(CHARACTER_ENCODING).length;
    } else if (att.b() != null) {
      byteSize += att.b().asByteArray().length;
    } else if (att.hasNs()) {
      for (String number : att.ns()) {
        byteSize += number.getBytes(CHARACTER_ENCODING).length;
      }
    } else if (att.hasSs()) {
      for (String string : att.ss()) {
        byteSize += string.getBytes(CHARACTER_ENCODING).length;
      }
    } else if (att.hasBs()) {
      for (SdkBytes sdkBytes : att.bs()) {
        byteSize += sdkBytes.asByteArray().length;
      }
    } else if (att.hasM()) {
      for (Entry<String, AttributeValue> entry : att.m().entrySet()) {
        byteSize += getAttributeSizeBytes(entry.getValue())
            + entry.getKey().getBytes(CHARACTER_ENCODING).length;
      }
    } else if (att.hasL()) {
      for (AttributeValue entry : att.l()) {
        byteSize += getAttributeSizeBytes(entry);
      }
    }
    return byteSize;
  }

  static long getBoundedBatchLimit(Configuration config, long batchSize) {
    long maxItemsPerBatch = config.getLong(MAX_ITEMS_PER_BATCH, DEFAULT_MAX_ITEMS_PER_BATCH);
    return Math.min(Math.max(batchSize, 1), maxItemsPerBatch);
  }

  public static String getValueFromConf(Configuration conf, String confKey, String defaultValue) {
    if (conf == null) {
      return defaultValue;
    }
    return conf.get(confKey, defaultValue);
  }

  public static String getValueFromConf(Configuration conf, String confKey) {
    return getValueFromConf(conf, confKey, null);
  }

  /**
   * Get custom DynamoDB end-point from configuration.
   * @param conf   Job Configuration
   * @param region optional preferred region
   * @return end-point for DynamoDb service
   */
  public static String getDynamoDBEndpoint(Configuration conf, String region) {
    String endpoint = getValueFromConf(conf, DynamoDBConstants.ENDPOINT);
    if (Strings.isNullOrEmpty(endpoint)) {
      log.info(DynamoDBConstants.ENDPOINT + " not found from configuration.");
    } else {
      log.info("Using endpoint for DynamoDB: " + endpoint);
    }
    return endpoint;
  }

  /**
   * Calculates DynamoDB region.
   *
   * Algorithm details:
   * <ol>
   * <li> Use region in job configuration "dynamodb.region" value if available
   * <li> Use region in job configuration "dynamodb.regionid" value if available
   * <li> Use endpoint from EC2 Metadata of instance if available
   * <li> If all previous attempts fail, default to us-east-1 region
   * </ol>
   *
   * @param conf
   * @param region
   * @return region for DynamoDB service
   */
  public static String getDynamoDBRegion(Configuration conf, String region) {
    if (!Strings.isNullOrEmpty(region)) {
      return region;
    }

    // Return region in job configuration "dynamodb.region" value if available
    region = getValueFromConf(conf, DynamoDBConstants.REGION);
    if (!Strings.isNullOrEmpty(region)) {
      return region;
    }

    // Return region in job configuration "dynamodb.regionid" value if available
    region = getValueFromConf(conf, DynamoDBConstants.REGION_ID);
    if (!Strings.isNullOrEmpty(region)) {
      return region;
    }

    // Return region from EC2 Metadata of instance if available
    try {
      region = EC2MetadataUtils.getEC2InstanceRegion();
    } catch (Exception e) {
      log.warn(String.format("Exception when attempting to get AWS region information. "
          + "Will ignore and default to %s", DynamoDBConstants.DEFAULT_AWS_REGION), e);
    }
    if (!Strings.isNullOrEmpty(region)) {
      return region;
    }

    // Default to us-east-1 region if all previous attempts fail
    return DynamoDBConstants.DEFAULT_AWS_REGION;
  }

  /**
   *
   * Utility method to load an aws credentials provider from config via reflection. There are two
   * strategies followed:
   *   1. Load credential provider via its 'create' method.
   *      This is the intended credential provider construction mechanism with aws-java-sdk-v2
   *      For more information, visit {@link <a href="https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/migration-client-credentials.html">Credential Provider Changes</a>}.
   *   2. If 'create' method is not found, fallback to default no-arg constructor.
   *      This is kept to ensure utility method maintains backwards compatibility with what it
   *      used to support.
   *
   * @param providerClass - class name loaded from conf used as custom credential provider
   * @return - credential provider loaded via reflection using class name from conf
   */
  public static AwsCredentialsProvider loadAwsCredentialsProvider(
      String providerClass,
      Configuration conf) {
    if (DynamoDBReflectionUtils.hasFactoryMethod(providerClass, "create")) {
      log.debug("Provider: " + providerClass + " contains required method for creation - create()");
      return DynamoDBReflectionUtils.createInstanceFromFactory(providerClass, conf, "create");
    } else {
      log.debug("Falling back to default constructor.");
      return DynamoDBReflectionUtils.createInstanceOf(providerClass, conf);
    }
  }

  public static JobClient createJobClient(JobConf jobConf) {
    try {
      return new JobClient(jobConf);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static int calcMaxMapTasks(JobClient jobClient) throws IOException {
    JobConf conf = (JobConf) jobClient.getConf();
    NodeCapacityProvider nodeCapacityProvider = new ClusterTopologyNodeCapacityProvider(conf);
    YarnContainerAllocator yarnContainerAllocator = new RoundRobinYarnContainerAllocator();
    TaskCalculator taskCalculator = new TaskCalculator(jobClient, nodeCapacityProvider,
        yarnContainerAllocator);
    return taskCalculator.getMaxMapTasks();
  }

  /**
   * Check whether current resource manager is Yarn. (JobConf)
   * @param jobConf the configuration used by Hadoop MapReduce.
   * @return true if current resource manager is Yarn, false otherwise.
   */
  public static boolean isYarnEnabled(final JobConf jobConf) {
    return jobConf.getBoolean(DynamoDBConstants.YARN_RESOURCE_MANAGER_ENABLED,
        DynamoDBConstants.DEFAULT_YARN_RESOURCE_MANAGER_ENABLED);
  }

  /**
   * Check whether current resource manager is Yarn. (Configuration)
   * @param jobConf the configuration used by Hadoop MapReduce.
   * @return true if current resource manager is Yarn, false otherwise.
   */
  public static boolean isYarnEnabled(final Configuration jobConf) {
    return jobConf.getBoolean(DynamoDBConstants.YARN_RESOURCE_MANAGER_ENABLED,
      DynamoDBConstants.DEFAULT_YARN_RESOURCE_MANAGER_ENABLED);
  }

  /**
   * Calculate the estimate length for each split.
   * @param jobConf the configuration used by Hadoop MapReduce.
   * @param approxItemCountPerSplit the estimate item count for each split.
   * @return the estimated length for each split.
   */
  public static long calculateEstimateLength(final Configuration jobConf,
      long approxItemCountPerSplit) {
    long averageItemSize = (long) jobConf.getDouble(DynamoDBConstants.AVG_ITEM_SIZE,
        0);
    long splitSize = jobConf.getLong(DynamoDBConstants.SEGMENT_SPLIT_SIZE,
        DEFAULT_SEGMENT_SPLIT_SIZE);
    return Math.max(splitSize, averageItemSize * approxItemCountPerSplit);
  }

  /**
   * Since ByteBuffer does not have a no-arg constructor we hand serialize/deserialize them.
   */
  private static class ByteBufferSerializer implements JsonSerializer<ByteBuffer> {

    @Override
    public JsonElement serialize(ByteBuffer byteBuffer, Type type, JsonSerializationContext
        context) {

      String base64String = DynamoDBUtil.base64EncodeByteArray(byteBuffer.array());
      return new JsonPrimitive(base64String);
    }

  }

  /**
   * Since ByteBuffer does not have a no-arg constructor we hand serialize/deserialize them.
   */
  private static class ByteBufferDeserializer implements JsonDeserializer<ByteBuffer> {

    @Override
    public ByteBuffer deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext
        context) throws JsonParseException {

      String base64String = jsonElement.getAsJsonPrimitive().getAsString();
      return DynamoDBUtil.base64StringToByteBuffer(base64String);
    }
  }

  private static class SdkBytesSerializer implements JsonSerializer<SdkBytes> {

    @Override
    public JsonElement serialize(SdkBytes sdkBytes, Type type, JsonSerializationContext
        context) {

      String base64String = DynamoDBUtil.base64EncodeByteArray(sdkBytes.asByteArray());
      return new JsonPrimitive(base64String);
    }
  }

  private static class SdkBytesDeserializer implements JsonDeserializer<SdkBytes> {

    @Override
    public SdkBytes deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext
        context) throws JsonParseException {

      String base64String = jsonElement.getAsJsonPrimitive().getAsString();
      return SdkBytes.fromByteBuffer(DynamoDBUtil.base64StringToByteBuffer(base64String));
    }
  }

  private DynamoDBUtil() {

  }

}
