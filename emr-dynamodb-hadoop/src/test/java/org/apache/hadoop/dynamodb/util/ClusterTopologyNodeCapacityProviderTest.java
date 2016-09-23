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

package org.apache.hadoop.dynamodb.util;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.mapred.JobConf;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

@RunWith(MockitoJUnitRunner.class)
public class ClusterTopologyNodeCapacityProviderTest {
  private static final int NODE_MANAGER_MEMORY = 123;
  private static final int SCHEDULER_MAX_ALLOCATION_MEMORY = 456;

  private final JobConf conf = new JobConf();

  @Before
  public void setup() {
    conf.setInt("yarn.nodemanager.resource.memory-mb", NODE_MANAGER_MEMORY);
    conf.setInt("yarn.scheduler.maximum-allocation-mb", SCHEDULER_MAX_ALLOCATION_MEMORY);
  }

  @Test
  public void getMemoryInMB_masterAndCoreAreDifferent_nodeManagerMemoryReturned() throws
      IOException {
    final String jobFlowJsonString = "  {\n" + "  \"jobFlowId\": \"j-2AO77MNLG17NW\",\n" + "  " +
        "\"jobFlowCreationInstant\": 1429046932628,\n" + "  \"instanceCount\": 2,\n" + "  " +
        "\"masterInstanceId\": \"i-08dea4f4\",\n" + "  \"masterPrivateDnsName\": \"localhost\"," +
        "\n" + "  \"masterInstanceType\": \"m1.medium\",\n" + "  \"slaveInstanceType\": " +
        "\"m1.xlarge\",\n" + "  \"hadoopVersion\": \"2.4.0\",\n" + "  \"instanceGroups\": [\n" +
        "  {\n" + "    \"instanceGroupId\": \"ig-16NXM94TY33LB\",\n" + "    " +
        "\"instanceGroupName\": \"CORE\",\n" + "    \"instanceRole\": \"Core\",\n" + "    " +
        "\"marketType\": \"OnDemand\",\n" + "    \"instanceType\": \"m3.xlarge\",\n" + "    " +
        "\"requestedInstanceCount\": 1\n" + "  },\n" + "  {\n" + "    \"instanceGroupId\": " +
        "\"ig-2XQ29JGCTKLBL\",\n" + "    \"instanceGroupName\": \"MASTER\",\n" + "    " +
        "\"instanceRole\": \"Master\",\n" + "    \"marketType\": \"OnDemand\",\n" + "    " +
        "\"instanceType\": \"m3.xlarge\",\n" + "    \"requestedInstanceCount\": 1\n" + "  }]\n" +
        "  }";

    ClusterTopologyNodeCapacityProvider nodeCapacityProvider = new
        ClusterTopologyNodeCapacityProvider(conf) {
          @Override
          String readJobFlowJsonString() throws IOException {
            return jobFlowJsonString;
          }
        };
    assertEquals(NODE_MANAGER_MEMORY, nodeCapacityProvider.getCoreNodeMemoryMB());
  }

  @Test
  public void getMemoryInMB_masterAndCoreAreDifferent_schedulerMaxAllocationMemoryReturned()
      throws IOException {
    final String jobFlowJsonString = "  {\n" + "  \"jobFlowId\": \"j-2AO77MNLG17NW\",\n" + "  " +
        "\"jobFlowCreationInstant\": 1429046932628,\n" + "  \"instanceCount\": 2,\n" + "  " +
        "\"masterInstanceId\": \"i-08dea4f4\",\n" + "  \"masterPrivateDnsName\": \"localhost\"," +
        "\n" + "  \"masterInstanceType\": \"m1.medium\",\n" + "  \"slaveInstanceType\": " +
        "\"m1.xlarge\",\n" + "  \"hadoopVersion\": \"2.4.0\",\n" + "  \"instanceGroups\": [\n" +
        "  {\n" + "    \"instanceGroupId\": \"ig-16NXM94TY33LB\",\n" + "    " +
        "\"instanceGroupName\": \"CORE\",\n" + "    \"instanceRole\": \"Core\",\n" + "    " +
        "\"marketType\": \"OnDemand\",\n" + "    \"instanceType\": \"m3.xlarge\",\n" + "    " +
        "\"requestedInstanceCount\": 1\n" + "  },\n" + "  {\n" + "    \"instanceGroupId\": " +
        "\"ig-2XQ29JGCTKLBL\",\n" + "    \"instanceGroupName\": \"MASTER\",\n" + "    " +
        "\"instanceRole\": \"Master\",\n" + "    \"marketType\": \"OnDemand\",\n" + "    " +
        "\"instanceType\": \"m1.medium\",\n" + "    \"requestedInstanceCount\": 1\n" + "  }]\n" +
        "  }";

    ClusterTopologyNodeCapacityProvider nodeCapacityProvider = new
        ClusterTopologyNodeCapacityProvider(conf) {
          @Override
          String readJobFlowJsonString() throws IOException {
            return jobFlowJsonString;
          }
        };
    assertEquals(SCHEDULER_MAX_ALLOCATION_MEMORY, nodeCapacityProvider.getCoreNodeMemoryMB());
  }

  @Test
  public void getMemoryInMB_noJobFlowFile_schedulerMaxAllocationMemoryReturned() {
    // It is safe to assume that the machine to run the test has no file
    // /mnt/var/lib/info/job-flow.json
    ClusterTopologyNodeCapacityProvider nodeCapacityProvider = new
        ClusterTopologyNodeCapacityProvider(conf);
    assertEquals(SCHEDULER_MAX_ALLOCATION_MEMORY, nodeCapacityProvider.getCoreNodeMemoryMB());
  }
}
