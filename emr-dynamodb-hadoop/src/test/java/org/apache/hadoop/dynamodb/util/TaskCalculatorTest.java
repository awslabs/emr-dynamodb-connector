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

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

@RunWith(MockitoJUnitRunner.class)
public class TaskCalculatorTest {
  private static final int NODES = 5;
  private static final int REDUCERS = 1;
  private final JobConf conf = new JobConf();
  @Mock
  private JobClient client;
  @Mock
  private NodeCapacityProvider nodeCapacityProvider;
  @Mock
  private YarnContainerAllocator yarnContainerAllocator;
  @Mock
  private ClusterStatus clusterStatus;
  private TaskCalculator taskCalculator;

  @Before
  public void setup() throws IOException {
    conf.setNumReduceTasks(REDUCERS);
    when(clusterStatus.getTaskTrackers()).thenReturn(NODES);
    when(client.getClusterStatus()).thenReturn(clusterStatus);
    when(client.getConf()).thenReturn(conf);

    taskCalculator = new TaskCalculator(client, nodeCapacityProvider, yarnContainerAllocator);
  }

  @Test
  public void calcMaxMapTasks_m3xlargeInconsistent_correctAllocatorCallMade() throws IOException {
    when(nodeCapacityProvider.getCoreNodeMemoryMB()).thenReturn(11520); // 45 slots
    conf.setInt("yarn.scheduler.minimum-allocation-mb", 256); // 1 slot
    conf.setInt("yarn.app.mapreduce.am.resource.mb", 2880); // 12 slots
    conf.setInt("mapreduce.map.memory.mb", 1440); // 6 slots
    conf.setInt("mapreduce.reduce.memory.mb", 2880); // 12 slots

    taskCalculator.getMaxMapTasks();
    verify(yarnContainerAllocator).getMaxMappers(NODES, REDUCERS, 45 /* node slots */, 12 /* app
    master slots */, 6 /* map slots */, 12 /* reduce slots */);
  }

  @Test
  public void calcMaxMapTasks_m3xlargeConsistent_correctAllocatorCallMade() throws IOException {
    when(nodeCapacityProvider.getCoreNodeMemoryMB()).thenReturn(11520); // 8 slots
    conf.setInt("yarn.scheduler.minimum-allocation-mb", 1440); // 1 slot
    conf.setInt("yarn.app.mapreduce.am.resource.mb", 2880); // 2 slots
    conf.setInt("mapreduce.map.memory.mb", 1440); // 1 slot
    conf.setInt("mapreduce.reduce.memory.mb", 2880); // 2 slots

    taskCalculator.getMaxMapTasks();
    verify(yarnContainerAllocator).getMaxMappers(NODES, REDUCERS, 8 /* node slots */, 2 /* app
    master slots */, 1 /* map slots */, 2 /* reduce slots */);
  }

}
