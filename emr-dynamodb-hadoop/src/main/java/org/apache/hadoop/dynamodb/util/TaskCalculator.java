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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRJobConfig;

import java.io.IOException;

public class TaskCalculator {
  private static final Log log = LogFactory.getLog(TaskCalculator.class);

  private final JobClient jobClient;
  private final NodeCapacityProvider nodeCapacityProvider;
  private final YarnContainerAllocator yarnContainerAllocator;

  public TaskCalculator(JobClient jobClient, NodeCapacityProvider nodeCapacityProvider,
      YarnContainerAllocator yarnContainerAllocator) {
    this.jobClient = jobClient;
    this.nodeCapacityProvider = nodeCapacityProvider;
    this.yarnContainerAllocator = yarnContainerAllocator;
  }

  public int getMaxMapTasks() throws IOException {
    JobConf conf = (JobConf) jobClient.getConf();

    // Total number of nodes in the cluster
    int nodes = jobClient.getClusterStatus().getTaskTrackers();
    log.info("Cluster has " + nodes + " active nodes.");
    if (nodes == 0) {
      log.warn("Cluster doesn't have any nodes");
      return 0;
    }

    // Memory per slot
    int slotMemory = conf.getInt("yarn.scheduler.minimum-allocation-mb", 1024); // Default value
    // from yarn-default.xml

    // Number of slots in a core node
    int nodeMemory = nodeCapacityProvider.getCoreNodeMemoryMB();
    int nodeSlots = nodeMemory / slotMemory;

    // Number of slots for a mapper
    int mapMemory = conf.getInt(MRJobConfig.MAP_MEMORY_MB, MRJobConfig.DEFAULT_MAP_MEMORY_MB);
    int mapSlots = (int) Math.ceil((double) mapMemory / slotMemory);

    // Number of slots for an application master
    int amMemory = conf.getInt(MRJobConfig.MR_AM_VMEM_MB, MRJobConfig.DEFAULT_MR_AM_VMEM_MB);
    int appMasterSlots = (int) Math.ceil((double) amMemory / slotMemory);

    // Number of slots for a reducer
    int reduceMemory = conf.getInt(MRJobConfig.REDUCE_MEMORY_MB, MRJobConfig
        .DEFAULT_REDUCE_MEMORY_MB);
    int reduceSlots = (int) Math.ceil((double) reduceMemory / slotMemory);

    // Number of reducers
    int reducers = conf.getNumReduceTasks();

    // Calculate the number of mappers
    int mappers = yarnContainerAllocator.getMaxMappers(nodes, reducers, nodeSlots,
        appMasterSlots, mapSlots, reduceSlots);

    log.info("Slot size: " + slotMemory + "MB.");
    log.info("Node manager can allocate " + nodeMemory + "MB (" + nodeSlots + " slots) for "
        + "containers on each node.");
    log.info("Each mapper needs: " + mapMemory + "MB. (" + mapSlots + " slots)");
    log.info("Each reducer needs: " + reduceMemory + "MB. (" + reduceSlots + " slots)");
    log.info("MapReduce Application Manager needs: " + amMemory + " MB. (" + appMasterSlots + " "
        + "slots)");
    log.info("Number of reducers: " + reducers);
    log.info("Max number of cluster map tasks: " + mappers);

    if (mappers < 1) {
      log.warn("The calculated max number of concurrent map tasks is less than 1. Use 1 instead.");
      mappers = 1;
    }

    return mappers;
  }
}
