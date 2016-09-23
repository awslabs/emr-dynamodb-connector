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

public class RoundRobinYarnContainerAllocator implements YarnContainerAllocator {
  private static final Log log = LogFactory.getLog(RoundRobinYarnContainerAllocator.class);

  /**
   * {@inheritDoc}
   */
  @Override
  public int getMaxMappers(int nodes, int reducers, int nodeSlots, int appMasterSlots, int
      mapSlots, int reduceSlots) {
    // Initialize all nodes with fully available slots
    int[] clusterNodeSlots = new int[nodes];
    for (int k = 0; k < nodes; k++) {
      clusterNodeSlots[k] = nodeSlots;
    }

    // Allocate the app master in the 1st node
    clusterNodeSlots[0] -= appMasterSlots;

    // Find slots for all reducers using round robin
    int reducerIndex = 0;
    int nodeIndex = 1;
    for (; reducerIndex < reducers; reducerIndex++) {
      if (nodeIndex >= nodes) {
        nodeIndex = 0;
      }
      if (clusterNodeSlots[nodeIndex] >= reduceSlots) {
        clusterNodeSlots[nodeIndex] -= reduceSlots;
      } else {
        break;
      }
      nodeIndex++;
    }

    if (reducerIndex < reducers) {
      log.warn("All slots are used for the app master and reducers. No slots for mappers.");
      return 0;
    }

    // Use all remaining slots for mappers
    int mappers = 0;
    for (nodeIndex = 0; nodeIndex < nodes; nodeIndex++) {
      mappers += clusterNodeSlots[nodeIndex] / mapSlots;
    }

    return mappers;
  }
}
