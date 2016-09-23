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

public interface YarnContainerAllocator {
  /**
   * Calculates the max number of mappers based on the cluster configuration.
   *
   * @param nodes          number of core nodes
   * @param reducers       number of reducers
   * @param nodeSlots      number of slots in a core node
   * @param appMasterSlots number of slots for an application master
   * @param mapSlots       number of slots for a mapper
   * @param reduceSlots    number of slots for a reducer
   * @return the calculated max number of mappers
   */
  int getMaxMappers(int nodes, int reducers, int nodeSlots, int appMasterSlots, int mapSlots, int
      reduceSlots);
}
