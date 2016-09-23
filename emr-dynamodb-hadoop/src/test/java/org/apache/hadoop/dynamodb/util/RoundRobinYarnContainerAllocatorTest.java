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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RoundRobinYarnContainerAllocatorTest {
  private static final int NODE_SLOTS = 8;
  private static final int APP_MASTER_SLOTS = 2;
  private static final int MAP_SLOTS = 4;
  private static final int REDUCE_SLOTS = 2;

  private final RoundRobinYarnContainerAllocator yarnContainerAllocator = new
      RoundRobinYarnContainerAllocator();

  @Test
  public void calculateMappers_1Node1Reducer_1Mapper() {
    // 1st node: 1 app master, 1 reducer, 1 mapper
    int mappers = yarnContainerAllocator.getMaxMappers(1 /* nodes */, 1 /* reducers */,
        NODE_SLOTS, APP_MASTER_SLOTS, MAP_SLOTS, REDUCE_SLOTS);
    assertEquals(1, mappers);
  }

  @Test
  public void calculateMappers_1Node2Reducer_0Mapper() {
    // 1st node: 1 app master, 2 reducers
    int mappers = yarnContainerAllocator.getMaxMappers(1 /* nodes */, 2 /* reducers */,
        NODE_SLOTS, APP_MASTER_SLOTS, MAP_SLOTS, REDUCE_SLOTS);
    assertEquals(0, mappers);
  }

  @Test
  public void calculateMappers_2Nodes1Reducer_2Mappers() {
    // 1st node: 1 app master, 1 mapper
    // 2nd node: 1 reducer, 1 mapper
    int mappers = yarnContainerAllocator.getMaxMappers(2 /* nodes */, 1 /* reducers */,
        NODE_SLOTS, APP_MASTER_SLOTS, MAP_SLOTS, REDUCE_SLOTS);
    assertEquals(2, mappers);
  }

  @Test
  public void calculateMappers_2Nodes2Reducers_2Mappers() {
    // 1st node: 1 app master, 1 reducer, 1 mapper
    // 2nd node: 1 reducer, 1 mapper
    int mappers = yarnContainerAllocator.getMaxMappers(2 /* nodes */, 2 /* reducers */,
        NODE_SLOTS, APP_MASTER_SLOTS, MAP_SLOTS, REDUCE_SLOTS);
    assertEquals(2, mappers);
  }

  @Test
  public void calculateMappers_2Nodes3Reducers_2Mappers() {
    // 1st node: 1 app master, 1 reducer, 1 mapper
    // 2nd node: 2 reducers, 1 mapper
    int mappers = yarnContainerAllocator.getMaxMappers(2 /* nodes */, 3 /* reducers */,
        NODE_SLOTS, APP_MASTER_SLOTS, MAP_SLOTS, REDUCE_SLOTS);
    assertEquals(2, mappers);
  }

  @Test
  public void calculateMappers_2Nodes4Reducers_1Mapper() {
    // 1st node: 1 app master, 2 reducer
    // 2nd node: 2 reducers, 1 mapper
    int mappers = yarnContainerAllocator.getMaxMappers(2 /* nodes */, 4 /* reducers */,
        NODE_SLOTS, APP_MASTER_SLOTS, MAP_SLOTS, REDUCE_SLOTS);
    assertEquals(1, mappers);
  }

  @Test
  public void calculateMappers_3Nodes1Reducer_4Mappers() {
    // 1st node: 1 app master, 1 mapper
    // 2nd node: 1 reducer, 1 mapper
    // 3rd node: 2 mappers
    int mappers = yarnContainerAllocator.getMaxMappers(3 /* nodes */, 1 /* reducers */,
        NODE_SLOTS, APP_MASTER_SLOTS, MAP_SLOTS, REDUCE_SLOTS);
    assertEquals(4, mappers);
  }

  @Test
  public void calculateMappers_3Nodes2Reducer_3Mappers() {
    // 1st node: 1 app master, 1 mapper
    // 2nd node: 1 reducer, 1 mapper
    // 3rd node: 1 reducer, 1 mapper
    int mappers = yarnContainerAllocator.getMaxMappers(3 /* nodes */, 2 /* reducers */,
        NODE_SLOTS, APP_MASTER_SLOTS, MAP_SLOTS, REDUCE_SLOTS);
    assertEquals(3, mappers);
  }
}
