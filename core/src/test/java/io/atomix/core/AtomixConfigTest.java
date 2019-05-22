/*
 * Copyright 2018-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.core;

import java.time.Duration;

import io.atomix.cluster.ClusterConfig;
import io.atomix.cluster.MemberConfig;
import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.protocols.raft.partition.RaftPartitionGroup;
import io.atomix.protocols.raft.partition.RaftPartitionGroupConfig;
import io.atomix.utils.memory.MemorySize;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Atomix configuration test.
 */
public class AtomixConfigTest {
  @Test
  public void testDefaultAtomixConfig() throws Exception {
    AtomixConfig config = Atomix.config();
    assertTrue(config.getPartitionGroups().isEmpty());
  }

  @Test
  public void testAtomixConfig() throws Exception {
    AtomixConfig config = Atomix.config(getClass().getClassLoader().getResource("test.conf").getPath());

    ClusterConfig cluster = config.getClusterConfig();
    assertEquals("test", cluster.getClusterId());

    MemberConfig node = cluster.getNodeConfig();
    assertEquals("one", node.getId().id());
    assertEquals("localhost:5000", node.getAddress().toString());
    assertEquals("foo", node.getZoneId());
    assertEquals("bar", node.getRackId());
    assertEquals("baz", node.getHostId());
    assertEquals("bar", node.getProperties().getProperty("foo"));
    assertEquals("baz", node.getProperties().getProperty("bar"));

    MessagingConfig messaging = cluster.getMessagingConfig();
    assertEquals(2, messaging.getInterfaces().size());
    assertEquals("127.0.0.1", messaging.getInterfaces().get(0));
    assertEquals("0.0.0.0", messaging.getInterfaces().get(1));
    assertEquals(5000, messaging.getPort().intValue());
    assertEquals(Duration.ofSeconds(10), messaging.getConnectTimeout());
    assertTrue(messaging.getTlsConfig().isEnabled());

    RaftPartitionGroupConfig managementGroup = (RaftPartitionGroupConfig) config.getSystemGroup();
    assertEquals(RaftPartitionGroup.TYPE, managementGroup.getType());
    assertEquals(1, managementGroup.getPartitions());
    assertEquals(new MemorySize(1024 * 1024 * 16), managementGroup.getStorageConfig().getSegmentSize());

    RaftPartitionGroupConfig groupOne = (RaftPartitionGroupConfig) config.getPartitionGroups().get("one");
    assertEquals(RaftPartitionGroup.TYPE, groupOne.getType());
    assertEquals("one", groupOne.getName());
    assertEquals(7, groupOne.getPartitions());
  }
}
