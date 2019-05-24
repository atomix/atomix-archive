/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.cluster.impl;

import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.atomix.cluster.ClusterConfig;
import io.atomix.cluster.Member;
import io.atomix.cluster.MemberConfig;
import io.atomix.cluster.MemberEvent;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.MemberService;
import io.atomix.cluster.MembershipConfig;
import io.atomix.cluster.NodeConfig;
import io.atomix.cluster.NodeId;
import io.atomix.cluster.discovery.BootstrapDiscoveryConfig;
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;
import io.atomix.cluster.discovery.Node;
import io.atomix.cluster.grpc.impl.ChannelServiceImpl;
import io.atomix.cluster.grpc.impl.ServiceRegistryImpl;
import io.atomix.cluster.messaging.impl.TestMessagingServiceFactory;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Default cluster service test.
 */
public class ClusterMembershipManagerTest {
  private TestMessagingServiceFactory messagingServiceFactory = new TestMessagingServiceFactory();

  private Member buildMember(int memberId, String version) {
    return Member.newBuilder()
        .setId(String.valueOf(memberId))
        .setHost("localhost")
        .setPort(5000 + memberId)
        .setVersion(version)
        .build();
  }

  private Collection<Node> buildBootstrapNodes(int nodes) {
    return IntStream.range(1, nodes + 1)
        .mapToObj(id -> Node.newBuilder()
            .setId(String.valueOf(id))
            .setHost("localhost")
            .setPort(5000 + id)
            .build())
        .collect(Collectors.toList());
  }

  private CompletableFuture<ClusterMembershipManager> createService(int memberId, String version, Collection<Node> bootstrapLocations) {
    Member localMember = buildMember(memberId, version);
    MemberService memberService = new MemberManager(localMember);
    BootstrapDiscoveryProvider discoveryProvider = new BootstrapDiscoveryProvider();
    NodeDiscoveryManager discoveryService = new NodeDiscoveryManager(discoveryProvider);
    ServiceRegistryImpl serviceRegistry = new ServiceRegistryImpl();
    ChannelServiceImpl channelService = new ChannelServiceImpl();
    ClusterConfig config = new ClusterConfig()
        .setNodeConfig(new MemberConfig()
            .setId(String.valueOf(memberId))
            .setHost("localhost")
            .setPort(5000 + memberId));
    return serviceRegistry.start(config)
        .thenCompose(v -> channelService.start(config))
        .thenCompose(v -> discoveryProvider.start(new BootstrapDiscoveryConfig()
            .setNodes(bootstrapLocations.stream()
                .map(node -> new NodeConfig()
                    .setId(NodeId.from(node.getId(), node.getNamespace()))
                    .setHost(node.getHost())
                    .setPort(node.getPort()))
                .collect(Collectors.toList()))))
        .thenCompose(v -> discoveryService.start())
        .thenCompose(v -> {
          ClusterMembershipManager membershipManager = new ClusterMembershipManager(
              serviceRegistry,
              channelService,
              memberService,
              discoveryService);
          return membershipManager.start(new MembershipConfig()).thenApply(v2 -> membershipManager);
        });
  }

  @Test
  public void testClusterService() throws Exception {
    Collection<Node> bootstrapLocations = buildBootstrapNodes(3);

    CompletableFuture<ClusterMembershipManager> future1 = createService(1, "1.0.0", bootstrapLocations);
    CompletableFuture<ClusterMembershipManager> future2 = createService(2, "1.0.0", bootstrapLocations);
    CompletableFuture<ClusterMembershipManager> future3 = createService(3, "1.0.1", bootstrapLocations);

    ClusterMembershipManager clusterService1 = future1.join();
    ClusterMembershipManager clusterService2 = future2.join();
    ClusterMembershipManager clusterService3 = future3.join();

    Thread.sleep(5000);

    assertEquals(3, clusterService1.getMembers().size());
    assertEquals(3, clusterService2.getMembers().size());
    assertEquals(3, clusterService3.getMembers().size());

    assertTrue(clusterService1.getLocalMember().getState() == Member.State.ALIVE);
    assertTrue(clusterService1.getMember(MemberId.from("1")).getState() == Member.State.ALIVE);
    assertTrue(clusterService1.getMember(MemberId.from("2")).getState() == Member.State.ALIVE);
    assertTrue(clusterService1.getMember(MemberId.from("3")).getState() == Member.State.ALIVE);

    assertEquals("1.0.0", clusterService1.getMember("1").getVersion());
    assertEquals("1.0.0", clusterService1.getMember("2").getVersion());
    assertEquals("1.0.1", clusterService1.getMember("3").getVersion());

    ClusterMembershipManager ephemeralClusterService = createService(4, "1.1.0", bootstrapLocations).join();

    Thread.sleep(1000);

    assertEquals(4, clusterService1.getMembers().size());
    assertEquals(4, clusterService2.getMembers().size());
    assertEquals(4, clusterService3.getMembers().size());
    assertEquals(4, ephemeralClusterService.getMembers().size());

    assertEquals("1.0.0", clusterService1.getMember("1").getVersion());
    assertEquals("1.0.0", clusterService1.getMember("2").getVersion());
    assertEquals("1.0.1", clusterService1.getMember("3").getVersion());
    assertEquals("1.1.0", clusterService1.getMember("4").getVersion());

    clusterService1.stop().join();

    Thread.sleep(5000);

    assertEquals(3, clusterService2.getMembers().size());

    assertNull(clusterService2.getMember(MemberId.from("1")));
    assertTrue(clusterService2.getMember(MemberId.from("2")).getState() == Member.State.ALIVE);
    assertTrue(clusterService2.getMember(MemberId.from("3")).getState() == Member.State.ALIVE);
    assertTrue(clusterService2.getMember(MemberId.from("4")).getState() == Member.State.ALIVE);

    ephemeralClusterService.stop().join();

    Thread.sleep(5000);

    assertEquals(2, clusterService2.getMembers().size());
    assertNull(clusterService2.getMember(MemberId.from("1")));
    assertTrue(clusterService2.getMember(MemberId.from("2")).getState() == Member.State.ALIVE);
    assertTrue(clusterService2.getMember(MemberId.from("3")).getState() == Member.State.ALIVE);
    assertNull(clusterService2.getMember(MemberId.from("4")));

    Thread.sleep(2500);

    assertEquals(2, clusterService2.getMembers().size());

    assertNull(clusterService2.getMember(MemberId.from("1")));
    assertTrue(clusterService2.getMember(MemberId.from("2")).getState() == Member.State.ALIVE);
    assertTrue(clusterService2.getMember(MemberId.from("3")).getState() == Member.State.ALIVE);
    assertNull(clusterService2.getMember(MemberId.from("4")));

    TestClusterMembershipEventListener eventListener = new TestClusterMembershipEventListener();
    clusterService2.addListener(eventListener);

    CompletableFuture.allOf(new CompletableFuture[]{clusterService1.stop(), clusterService2.stop(),
        clusterService3.stop()}).join();
  }

  private class TestClusterMembershipEventListener implements Consumer<MemberEvent> {
    private BlockingQueue<MemberEvent> queue = new ArrayBlockingQueue<>(10);

    @Override
    public void accept(MemberEvent event) {
      queue.add(event);
    }

    MemberEvent nextEvent() {
      try {
        return queue.poll(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        return null;
      }
    }
  }
}
