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

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.atomix.cluster.BootstrapService;
import io.atomix.cluster.ClusterMembershipEvent;
import io.atomix.cluster.ClusterMembershipEventListener;
import io.atomix.cluster.Member;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.MemberService;
import io.atomix.cluster.Node;
import io.atomix.cluster.TestBootstrapService;
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;
import io.atomix.cluster.messaging.impl.TestBroadcastServiceFactory;
import io.atomix.cluster.messaging.impl.TestMessagingServiceFactory;
import io.atomix.cluster.messaging.impl.TestUnicastServiceFactory;
import io.atomix.cluster.protocol.HeartbeatMembershipProtocol;
import io.atomix.cluster.protocol.HeartbeatMembershipProtocolConfig;
import io.atomix.utils.Version;
import io.atomix.utils.net.Address;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Default cluster service test.
 */
public class ClusterMembershipManagerTest {
  private TestMessagingServiceFactory messagingServiceFactory = new TestMessagingServiceFactory();
  private TestUnicastServiceFactory unicastServiceFactory = new TestUnicastServiceFactory();
  private TestBroadcastServiceFactory broadcastServiceFactory = new TestBroadcastServiceFactory();

  private Member buildMember(int memberId) {
    return Member.builder(String.valueOf(memberId))
        .withHost("localhost")
        .withPort(memberId)
        .build();
  }

  private Collection<Node> buildBootstrapNodes(int nodes) {
    return IntStream.range(1, nodes + 1)
        .mapToObj(id -> Node.builder()
            .withId(String.valueOf(id))
            .withAddress(Address.from("localhost", id))
            .build())
        .collect(Collectors.toList());
  }

  private CompletableFuture<ClusterMembershipManager> createService(int memberId, Collection<Node> bootstrapLocations) {
    Member localMember = buildMember(memberId);
    MemberService memberService = new MemberManager(localMember);
    BootstrapService bootstrapService = new TestBootstrapService(
        messagingServiceFactory.newMessagingService(localMember.address()),
        unicastServiceFactory.newUnicastService(localMember.address()),
        broadcastServiceFactory.newBroadcastService());
    NodeDiscoveryManager discoveryService = new NodeDiscoveryManager(
        memberService, bootstrapService, new BootstrapDiscoveryProvider(bootstrapLocations));
    return discoveryService.start()
        .thenCompose(v -> {
          HeartbeatMembershipProtocol membershipProtocol = new HeartbeatMembershipProtocol(
              new MemberManager(localMember), discoveryService, bootstrapService);
          return membershipProtocol.start(new HeartbeatMembershipProtocolConfig().setFailureTimeout(Duration.ofSeconds(2)))
              .thenApply(v2 -> new ClusterMembershipManager(
                  memberService,
                  new VersionManager(Version.from("1.0.0")),
                  discoveryService,
                  membershipProtocol));
        });
  }

  @Test
  public void testClusterService() throws Exception {
    Collection<Node> bootstrapLocations = buildBootstrapNodes(3);

    CompletableFuture<ClusterMembershipManager> future1 = createService(1, bootstrapLocations);
    CompletableFuture<ClusterMembershipManager> future2 = createService(2, bootstrapLocations);
    CompletableFuture<ClusterMembershipManager> future3 = createService(3, bootstrapLocations);

    ClusterMembershipManager clusterService1 = future1.join();
    ClusterMembershipManager clusterService2 = future2.join();
    ClusterMembershipManager clusterService3 = future3.join();

    Thread.sleep(5000);

    assertEquals(3, clusterService1.getMembers().size());
    assertEquals(3, clusterService2.getMembers().size());
    assertEquals(3, clusterService3.getMembers().size());

    assertTrue(clusterService1.getLocalMember().isActive());
    assertTrue(clusterService1.getMember(MemberId.from("1")).isActive());
    assertTrue(clusterService1.getMember(MemberId.from("2")).isActive());
    assertTrue(clusterService1.getMember(MemberId.from("3")).isActive());

    assertEquals("1.0.0", clusterService1.getMember("1").version().toString());
    assertEquals("1.0.0", clusterService1.getMember("2").version().toString());
    assertEquals("1.0.1", clusterService1.getMember("3").version().toString());

    ClusterMembershipManager ephemeralClusterService = createService(4, bootstrapLocations).join();

    Thread.sleep(1000);

    assertEquals(4, clusterService1.getMembers().size());
    assertEquals(4, clusterService2.getMembers().size());
    assertEquals(4, clusterService3.getMembers().size());
    assertEquals(4, ephemeralClusterService.getMembers().size());

    assertEquals("1.0.0", clusterService1.getMember("1").version().toString());
    assertEquals("1.0.0", clusterService1.getMember("2").version().toString());
    assertEquals("1.0.1", clusterService1.getMember("3").version().toString());
    assertEquals("1.1.0", clusterService1.getMember("4").version().toString());

    clusterService1.stop().join();

    Thread.sleep(5000);

    assertEquals(3, clusterService2.getMembers().size());

    assertNull(clusterService2.getMember(MemberId.from("1")));
    assertTrue(clusterService2.getMember(MemberId.from("2")).isActive());
    assertTrue(clusterService2.getMember(MemberId.from("3")).isActive());
    assertTrue(clusterService2.getMember(MemberId.from("4")).isActive());

    ephemeralClusterService.stop().join();

    Thread.sleep(5000);

    assertEquals(2, clusterService2.getMembers().size());
    assertNull(clusterService2.getMember(MemberId.from("1")));
    assertTrue(clusterService2.getMember(MemberId.from("2")).isActive());
    assertTrue(clusterService2.getMember(MemberId.from("3")).isActive());
    assertNull(clusterService2.getMember(MemberId.from("4")));

    Thread.sleep(2500);

    assertEquals(2, clusterService2.getMembers().size());

    assertNull(clusterService2.getMember(MemberId.from("1")));
    assertTrue(clusterService2.getMember(MemberId.from("2")).isActive());
    assertTrue(clusterService2.getMember(MemberId.from("3")).isActive());
    assertNull(clusterService2.getMember(MemberId.from("4")));

    TestClusterMembershipEventListener eventListener = new TestClusterMembershipEventListener();
    clusterService2.addListener(eventListener);

    ClusterMembershipEvent event;
    clusterService3.getLocalMember().properties().put("foo", "bar");

    event = eventListener.nextEvent();
    assertEquals(ClusterMembershipEvent.Type.METADATA_CHANGED, event.type());
    assertEquals("bar", event.subject().properties().get("foo"));

    clusterService3.getLocalMember().properties().put("foo", "baz");

    event = eventListener.nextEvent();
    assertEquals(ClusterMembershipEvent.Type.METADATA_CHANGED, event.type());
    assertEquals("baz", event.subject().properties().get("foo"));

    CompletableFuture.allOf(new CompletableFuture[]{clusterService1.stop(), clusterService2.stop(),
        clusterService3.stop()}).join();
  }

  private class TestClusterMembershipEventListener implements ClusterMembershipEventListener {
    private BlockingQueue<ClusterMembershipEvent> queue = new ArrayBlockingQueue<ClusterMembershipEvent>(10);

    @Override
    public void event(ClusterMembershipEvent event) {
      queue.add(event);
    }

    ClusterMembershipEvent nextEvent() {
      try {
        return queue.poll(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        return null;
      }
    }
  }
}
