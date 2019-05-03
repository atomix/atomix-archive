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
package io.atomix.cluster.messaging.impl;

import java.time.Duration;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.util.concurrent.MoreExecutors;
import io.atomix.cluster.BootstrapService;
import io.atomix.cluster.Member;
import io.atomix.cluster.MemberService;
import io.atomix.cluster.Node;
import io.atomix.cluster.TestBootstrapService;
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;
import io.atomix.cluster.impl.ClusterMembershipManager;
import io.atomix.cluster.impl.MemberManager;
import io.atomix.cluster.impl.NodeDiscoveryManager;
import io.atomix.cluster.impl.VersionManager;
import io.atomix.cluster.protocol.HeartbeatMembershipProtocol;
import io.atomix.cluster.protocol.HeartbeatMembershipProtocolConfig;
import io.atomix.utils.Version;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Namespaces;
import io.atomix.utils.serializer.Serializer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Cluster event service test.
 */
public class ClusterEventManagerTest {
  private static final Serializer SERIALIZER = Serializer.using(Namespaces.BASIC);

  private TestMessagingServiceFactory messagingServiceFactory = new TestMessagingServiceFactory();
  private TestUnicastServiceFactory unicastServiceFactory = new TestUnicastServiceFactory();
  private TestBroadcastServiceFactory broadcastServiceFactory = new TestBroadcastServiceFactory();

  private Member buildNode(int memberId) {
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

  private CompletableFuture<ClusterEventManager> createService(int memberId, Collection<Node> bootstrapLocations) {
    Member localMember = buildNode(memberId);
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
              .thenCompose(v2 -> {
                ClusterMembershipManager clusterService = new ClusterMembershipManager(
                    memberService,
                    new VersionManager(Version.from("1.0.0")),
                    discoveryService,
                    membershipProtocol);
                return clusterService.start()
                    .thenApply(v3 -> new ClusterEventManager(clusterService, bootstrapService.getMessagingService()));
              });
        });
  }

  @Test
  public void testClusterEventService() throws Exception {
    Collection<Node> bootstrapLocations = buildBootstrapNodes(3);

    CompletableFuture<ClusterEventManager> future1 = createService(1, bootstrapLocations);
    CompletableFuture<ClusterEventManager> future2 = createService(2, bootstrapLocations);
    CompletableFuture<ClusterEventManager> future3 = createService(3, bootstrapLocations);

    ClusterEventManager eventService1 = future1.join();
    ClusterEventManager eventService2 = future2.join();
    ClusterEventManager eventService3 = future3.join();

    Thread.sleep(100);

    Set<Integer> events = new CopyOnWriteArraySet<>();

    eventService1.<String>subscribe("test1", SERIALIZER::decode, message -> {
      assertEquals("Hello world!", message);
      events.add(1);
    }, MoreExecutors.directExecutor()).join();

    eventService2.<String>subscribe("test1", SERIALIZER::decode, message -> {
      assertEquals("Hello world!", message);
      events.add(2);
    }, MoreExecutors.directExecutor()).join();

    eventService2.<String>subscribe("test1", SERIALIZER::decode, message -> {
      assertEquals("Hello world!", message);
      events.add(3);
    }, MoreExecutors.directExecutor()).join();

    eventService3.broadcast("test1", "Hello world!", SERIALIZER::encode);

    Thread.sleep(100);

    assertEquals(3, events.size());
    events.clear();

    eventService3.unicast("test1", "Hello world!");
    Thread.sleep(100);
    assertEquals(1, events.size());
    assertTrue(events.contains(3));
    events.clear();

    eventService3.unicast("test1", "Hello world!");
    Thread.sleep(100);
    assertEquals(1, events.size());
    assertTrue(events.contains(1));
    events.clear();

    eventService3.unicast("test1", "Hello world!");
    Thread.sleep(100);
    assertEquals(1, events.size());
    assertTrue(events.contains(2));
    events.clear();

    eventService3.unicast("test1", "Hello world!");
    Thread.sleep(100);
    assertEquals(1, events.size());
    assertTrue(events.contains(3));
    events.clear();

    eventService1.<String, String>subscribe("test2", SERIALIZER::decode, message -> {
      events.add(1);
      return message;
    }, SERIALIZER::encode, MoreExecutors.directExecutor()).join();
    eventService2.<String, String>subscribe("test2", SERIALIZER::decode, message -> {
      events.add(2);
      return message;
    }, SERIALIZER::encode, MoreExecutors.directExecutor()).join();

    assertEquals("Hello world!", eventService3.send("test2", "Hello world!").join());
    assertEquals(1, events.size());
    assertTrue(events.contains(1));
    events.clear();

    assertEquals("Hello world!", eventService3.send("test2", "Hello world!").join());
    assertEquals(1, events.size());
    assertTrue(events.contains(2));
    events.clear();

    assertEquals("Hello world!", eventService3.send("test2", "Hello world!").join());
    assertEquals(1, events.size());
    assertTrue(events.contains(1));
  }
}
