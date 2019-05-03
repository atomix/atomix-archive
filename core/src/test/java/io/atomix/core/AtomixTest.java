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

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.atomix.cluster.ClusterMembershipEvent;
import io.atomix.cluster.ClusterMembershipEventListener;
import io.atomix.cluster.Member;
import io.atomix.core.counter.AtomicCounter;
import io.atomix.core.counter.AtomicCounterType;
import io.atomix.core.counter.DistributedCounterType;
import io.atomix.core.idgenerator.AtomicIdGeneratorType;
import io.atomix.core.lock.AtomicLockType;
import io.atomix.core.lock.DistributedLockType;
import io.atomix.core.log.DistributedLog;
import io.atomix.core.log.DistributedLogPartition;
import io.atomix.core.map.AtomicMapType;
import io.atomix.core.map.DistributedMap;
import io.atomix.core.map.DistributedMapType;
import io.atomix.core.set.DistributedSetType;
import io.atomix.core.value.AtomicValueType;
import io.atomix.core.value.DistributedValueType;
import io.atomix.protocols.log.DistributedLogProtocol;
import io.atomix.protocols.log.partition.LogPartitionGroup;
import io.atomix.protocols.raft.partition.RaftPartitionGroup;
import io.atomix.utils.concurrent.Futures;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Atomix test.
 */
public class AtomixTest extends AbstractAtomixTest {
  private List<Atomix> instances;

  @Before
  public void setupInstances() throws Exception {
    setupAtomix();
    instances = new ArrayList<>();
  }

  @After
  public void teardownInstances() throws Exception {
    List<CompletableFuture<Void>> futures = instances.stream().map(Atomix::stop).collect(Collectors.toList());
    try {
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).get(30, TimeUnit.SECONDS);
    } catch (Exception e) {
      // Do nothing
    }
    teardownAtomix();
  }

  /**
   * Creates and starts a new test Atomix instance.
   */
  protected CompletableFuture<Atomix> startAtomix(int id, List<Integer> persistentIds) {
    return startAtomix(id, persistentIds, b -> b.build());
  }

  /**
   * Creates and starts a new test Atomix instance.
   */
  protected CompletableFuture<Atomix> startAtomix(int id, List<Integer> persistentIds, Function<AtomixBuilder, Atomix> builderFunction) {
    Atomix atomix = createAtomix(id, persistentIds, builderFunction);
    instances.add(atomix);
    return atomix.start().thenApply(v -> atomix);
  }

  /**
   * Creates and starts a new test Atomix instance.
   */
  protected CompletableFuture<Atomix> startAtomix(int id, List<Integer> persistentIds, Properties properties, Function<AtomixBuilder, Atomix> builderFunction) {
    Atomix atomix = createAtomix(id, persistentIds, properties, builderFunction);
    instances.add(atomix);
    return atomix.start().thenApply(v -> atomix);
  }

  /**
   * Tests scaling up a cluster.
   */
  @Test
  public void testScaleUpPersistent() throws Exception {
    Atomix atomix1 = startAtomix(1, Arrays.asList(1), consensus(1, Arrays.asList(1)))
        .get(30, TimeUnit.SECONDS);
    Atomix atomix2 = startAtomix(2, Arrays.asList(1, 2)).get(30, TimeUnit.SECONDS);
    Atomix atomix3 = startAtomix(3, Arrays.asList(1, 2, 3)).get(30, TimeUnit.SECONDS);
  }

  @Test
  public void testLogPrimitive() throws Exception {
    CompletableFuture<Atomix> future1 = startAtomix(1, Arrays.asList(1, 2), builder ->
        builder.withManagementGroup(RaftPartitionGroup.builder("system")
            .withNumPartitions(1)
            .withMembers(String.valueOf(1), String.valueOf(2))
            .withDataDirectory(new File(new File(DATA_DIR, "log"), "1"))
            .build())
            .withPartitionGroups(LogPartitionGroup.builder("log")
                .withNumPartitions(3)
                .build())
            .build());

    CompletableFuture<Atomix> future2 = startAtomix(2, Arrays.asList(1, 2), builder ->
        builder.withManagementGroup(RaftPartitionGroup.builder("system")
            .withNumPartitions(1)
            .withMembers(String.valueOf(1), String.valueOf(2))
            .withDataDirectory(new File(new File(DATA_DIR, "log"), "2"))
            .build())
            .withPartitionGroups(LogPartitionGroup.builder("log")
                .withNumPartitions(3)
                .build())
            .build());

    Atomix atomix1 = future1.get();
    Atomix atomix2 = future2.get();

    DistributedLog<String> log1 = atomix1.<String>logBuilder()
        .withProtocol(DistributedLogProtocol.builder()
            .build())
        .build();

    DistributedLog<String> log2 = atomix2.<String>logBuilder()
        .withProtocol(DistributedLogProtocol.builder()
            .build())
        .build();

    assertEquals(3, log1.getPartitions().size());
    assertEquals(1, log1.getPartitions().get(0).id());
    assertEquals(2, log1.getPartitions().get(1).id());
    assertEquals(3, log1.getPartitions().get(2).id());
    assertEquals(1, log2.getPartition(1).id());
    assertEquals(2, log2.getPartition(2).id());
    assertEquals(3, log2.getPartition(3).id());

    DistributedLogPartition<String> partition1 = log1.getPartition("Hello world!");
    DistributedLogPartition<String> partition2 = log2.getPartition("Hello world!");
    assertEquals(partition1.id(), partition2.id());

    CountDownLatch latch = new CountDownLatch(2);
    partition2.consume(record -> {
      assertEquals("Hello world!", record.value());
      latch.countDown();
    });
    log2.consume(record -> {
      assertEquals("Hello world!", record.value());
      latch.countDown();
    });
    partition1.produce("Hello world!");
    latch.await(10, TimeUnit.SECONDS);
    assertEquals(0, latch.getCount());
  }

  @Test
  public void testLogBasedPrimitives() throws Exception {
    CompletableFuture<Atomix> future1 = startAtomix(1, Arrays.asList(1, 2), builder ->
        builder.withManagementGroup(RaftPartitionGroup.builder("system")
            .withNumPartitions(1)
            .withMembers(String.valueOf(1), String.valueOf(2))
            .withDataDirectory(new File(new File(DATA_DIR, "log"), "1"))
            .build())
            .withPartitionGroups(LogPartitionGroup.builder("log")
                .withNumPartitions(3)
                .build())
            .build());

    CompletableFuture<Atomix> future2 = startAtomix(2, Arrays.asList(1, 2), builder ->
        builder.withManagementGroup(RaftPartitionGroup.builder("system")
            .withNumPartitions(1)
            .withMembers(String.valueOf(1), String.valueOf(2))
            .withDataDirectory(new File(new File(DATA_DIR, "log"), "2"))
            .build())
            .withPartitionGroups(LogPartitionGroup.builder("log")
                .withNumPartitions(3)
                .build())
            .build());

    Atomix atomix1 = future1.get();
    Atomix atomix2 = future2.get();

    DistributedMap<String, String> map1 = atomix1.<String, String>mapBuilder("test-map")
        .withProtocol(DistributedLogProtocol.builder().build())
        .build();

    DistributedMap<String, String> map2 = atomix2.<String, String>mapBuilder("test-map")
        .withProtocol(DistributedLogProtocol.builder().build())
        .build();

    CountDownLatch latch = new CountDownLatch(1);
    map2.addListener(event -> {
      map2.async().get("foo").thenAccept(value -> {
        assertEquals("bar", value);
        latch.countDown();
      });
    });
    map1.put("foo", "bar");
    latch.await(10, TimeUnit.SECONDS);
    assertEquals(0, latch.getCount());

    AtomicCounter counter1 = atomix1.atomicCounterBuilder("test-counter")
        .withProtocol(DistributedLogProtocol.builder().build())
        .build();

    AtomicCounter counter2 = atomix2.atomicCounterBuilder("test-counter")
        .withProtocol(DistributedLogProtocol.builder().build())
        .build();

    assertEquals(1, counter1.incrementAndGet());
    assertEquals(1, counter1.get());
    Thread.sleep(1000);
    assertEquals(1, counter2.get());
    assertEquals(2, counter2.incrementAndGet());
  }

  @Test
  public void testStopStartConsensus() throws Exception {
    Atomix atomix1 = startAtomix(1, Arrays.asList(1), consensus(1, Arrays.asList(1))).get(30, TimeUnit.SECONDS);
    atomix1.stop().get(30, TimeUnit.SECONDS);
    try {
      atomix1.start().get(30, TimeUnit.SECONDS);
      fail("Expected ExecutionException");
    } catch (ExecutionException ex) {
      assertTrue(ex.getCause() instanceof IllegalStateException);
      assertEquals("Atomix instance shutdown", ex.getCause().getMessage());
    }
  }

  /**
   * Tests a client joining and leaving the cluster.
   */
  @Test
  public void testClientJoinLeaveConsensus() throws Exception {
    testClientJoinLeave(
        consensus(1, Arrays.asList(1, 2, 3)),
        consensus(2, Arrays.asList(1, 2, 3)),
        consensus(3, Arrays.asList(1, 2, 3)));
  }

  private void testClientJoinLeave(Function<AtomixBuilder, Atomix>... profiles) throws Exception {
    List<CompletableFuture<Atomix>> futures = new ArrayList<>();
    futures.add(startAtomix(1, Arrays.asList(1, 2, 3), profiles[0]));
    futures.add(startAtomix(2, Arrays.asList(1, 2, 3), profiles[1]));
    futures.add(startAtomix(3, Arrays.asList(1, 2, 3), profiles[2]));
    Futures.allOf(futures).get(30, TimeUnit.SECONDS);

    TestClusterMembershipEventListener dataListener = new TestClusterMembershipEventListener();
    instances.get(0).getMembershipService().addListener(dataListener);

    Atomix client1 = startAtomix(4, Arrays.asList(1, 2, 3)).get(30, TimeUnit.SECONDS);
    assertEquals(1, client1.getPartitionService().getPartitionGroups().size());

    // client1 added to data node
    ClusterMembershipEvent event1 = dataListener.event();
    assertEquals(ClusterMembershipEvent.Type.MEMBER_ADDED, event1.type());

    Thread.sleep(1000);

    TestClusterMembershipEventListener clientListener = new TestClusterMembershipEventListener();
    client1.getMembershipService().addListener(clientListener);

    Atomix client2 = startAtomix(5, Arrays.asList(1, 2, 3)).get(30, TimeUnit.SECONDS);
    assertEquals(1, client2.getPartitionService().getPartitionGroups().size());

    // client2 added to data node
    assertEquals(ClusterMembershipEvent.Type.MEMBER_ADDED, dataListener.event().type());

    // client2 added to client node
    assertEquals(ClusterMembershipEvent.Type.MEMBER_ADDED, clientListener.event().type());

    client2.stop().get(30, TimeUnit.SECONDS);

    // client2 removed from data node
    assertEquals(ClusterMembershipEvent.Type.REACHABILITY_CHANGED, dataListener.event().type());
    assertEquals(ClusterMembershipEvent.Type.MEMBER_REMOVED, dataListener.event().type());

    // client2 removed from client node
    assertEquals(ClusterMembershipEvent.Type.REACHABILITY_CHANGED, clientListener.event().type());
    assertEquals(ClusterMembershipEvent.Type.MEMBER_REMOVED, clientListener.event().type());
  }

  /**
   * Tests a client properties.
   */
  @Test
  public void testClientProperties() throws Exception {
    List<CompletableFuture<Atomix>> futures = new ArrayList<>();
    futures.add(startAtomix(1, Arrays.asList(1, 2, 3), consensus(1, Arrays.asList(1, 2, 3))));
    futures.add(startAtomix(2, Arrays.asList(1, 2, 3), consensus(2, Arrays.asList(1, 2, 3))));
    futures.add(startAtomix(3, Arrays.asList(1, 2, 3), consensus(3, Arrays.asList(1, 2, 3))));
    Futures.allOf(futures).get(30, TimeUnit.SECONDS);

    TestClusterMembershipEventListener dataListener = new TestClusterMembershipEventListener();
    instances.get(0).getMembershipService().addListener(dataListener);

    Properties properties = new Properties();
    properties.setProperty("a-key", "a-value");
    Atomix client1 = startAtomix(4, Arrays.asList(1, 2, 3), properties, AtomixBuilder::build).get(30, TimeUnit.SECONDS);
    assertEquals(1, client1.getPartitionService().getPartitionGroups().size());

    // client1 added to data node
    ClusterMembershipEvent event1 = dataListener.event();
    assertEquals(ClusterMembershipEvent.Type.MEMBER_ADDED, event1.type());

    Member member = event1.subject();

    assertNotNull(member.properties());
    assertEquals(1, member.properties().size());
    assertEquals("a-value", member.properties().get("a-key"));
  }

  @Test
  public void testPrimitiveGetters() throws Exception {
    List<CompletableFuture<Atomix>> futures = new ArrayList<>();
    futures.add(startAtomix(1, Arrays.asList(1, 2, 3), consensus(1, Arrays.asList(1, 2, 3))));
    futures.add(startAtomix(2, Arrays.asList(1, 2, 3), consensus(2, Arrays.asList(1, 2, 3))));
    futures.add(startAtomix(3, Arrays.asList(1, 2, 3), consensus(3, Arrays.asList(1, 2, 3))));
    Futures.allOf(futures).get(30, TimeUnit.SECONDS);

    Atomix atomix = startAtomix(4, Arrays.asList(1, 2, 3)).get(30, TimeUnit.SECONDS);

    assertEquals("a", atomix.atomicCounterBuilder("a").get().name());
    assertEquals(AtomicCounterType.instance(), atomix.atomicCounterBuilder("a").get().type());
    assertSame(atomix.atomicCounterBuilder("a").get(), atomix.atomicCounterBuilder("a").get());
    assertEquals(1, atomix.getPrimitives(AtomicCounterType.instance()).size());

    assertEquals("b", atomix.atomicMapBuilder("b").get().name());
    assertEquals(AtomicMapType.instance(), atomix.atomicMapBuilder("b").get().type());
    assertSame(atomix.atomicMapBuilder("b").get(), atomix.atomicMapBuilder("b").get());
    assertEquals(1, atomix.getPrimitives(AtomicMapType.instance()).size());

    assertEquals("e", atomix.atomicIdGeneratorBuilder("e").get().name());
    assertEquals(AtomicIdGeneratorType.instance(), atomix.atomicIdGeneratorBuilder("e").get().type());
    assertSame(atomix.atomicIdGeneratorBuilder("e").get(), atomix.atomicIdGeneratorBuilder("e").get());
    assertEquals(1, atomix.getPrimitives(AtomicIdGeneratorType.instance()).size());

    assertEquals("f", atomix.atomicLockBuilder("f").get().name());
    assertEquals(AtomicLockType.instance(), atomix.atomicLockBuilder("f").get().type());
    assertSame(atomix.atomicLockBuilder("f").get(), atomix.atomicLockBuilder("f").get());
    assertEquals(1, atomix.getPrimitives(AtomicLockType.instance()).size());

    assertEquals("k", atomix.atomicValueBuilder("k").get().name());
    assertEquals(AtomicValueType.instance(), atomix.atomicValueBuilder("k").get().type());
    assertSame(atomix.atomicValueBuilder("k").get(), atomix.atomicValueBuilder("k").get());
    assertEquals(1, atomix.getPrimitives(AtomicValueType.instance()).size());

    assertEquals("l", atomix.counterBuilder("l").get().name());
    assertEquals(DistributedCounterType.instance(), atomix.counterBuilder("l").get().type());
    assertSame(atomix.counterBuilder("l").get(), atomix.counterBuilder("l").get());
    assertEquals(1, atomix.getPrimitives(DistributedCounterType.instance()).size());

    assertEquals("q", atomix.lockBuilder("q").get().name());
    assertEquals(DistributedLockType.instance(), atomix.lockBuilder("q").get().type());
    assertSame(atomix.lockBuilder("q").get(), atomix.lockBuilder("q").get());
    assertEquals(1, atomix.getPrimitives(DistributedLockType.instance()).size());

    assertEquals("r", atomix.mapBuilder("r").get().name());
    assertEquals(DistributedMapType.instance(), atomix.mapBuilder("r").get().type());
    assertSame(atomix.mapBuilder("r").get(), atomix.mapBuilder("r").get());
    assertEquals(1, atomix.getPrimitives(DistributedMapType.instance()).size());

    assertEquals("y", atomix.setBuilder("y").get().name());
    assertEquals(DistributedSetType.instance(), atomix.setBuilder("y").get().type());
    assertSame(atomix.setBuilder("y").get(), atomix.setBuilder("y").get());
    assertEquals(1, atomix.getPrimitives(DistributedSetType.instance()).size());

    assertEquals("bb", atomix.valueBuilder("bb").get().name());
    assertEquals(DistributedValueType.instance(), atomix.valueBuilder("bb").get().type());
    assertSame(atomix.valueBuilder("bb").get(), atomix.valueBuilder("bb").get());
    assertEquals(1, atomix.getPrimitives(DistributedValueType.instance()).size());

    assertEquals(10, atomix.getPrimitives().size());
  }

  @Test
  public void testPrimitiveBuilders() throws Exception {
    List<CompletableFuture<Atomix>> futures = new ArrayList<>();
    futures.add(startAtomix(1, Arrays.asList(1, 2, 3), consensus(1, Arrays.asList(1, 2, 3))));
    futures.add(startAtomix(2, Arrays.asList(1, 2, 3), consensus(2, Arrays.asList(1, 2, 3))));
    futures.add(startAtomix(3, Arrays.asList(1, 2, 3), consensus(3, Arrays.asList(1, 2, 3))));
    Futures.allOf(futures).get(30, TimeUnit.SECONDS);

    Atomix atomix = startAtomix(4, Arrays.asList(1, 2, 3)).get(30, TimeUnit.SECONDS);

    assertEquals("a", atomix.atomicCounterBuilder("a").build().name());
    assertEquals(AtomicCounterType.instance(), atomix.atomicCounterBuilder("a").build().type());

    assertEquals("b", atomix.atomicMapBuilder("b").build().name());
    assertEquals(AtomicMapType.instance(), atomix.atomicMapBuilder("b").build().type());

    assertEquals("e", atomix.atomicIdGeneratorBuilder("e").build().name());
    assertEquals(AtomicIdGeneratorType.instance(), atomix.atomicIdGeneratorBuilder("e").build().type());

    assertEquals("f", atomix.atomicLockBuilder("f").build().name());
    assertEquals(AtomicLockType.instance(), atomix.atomicLockBuilder("f").build().type());

    assertEquals("k", atomix.atomicValueBuilder("k").build().name());
    assertEquals(AtomicValueType.instance(), atomix.atomicValueBuilder("k").build().type());

    assertEquals("l", atomix.counterBuilder("l").build().name());
    assertEquals(DistributedCounterType.instance(), atomix.counterBuilder("l").build().type());

    assertEquals("q", atomix.lockBuilder("q").build().name());
    assertEquals(DistributedLockType.instance(), atomix.lockBuilder("q").build().type());

    assertEquals("r", atomix.mapBuilder("r").build().name());
    assertEquals(DistributedMapType.instance(), atomix.mapBuilder("r").build().type());

    assertEquals("y", atomix.setBuilder("y").build().name());
    assertEquals(DistributedSetType.instance(), atomix.setBuilder("y").build().type());

    assertEquals("bb", atomix.valueBuilder("bb").build().name());
    assertEquals(DistributedValueType.instance(), atomix.valueBuilder("bb").build().type());
  }

  private Function<AtomixBuilder, Atomix> consensus(int memberId, List<Integer> members) {
    return builder ->
        builder.withManagementGroup(RaftPartitionGroup.builder("system")
            .withMembers(members.stream().map(String::valueOf).collect(Collectors.toList()))
            .withDataDirectory(new File(new File(DATA_DIR, "system"), String.valueOf(memberId)))
            .build())
            .addPartitionGroup(RaftPartitionGroup.builder("raft")
                .withMembers(members.stream().map(String::valueOf).collect(Collectors.toList()))
                .withDataDirectory(new File(new File(DATA_DIR, "raft"), String.valueOf(memberId)))
                .build())
            .build();
  }

  private static class TestClusterMembershipEventListener implements ClusterMembershipEventListener {
    private final BlockingQueue<ClusterMembershipEvent> queue = new LinkedBlockingQueue<>();

    @Override
    public void event(ClusterMembershipEvent event) {
      try {
        queue.put(event);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    public boolean eventReceived() {
      return !queue.isEmpty();
    }

    public ClusterMembershipEvent event() throws InterruptedException, TimeoutException {
      ClusterMembershipEvent event = queue.poll(15, TimeUnit.SECONDS);
      if (event == null) {
        throw new TimeoutException();
      }
      return event;
    }
  }
}
