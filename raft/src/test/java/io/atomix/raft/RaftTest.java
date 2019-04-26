/*
 * Copyright 2019-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.raft;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import io.atomix.raft.cluster.RaftClusterEvent;
import io.atomix.raft.cluster.RaftMember;
import io.atomix.raft.protocol.TestRaftProtocolFactory;
import io.atomix.raft.storage.RaftStorage;
import io.atomix.storage.StorageLevel;
import io.atomix.utils.stream.StreamHandler;
import io.atomix.utils.concurrent.SingleThreadContext;
import io.atomix.utils.concurrent.ThreadContext;
import net.jodah.concurrentunit.ConcurrentTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Raft test.
 */
public class RaftTest extends ConcurrentTestCase {
  protected volatile int nextId;
  protected volatile List<RaftMember> members;
  protected volatile List<RaftClient> clients = new ArrayList<>();
  protected volatile List<RaftServer> servers = new ArrayList<>();
  protected volatile TestRaftProtocolFactory protocolFactory;
  protected volatile ThreadContext context;

  /**
   * Tests starting several members individually.
   */
  @Test
  public void testSingleMemberStart() throws Throwable {
    RaftServer server = createServers(1).get(0);
    server.bootstrap().thenRun(this::resume);
    await(10000);
    RaftServer joiner1 = createServer(nextNodeId());
    joiner1.join(server.cluster().getMember().memberId()).thenRun(this::resume);
    await(10000);
    RaftServer joiner2 = createServer(nextNodeId());
    joiner2.join(server.cluster().getMember().memberId()).thenRun(this::resume);
    await(10000);
  }

  /**
   * Tests joining a server after many entries have been committed.
   */
  @Test
  public void testActiveJoinLate() throws Throwable {
    testServerJoinLate(RaftMember.Type.ACTIVE, RaftServer.Role.FOLLOWER);
  }

  /**
   * Tests joining a server after many entries have been committed.
   */
  @Test
  public void testPassiveJoinLate() throws Throwable {
    testServerJoinLate(RaftMember.Type.PASSIVE, RaftServer.Role.PASSIVE);
  }

  /**
   * Tests joining a server after many entries have been committed.
   */
  private void testServerJoinLate(RaftMember.Type type, RaftServer.Role role) throws Throwable {
    createServers(3);
    RaftClient client = createClient();
    submit(client, 0, 100);
    await(15000);
    RaftServer joiner = createServer(nextNodeId());
    joiner.addRoleChangeListener(s -> {
      if (s == role) {
        resume();
      }
    });
    if (type == RaftMember.Type.ACTIVE) {
      joiner.join(members.stream().map(RaftMember::memberId).collect(Collectors.toList())).thenRun(this::resume);
    } else {
      joiner.listen(members.stream().map(RaftMember::memberId).collect(Collectors.toList())).thenRun(this::resume);
    }
    await(15000, 2);
    submit(client, 0, 10);
    await(15000);
    Thread.sleep(5000);
  }

  /**
   * Submits a bunch of commands recursively.
   */
  private void submit(RaftClient client, int count, int total) {
    if (count < total) {
      client.write("Hello world!".getBytes()).whenComplete((result, error) -> {
        threadAssertNull(error);
        submit(client, count + 1, total);
      });
    } else {
      resume();
    }
  }

  /**
   * Tests transferring leadership.
   */
  @Test
  @Ignore
  public void testTransferLeadership() throws Throwable {
    List<RaftServer> servers = createServers(3);
    RaftClient client = createClient();
    submit(client, 0, 1000);
    RaftServer follower = servers.stream()
        .filter(RaftServer::isFollower)
        .findFirst()
        .get();
    follower.promote().thenRun(this::resume);
    await(15000, 2);
    assertTrue(follower.isLeader());
  }

  /**
   * Tests joining a server to an existing cluster.
   */
  @Test
  public void testCrashRecover() throws Throwable {
    List<RaftServer> servers = createServers(3);
    RaftClient client = createClient();
    submit(client, 0, 100);
    await(30000);
    Thread.sleep(15000);
    servers.get(0).shutdown().get(10, TimeUnit.SECONDS);
    RaftServer server = createServer(members.get(0).memberId());
    server.join(members.stream().map(RaftMember::memberId).collect(Collectors.toList())).thenRun(this::resume);
    await(30000);
    submit(client, 0, 100);
    await(30000);
  }

  /**
   * Tests leaving a sever from a cluster.
   */
  @Test
  public void testServerLeave() throws Throwable {
    List<RaftServer> servers = createServers(3);
    RaftServer server = servers.get(0);
    server.leave().thenRun(this::resume);
    await(30000);
  }

  /**
   * Tests leaving the leader from a cluster.
   */
  @Test
  public void testLeaderLeave() throws Throwable {
    List<RaftServer> servers = createServers(3);
    RaftServer server = servers.stream().filter(s -> s.getRole() == RaftServer.Role.LEADER).findFirst().get();
    server.leave().thenRun(this::resume);
    await(30000);
  }

  /**
   * Tests an active member joining the cluster.
   */
  @Test
  public void testActiveJoin() throws Throwable {
    testServerJoin(RaftMember.Type.ACTIVE);
  }

  /**
   * Tests a passive member joining the cluster.
   */
  @Test
  public void testPassiveJoin() throws Throwable {
    testServerJoin(RaftMember.Type.PASSIVE);
  }

  /**
   * Tests a server joining the cluster.
   */
  private void testServerJoin(RaftMember.Type type) throws Throwable {
    createServers(3);
    RaftServer server = createServer(nextNodeId());
    if (type == RaftMember.Type.ACTIVE) {
      server.join(members.stream().map(RaftMember::memberId).collect(Collectors.toList())).thenRun(this::resume);
    } else {
      server.listen(members.stream().map(RaftMember::memberId).collect(Collectors.toList())).thenRun(this::resume);
    }
    await(15000);
  }

  /**
   * Tests joining and leaving the cluster, resizing the quorum.
   */
  @Test
  public void testResize() throws Throwable {
    RaftServer server = createServers(1).get(0);
    RaftServer joiner = createServer(nextNodeId());
    joiner.join(members.stream().map(RaftMember::memberId).collect(Collectors.toList())).thenRun(this::resume);
    await(15000);
    server.leave().thenRun(this::resume);
    await(15000);
    joiner.leave().thenRun(this::resume);
  }

  /**
   * Tests an active member join event.
   */
  @Test
  public void testActiveJoinEvent() throws Throwable {
    testJoinEvent(RaftMember.Type.ACTIVE);
  }

  /**
   * Tests a passive member join event.
   */
  @Test
  public void testPassiveJoinEvent() throws Throwable {
    testJoinEvent(RaftMember.Type.PASSIVE);
  }

  /**
   * Tests a member join event.
   */
  private void testJoinEvent(RaftMember.Type type) throws Throwable {
    List<RaftServer> servers = createServers(3);

    RaftMember member = nextMember(type);

    RaftServer server = servers.get(0);
    server.cluster().addListener(event -> {
      if (event.type() == RaftClusterEvent.Type.JOIN) {
        threadAssertEquals(event.subject().memberId(), member.memberId());
        resume();
      }
    });

    RaftServer joiner = createServer(member.memberId());
    if (type == RaftMember.Type.ACTIVE) {
      joiner.join(members.stream().map(RaftMember::memberId).collect(Collectors.toList())).thenRun(this::resume);
    } else {
      joiner.listen(members.stream().map(RaftMember::memberId).collect(Collectors.toList())).thenRun(this::resume);
    }
    await(15000, 2);
  }

  /**
   * Tests demoting the leader.
   */
  @Test
  public void testDemoteLeader() throws Throwable {
    List<RaftServer> servers = createServers(3);

    RaftServer leader = servers.stream()
        .filter(s -> s.cluster().getMember().equals(s.cluster().getLeader()))
        .findFirst()
        .get();

    RaftServer follower = servers.stream()
        .filter(s -> !s.cluster().getMember().equals(s.cluster().getLeader()))
        .findFirst()
        .get();

    follower.cluster().getMember(leader.cluster().getMember().memberId()).addTypeChangeListener(t -> {
      threadAssertEquals(t, RaftMember.Type.PASSIVE);
      resume();
    });
    leader.cluster().getMember().demote(RaftMember.Type.PASSIVE).thenRun(this::resume);
    await(15000, 2);
  }

  /**
   * Tests submitting a command.
   */
  @Test
  public void testOneNodeSubmitCommand() throws Throwable {
    testSubmitCommand(1);
  }

  /**
   * Tests submitting a command.
   */
  @Test
  public void testTwoNodeSubmitCommand() throws Throwable {
    testSubmitCommand(2);
  }

  /**
   * Tests submitting a command.
   */
  @Test
  public void testThreeNodeSubmitCommand() throws Throwable {
    testSubmitCommand(3);
  }

  /**
   * Tests submitting a command.
   */
  @Test
  public void testFourNodeSubmitCommand() throws Throwable {
    testSubmitCommand(4);
  }

  /**
   * Tests submitting a command.
   */
  @Test
  public void testFiveNodeSubmitCommand() throws Throwable {
    testSubmitCommand(5);
  }

  /**
   * Tests submitting a command with a configured consistency level.
   */
  private void testSubmitCommand(int nodes) throws Throwable {
    createServers(nodes);

    RaftClient client = createClient();
    client.write("Hello world!".getBytes()).thenRun(this::resume);

    await(5000);
  }

  /**
   * Tests submitting a command.
   */
  @Test
  public void testTwoOfThreeNodeSubmitCommand() throws Throwable {
    testSubmitCommand(2, 3);
  }

  @Test
  public void testNodeCatchUpAfterCompaction() throws Throwable {
    // given
    createServers(3);

    servers.get(0).shutdown();
    RaftClient client = createClient();

    final int entries = 10;
    final int entrySize = 1024;
    for (int i = 0; i < entries; i++) {
      client.write("Hello world!".getBytes())
          .get(1_000, TimeUnit.MILLISECONDS);
    }

    // when
    CompletableFuture
        .allOf(servers.get(1).compact(),
            servers.get(2).compact())
        .get(15_000, TimeUnit.MILLISECONDS);

    // then
    final RaftServer server = createServer(members.get(0).memberId());
    List<String> members =
        this.members
            .stream()
            .map(RaftMember::memberId)
            .collect(Collectors.toList());

    server.join(members).get(15_000, TimeUnit.MILLISECONDS);
  }

  /**
   * Tests submitting a command.
   */
  @Test
  public void testThreeOfFourNodeSubmitCommand() throws Throwable {
    testSubmitCommand(3, 4);
  }

  /**
   * Tests submitting a command.
   */
  @Test
  public void testThreeOfFiveNodeSubmitCommand() throws Throwable {
    testSubmitCommand(3, 5);
  }

  /**
   * Tests submitting a command to a partial cluster.
   */
  private void testSubmitCommand(int live, int total) throws Throwable {
    createServers(live, total);

    RaftClient client = createClient();
    client.write("Hello world!".getBytes()).thenRun(this::resume);

    await(30000);
  }

  /**
   * Tests submitting a query.
   */
  @Test
  public void testOneNodeSubmitQueryWithSequentialConsistency() throws Throwable {
    testSubmitQuery(1, ReadConsistency.SEQUENTIAL);
  }

  /**
   * Tests submitting a query.
   */
  @Test
  public void testOneNodeSubmitQueryWithBoundedLinearizableConsistency() throws Throwable {
    testSubmitQuery(1, ReadConsistency.LINEARIZABLE_LEASE);
  }

  /**
   * Tests submitting a query.
   */
  @Test
  public void testOneNodeSubmitQueryWithLinearizableConsistency() throws Throwable {
    testSubmitQuery(1, ReadConsistency.LINEARIZABLE);
  }

  /**
   * Tests submitting a query.
   */
  @Test
  public void testTwoNodeSubmitQueryWithSequentialConsistency() throws Throwable {
    testSubmitQuery(2, ReadConsistency.SEQUENTIAL);
  }

  /**
   * Tests submitting a query.
   */
  @Test
  public void testTwoNodeSubmitQueryWithBoundedLinearizableConsistency() throws Throwable {
    testSubmitQuery(2, ReadConsistency.LINEARIZABLE_LEASE);
  }

  /**
   * Tests submitting a query.
   */
  @Test
  public void testTwoNodeSubmitQueryWithLinearizableConsistency() throws Throwable {
    testSubmitQuery(2, ReadConsistency.LINEARIZABLE);
  }

  /**
   * Tests submitting a query.
   */
  @Test
  public void testThreeNodeSubmitQueryWithSequentialConsistency() throws Throwable {
    testSubmitQuery(3, ReadConsistency.SEQUENTIAL);
  }

  /**
   * Tests submitting a query.
   */
  @Test
  public void testThreeNodeSubmitQueryWithBoundedLinearizableConsistency() throws Throwable {
    testSubmitQuery(3, ReadConsistency.LINEARIZABLE_LEASE);
  }

  /**
   * Tests submitting a query.
   */
  @Test
  public void testThreeNodeSubmitQueryWithLinearizableConsistency() throws Throwable {
    testSubmitQuery(3, ReadConsistency.LINEARIZABLE);
  }

  /**
   * Tests submitting a query.
   */
  @Test
  public void testFourNodeSubmitQueryWithSequentialConsistency() throws Throwable {
    testSubmitQuery(4, ReadConsistency.SEQUENTIAL);
  }

  /**
   * Tests submitting a query.
   */
  @Test
  public void testFourNodeSubmitQueryWithBoundedLinearizableConsistency() throws Throwable {
    testSubmitQuery(4, ReadConsistency.LINEARIZABLE_LEASE);
  }

  /**
   * Tests submitting a query.
   */
  @Test
  public void testFourNodeSubmitQueryWithLinearizableConsistency() throws Throwable {
    testSubmitQuery(4, ReadConsistency.LINEARIZABLE);
  }

  /**
   * Tests submitting a query.
   */
  @Test
  public void testFiveNodeSubmitQueryWithSequentialConsistency() throws Throwable {
    testSubmitQuery(5, ReadConsistency.SEQUENTIAL);
  }

  /**
   * Tests submitting a query.
   */
  @Test
  public void testFiveNodeSubmitQueryWithBoundedLinearizableConsistency() throws Throwable {
    testSubmitQuery(5, ReadConsistency.LINEARIZABLE_LEASE);
  }

  /**
   * Tests submitting a query.
   */
  @Test
  public void testFiveNodeSubmitQueryWithLinearizableConsistency() throws Throwable {
    testSubmitQuery(5, ReadConsistency.LINEARIZABLE);
  }

  /**
   * Tests submitting a query with a configured consistency level.
   */
  private void testSubmitQuery(int nodes, ReadConsistency consistency) throws Throwable {
    createServers(nodes);

    RaftClient client = createClient();
    client.read(new byte[0]).thenRun(this::resume);

    await(30000);
  }

  /**
   * Returns the next unique member identifier.
   *
   * @return The next unique member identifier.
   */
  private String nextNodeId() {
    return String.valueOf(++nextId);
  }

  /**
   * Returns the next server address.
   *
   * @param type The startup member type.
   * @return The next server address.
   */
  private RaftMember nextMember(RaftMember.Type type) {
    return new TestMember(nextNodeId(), type);
  }

  /**
   * Creates a set of Raft servers.
   */
  private List<RaftServer> createServers(int nodes) throws Throwable {
    List<RaftServer> servers = new ArrayList<>();

    for (int i = 0; i < nodes; i++) {
      members.add(nextMember(RaftMember.Type.ACTIVE));
    }

    for (int i = 0; i < nodes; i++) {
      RaftServer server = createServer(members.get(i).memberId());
      if (members.get(i).getType() == RaftMember.Type.ACTIVE) {
        server.bootstrap(members.stream().map(RaftMember::memberId).collect(Collectors.toList())).thenRun(this::resume);
      } else {
        server.listen(members.stream().map(RaftMember::memberId).collect(Collectors.toList())).thenRun(this::resume);
      }
      servers.add(server);
    }

    await(30000 * nodes, nodes);

    return servers;
  }

  /**
   * Creates a set of Raft servers.
   */
  private List<RaftServer> createServers(int live, int total) throws Throwable {
    List<RaftServer> servers = new ArrayList<>();

    for (int i = 0; i < total; i++) {
      members.add(nextMember(RaftMember.Type.ACTIVE));
    }

    for (int i = 0; i < live; i++) {
      RaftServer server = createServer(members.get(i).memberId());
      if (members.get(i).getType() == RaftMember.Type.ACTIVE) {
        server.bootstrap(members.stream().map(RaftMember::memberId).collect(Collectors.toList())).thenRun(this::resume);
      } else {
        server.listen(members.stream().map(RaftMember::memberId).collect(Collectors.toList())).thenRun(this::resume);
      }
      servers.add(server);
    }

    await(30000 * live, live);

    return servers;
  }

  /**
   * Creates a Raft server.
   */
  private RaftServer createServer(String memberId) {
    RaftServer.Builder builder = RaftServer.builder(memberId)
        .withProtocol(protocolFactory.newServerProtocol(memberId))
        .withStateMachine(new TestStateMachine())
        .withStorage(RaftStorage.builder()
            .withStorageLevel(StorageLevel.DISK)
            .withDirectory(new File(String.format("target/test-logs/%s", memberId)))
            .withMaxSegmentSize(1024 * 10)
            .withMaxEntriesPerSegment(10)
            .build());

    RaftServer server = builder.build();

    servers.add(server);
    return server;
  }

  /**
   * Creates a Raft client.
   */
  private RaftClient createClient() throws Throwable {
    RaftClient client = RaftClient.builder()
        .withProtocol(protocolFactory.newClientProtocol())
        .build();
    client.connect(members.stream().map(RaftMember::memberId).collect(Collectors.toList())).thenRun(this::resume);
    await(30000);
    clients.add(client);
    return client;
  }

  @Before
  @After
  public void clearTests() throws Exception {
    clients.forEach(c -> {
      try {
        c.close().get(10, TimeUnit.SECONDS);
      } catch (Exception e) {
      }
    });

    servers.forEach(s -> {
      try {
        if (s.isRunning()) {
          s.shutdown().get(10, TimeUnit.SECONDS);
        }
      } catch (Exception e) {
      }
    });

    Path directory = Paths.get("target/test-logs/");
    if (Files.exists(directory)) {
      Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
          Files.delete(file);
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
          Files.delete(dir);
          return FileVisitResult.CONTINUE;
        }
      });
    }

    if (context != null) {
      context.close();
    }

    members = new ArrayList<>();
    nextId = 0;
    clients = new ArrayList<>();
    servers = new ArrayList<>();
    context = new SingleThreadContext("raft-test-messaging-%d");
    protocolFactory = new TestRaftProtocolFactory(context);
  }

  public static class TestStateMachine implements RaftStateMachine {
    @Override
    public void init(Context context) {

    }

    @Override
    public void snapshot(OutputStream output) {

    }

    @Override
    public void install(InputStream input) {

    }

    @Override
    public boolean canDelete(long index) {
      return false;
    }

    @Override
    public CompletableFuture<byte[]> apply(RaftCommand command) {
      return null;
    }

    @Override
    public CompletableFuture<Void> apply(RaftCommand command, StreamHandler<byte[]> handler) {
      return null;
    }

    @Override
    public CompletableFuture<byte[]> apply(RaftQuery query) {
      return null;
    }

    @Override
    public CompletableFuture<Void> apply(RaftQuery query, StreamHandler<byte[]> handler) {
      return null;
    }
  }

  /**
   * Test member.
   */
  public static class TestMember implements RaftMember {
    private final String memberId;
    private final Type type;

    TestMember(String memberId, Type type) {
      this.memberId = memberId;
      this.type = type;
    }

    @Override
    public String memberId() {
      return memberId;
    }

    @Override
    public Type getType() {
      return type;
    }

    @Override
    public int hash() {
      return 0;
    }

    @Override
    public void addTypeChangeListener(Consumer<Type> listener) {

    }

    @Override
    public void removeTypeChangeListener(Consumer<Type> listener) {

    }

    @Override
    public Instant getLastUpdated() {
      return null;
    }

    @Override
    public CompletableFuture<Void> promote() {
      return null;
    }

    @Override
    public CompletableFuture<Void> promote(Type type) {
      return null;
    }

    @Override
    public CompletableFuture<Void> demote() {
      return null;
    }

    @Override
    public CompletableFuture<Void> demote(Type type) {
      return null;
    }

    @Override
    public CompletableFuture<Void> remove() {
      return null;
    }
  }

}
