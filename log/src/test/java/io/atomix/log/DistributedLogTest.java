/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.log;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import io.atomix.log.impl.DefaultDistributedLogServer;
import io.atomix.log.protocol.TestLogProtocolFactory;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.serializer.serializers.DefaultSerializers;
import net.jodah.concurrentunit.ConcurrentTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Raft test.
 */
public class DistributedLogTest extends ConcurrentTestCase {
  private static final Serializer SERIALIZER = DefaultSerializers.BASIC;
  private volatile int memberId;
  private volatile int sessionId;
  private TestTermProviderFactory termProviderFactory;
  protected volatile List<String> nodes;
  protected volatile List<DistributedLogClient> clients = new ArrayList<>();
  protected volatile List<DistributedLogServer> servers = new ArrayList<>();
  protected volatile TestLogProtocolFactory protocolFactory;

  @Test
  public void testProducerConsumer() throws Throwable {
    createServers(3);
    DistributedLogClient client1 = createClient();
    DistributedLogClient client2 = createClient();

    client1.consumer().consume(1, record -> {
      threadAssertTrue(Arrays.equals("Hello world!".getBytes(), record.getValue().toByteArray()));
      resume();
    });
    client2.producer().append("Hello world!".getBytes());
    await(5000);
  }

  @Test
  public void testConsumeIndex() throws Throwable {
    createServers(3);
    DistributedLogClient client1 = createClient();
    DistributedLogClient client2 = createClient();

    for (int i = 1; i <= 10; i++) {
      client2.producer().append(String.valueOf(i).getBytes()).join();
    }

    client1.consumer().consume(10, record -> {
      threadAssertTrue(record.getIndex() == 10);
      threadAssertTrue(Arrays.equals("10".getBytes(), record.getValue().toByteArray()));
      resume();
    });

    await(5000);
  }

  @Test
  public void testConsumeAfterSizeCompact() throws Throwable {
    List<DistributedLogServer> servers = createServers(3);
    DistributedLogClient client1 = createClient();
    DistributedLogClient client2 = createClient();

    Predicate<List<DistributedLogServer>> predicate = s ->
        s.stream().map(sr -> ((DefaultDistributedLogServer) sr).context().journal().segments().size() > 2).reduce(Boolean::logicalOr).orElse(false);
    while (!predicate.test(servers)) {
      client1.producer().append(UUID.randomUUID().toString().getBytes());
    }
    servers.forEach(server -> ((DefaultDistributedLogServer) server).context().compact());

    client2.consumer().consume(1, record -> {
      threadAssertTrue(record.getIndex() > 1);
      resume();
    });
    await(5000);
  }

  @Test
  public void testConsumeAfterAgeCompact() throws Throwable {
    List<DistributedLogServer> servers = createServers(3);
    DistributedLogClient client1 = createClient();
    DistributedLogClient client2 = createClient();

    Predicate<List<DistributedLogServer>> predicate = s ->
        s.stream().map(sr -> ((DefaultDistributedLogServer) sr).context().journal().segments().size() > 1).reduce(Boolean::logicalOr).orElse(false);
    while (!predicate.test(servers)) {
      client1.producer().append(UUID.randomUUID().toString().getBytes());
    }
    Thread.sleep(1000);
    servers.forEach(server -> ((DefaultDistributedLogServer) server).context().compact());

    client2.consumer().consume(1, record -> {
      threadAssertTrue(record.getIndex() > 1);
      resume();
    });
    await(5000);
  }

  /**
   * Returns the next unique member identifier.
   *
   * @return The next unique member identifier.
   */
  private String nextMemberId() {
    return String.valueOf(++memberId);
  }

  /**
   * Creates a set of Raft servers.
   */
  private List<DistributedLogServer> createServers(int count) throws Throwable {
    List<DistributedLogServer> servers = new ArrayList<>();

    for (int i = 0; i < count; i++) {
      nodes.add(nextMemberId());
      DistributedLogServer server = createServer(nodes.get(i));
      server.start().thenRun(this::resume);
      servers.add(server);
    }

    await(5000, count);

    return servers;
  }

  /**
   * Creates a Raft server.
   */
  private DistributedLogServer createServer(String memberId) {
    DistributedLogServer server = DistributedLogServer.builder()
        .withServerId(memberId)
        .withProtocol(protocolFactory.newServerProtocol(memberId))
        .withTermProvider(termProviderFactory.newTermProvider(memberId))
        .withDirectory(new File("target/test-logs", memberId))
        .withMaxSegmentSize(1024 * 8)
        .withMaxLogSize(1024)
        .withMaxLogAge(Duration.ofMillis(10))
        .build();
    servers.add(server);
    return server;
  }

  /**
   * Creates a Raft client.
   */
  private DistributedLogClient createClient() throws Throwable {
    String memberId = nextMemberId();
    DistributedLogClient client = DistributedLogClient.builder()
        .withClientId(memberId)
        .withTermProvider(termProviderFactory.newTermProvider(memberId))
        .withProtocol(protocolFactory.newClientProtocol(memberId))
        .build();
    clients.add(client);
    return client;
  }

  @Before
  @After
  public void clearTests() throws Exception {
    Futures.allOf(servers.stream()
        .map(s -> s.stop().exceptionally(v -> null))
        .collect(Collectors.toList()))
        .get(30, TimeUnit.SECONDS);
    Futures.allOf(clients.stream()
        .map(c -> c.close().exceptionally(v -> null))
        .collect(Collectors.toList()))
        .get(30, TimeUnit.SECONDS);

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

    nodes = new ArrayList<>();
    memberId = 0;
    sessionId = 0;
    clients = new ArrayList<>();
    servers = new ArrayList<>();
    protocolFactory = new TestLogProtocolFactory();
    termProviderFactory = new TestTermProviderFactory();
  }

}
