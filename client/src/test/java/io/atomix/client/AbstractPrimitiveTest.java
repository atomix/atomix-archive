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
package io.atomix.client;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.atomix.primitive.protocol.ServiceProtocol;
import io.atomix.protocols.log.partition.LogPartitionGroup;
import io.atomix.protocols.raft.MultiRaftProtocol;
import io.atomix.protocols.raft.partition.RaftPartitionGroup;
import io.atomix.server.AtomixServer;
import org.junit.After;
import org.junit.Before;

/**
 * Base Atomix test.
 */
public abstract class AbstractPrimitiveTest {
  private AtomixServer server;
  private List<AtomixClient> clients;

  /**
   * Returns the primitive protocol with which to test.
   *
   * @return the protocol with which to test
   */
  protected ServiceProtocol protocol() {
    return MultiRaftProtocol.builder("raft").build();
  }

  /**
   * Returns a new Atomix instance.
   *
   * @return a new Atomix instance.
   */
  protected AtomixClient client() throws Exception {
    AtomixClient client = AtomixClient.builder()
        .withServer("localhost", 5000)
        .build();
    client.start().get(10, TimeUnit.SECONDS);
    return client;
  }

  @Before
  public void setupTest() throws Exception {
    deleteData();
    server = AtomixServer.builder()
        .withMemberId("test")
        .withHost("localhost")
        .withPort(5000)
        .withManagementGroup(RaftPartitionGroup.builder("system")
            .withMembers("test")
            .withNumPartitions(1)
            .withPartitionSize(1)
            .withDataDirectory(new File("target/test-data/system"))
            .build())
        .addPartitionGroup(RaftPartitionGroup.builder("raft")
            .withMembers("test")
            .withNumPartitions(3)
            .withPartitionSize(1)
            .withDataDirectory(new File("target/test-data/raft"))
            .build())
        .addPartitionGroup(LogPartitionGroup.builder("log")
            .withDataDirectory(new File("target/test-data/log"))
            .build())
        .build();
    clients = new CopyOnWriteArrayList<>();
  }

  @After
  public void teardownTest() throws Exception {
    List<CompletableFuture<Void>> futures = clients.stream().map(AtomixClient::stop).collect(Collectors.toList());
    try {
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).get(30, TimeUnit.SECONDS);
    } catch (Exception e) {
      // Do nothing
    }
    server.stop().get(30, TimeUnit.SECONDS);
    deleteData();
  }

  public void deleteData() throws IOException {
    if (Files.exists(Paths.get("target/test-data/"))) {
      Files.walkFileTree(Paths.get("target/test-data/"), new SimpleFileVisitor<Path>() {
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
  }
}
