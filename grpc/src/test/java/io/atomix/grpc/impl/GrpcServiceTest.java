/*
 * Copyright 2019-present Open Networking Foundation
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
package io.atomix.grpc.impl;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import io.atomix.cluster.Node;
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;
import io.atomix.core.Atomix;
import io.atomix.protocols.log.partition.LogPartitionGroup;
import io.atomix.protocols.raft.partition.RaftPartitionGroup;
import io.grpc.BindableService;
import io.grpc.Channel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.AbstractStub;
import org.junit.After;
import org.junit.Before;

/**
 * gRPC service test.
 */
public abstract class GrpcServiceTest<T extends AbstractStub<T>> {
  private List<Atomix> instances;
  private List<Server> servers;

  /**
   * Returns the Atomix instance with the given ID.
   *
   * @param instance the instance ID, a zero-based index ranging 0-2
   * @return the Atomix instance
   */
  protected Atomix atomix(int instance) {
    return instances.get(instance - 1);
  }

  /**
   * Returns a new instance of the service.
   *
   * @param atomix the Atomix instance
   * @return the service instance
   */
  protected abstract BindableService getService(Atomix atomix);

  /**
   * Returns a new stub for the given ID.
   *
   * @param instance the instance ID, ranging 1-3
   * @return a new stub
   */
  protected T getStub(int instance) {
    Atomix atomix = atomix(instance);
    return getStub(InProcessChannelBuilder.forName(atomix.getMembershipService().getLocalMember().id().id())
        .directExecutor().build());
  }

  /**
   * Creates a new Atomix client instance and stub.
   *
   * @return a new stub
   * @throws Exception if the Atomix instance could not be started
   */
  protected T createStub() throws Exception {
    Atomix atomix = buildClient(instances.size() + 1);
    instances.add(atomix);
    atomix.start().get(10, TimeUnit.SECONDS);
    Server server = InProcessServerBuilder.forName(atomix.getMembershipService().getLocalMember().id().id())
        .directExecutor()
        .addService(getService(atomix))
        .build()
        .start();
    servers.add(server);
    return getStub(instances.size());
  }

  /**
   * Returns a new stub using the given channel.
   *
   * @param channel the channel over which to execute the stub
   * @return a new stub
   */
  protected abstract T getStub(Channel channel);

  @Before
  public void beforeTest() throws Exception {
    deleteData();

    List<CompletableFuture<Atomix>> instanceFutures = new ArrayList<>(3);
    instances = new ArrayList<>(3);
    servers = new ArrayList<>(3);
    for (int i = 1; i <= 3; i++) {
      Atomix atomix = buildAtomix(i);
      instanceFutures.add(atomix.start().thenApply(v -> atomix));
      instances.add(atomix);
    }
    CompletableFuture.allOf(instanceFutures.toArray(new CompletableFuture[0])).get(30, TimeUnit.SECONDS);

    for (Atomix atomix : instances) {
      servers.add(InProcessServerBuilder.forName(atomix.getMembershipService().getLocalMember().id().id())
          .directExecutor()
          .addService(getService(atomix))
          .build()
          .start());
    }
  }

  @After
  public void afterTest() throws Exception {
    for (Server server : servers) {
      server.shutdownNow();
      server.awaitTermination();
    }

    List<CompletableFuture<Void>> instanceFutures = new ArrayList<>(3);
    for (Atomix instance : instances) {
      instanceFutures.add(instance.stop());
    }

    try {
      CompletableFuture.allOf(instanceFutures.toArray(new CompletableFuture[0])).get(30, TimeUnit.SECONDS);
    } finally {
      deleteData();
    }
  }

  protected Atomix buildAtomix(int memberId) {
    return Atomix.builder()
        .withClusterId("test")
        .withMemberId(String.valueOf(memberId))
        .withHost("localhost")
        .withPort(5000 + memberId)
        .withMembershipProvider(new BootstrapDiscoveryProvider(
            Node.builder()
                .withId("1")
                .withHost("localhost")
                .withPort(5001)
                .build(),
            Node.builder()
                .withId("2")
                .withHost("localhost")
                .withPort(5002)
                .build(),
            Node.builder()
                .withId("3")
                .withHost("localhost")
                .withPort(5003)
                .build()))
        .withManagementGroup(RaftPartitionGroup.builder("system")
            .withNumPartitions(1)
            .withPartitionSize(3)
            .withMembers("1", "2", "3")
            .withDataDirectory(new File("target/test-logs/" + memberId + "/system"))
            .build())
        .addPartitionGroup(RaftPartitionGroup.builder("data")
            .withNumPartitions(3)
            .withPartitionSize(3)
            .withMembers("1", "2", "3")
            .withDataDirectory(new File("target/test-logs/" + memberId + "/data"))
            .build())
        .addPartitionGroup(LogPartitionGroup.builder("log")
            .withNumPartitions(3)
            .withDataDirectory(new File("target/test-logs/" + memberId + "/log"))
            .withMaxSize(1024 * 1024)
            .build())
        .build();
  }

  protected Atomix buildClient(int memberId) {
    return Atomix.builder()
        .withClusterId("test")
        .withMemberId(String.valueOf(memberId))
        .withHost("localhost")
        .withPort(5000 + memberId)
        .withMembershipProvider(new BootstrapDiscoveryProvider(
            Node.builder()
                .withId("1")
                .withHost("localhost")
                .withPort(5001)
                .build(),
            Node.builder()
                .withId("2")
                .withHost("localhost")
                .withPort(5002)
                .build(),
            Node.builder()
                .withId("3")
                .withHost("localhost")
                .withPort(5003)
                .build()))
        .build();
  }

  protected static void deleteData() throws Exception {
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
  }
}
