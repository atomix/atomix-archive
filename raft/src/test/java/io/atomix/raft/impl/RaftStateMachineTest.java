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
package io.atomix.raft.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import io.atomix.raft.RaftCommand;
import io.atomix.raft.RaftQuery;
import io.atomix.raft.RaftServer;
import io.atomix.raft.RaftStateMachine;
import io.atomix.raft.protocol.RaftServerProtocol;
import io.atomix.raft.storage.RaftStorage;
import io.atomix.raft.storage.log.InitializeEntry;
import io.atomix.raft.storage.log.RaftLogEntry;
import io.atomix.raft.storage.log.RaftLogWriter;
import io.atomix.raft.storage.snapshot.Snapshot;
import io.atomix.utils.stream.StreamHandler;
import io.atomix.utils.concurrent.ThreadModel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Raft service manager test.
 */
public class RaftStateMachineTest {
  private static final Path PATH = Paths.get("target/test-logs/");

  private RaftContext raft;
  private AtomicBoolean snapshotTaken;
  private AtomicBoolean snapshotInstalled;

  @Test
  public void testSnapshotTakeInstall() throws Exception {
    RaftLogWriter writer = raft.getLogWriter();
    writer.append(RaftLogEntry.newBuilder()
        .setTerm(1)
        .setTimestamp(System.currentTimeMillis())
        .setInitialize(InitializeEntry.newBuilder().build())
        .build());
    writer.append(RaftLogEntry.newBuilder()
        .setTerm(1)
        .setTimestamp(System.currentTimeMillis())
        .setInitialize(InitializeEntry.newBuilder().build())
        .build());
    writer.commit(2);

    RaftStateMachineManager manager = raft.getServiceManager();

    manager.apply(2).join();

    Snapshot snapshot = manager.snapshot();
    assertEquals(2, snapshot.index());
    assertTrue(snapshotTaken.get());

    snapshot = snapshot.complete();

    assertEquals(2, raft.getSnapshotStore().getCurrentSnapshot().index());

    manager.install(snapshot);
    assertTrue(snapshotInstalled.get());
  }

  @Test
  public void testInstallSnapshotOnApply() throws Exception {
    RaftLogWriter writer = raft.getLogWriter();
    writer.append(RaftLogEntry.newBuilder()
        .setTerm(1)
        .setTimestamp(System.currentTimeMillis())
        .setInitialize(InitializeEntry.newBuilder().build())
        .build());
    writer.append(RaftLogEntry.newBuilder()
        .setTerm(1)
        .setTimestamp(System.currentTimeMillis())
        .setInitialize(InitializeEntry.newBuilder().build())
        .build());
    writer.commit(2);

    RaftStateMachineManager manager = raft.getServiceManager();

    manager.apply(2).join();

    Snapshot snapshot = manager.snapshot();
    assertEquals(2, snapshot.index());
    assertTrue(snapshotTaken.get());

    snapshot.complete();

    assertEquals(2, raft.getSnapshotStore().getCurrentSnapshot().index());

    writer.append(RaftLogEntry.newBuilder()
        .setTerm(1)
        .setTimestamp(System.currentTimeMillis())
        .setInitialize(InitializeEntry.newBuilder().build())
        .build());
    writer.commit(3);

    manager.apply(3).join();
    assertTrue(snapshotInstalled.get());
  }

  @Before
  public void setupContext() throws IOException {
    deleteStorage();

    RaftStorage storage = RaftStorage.builder()
        .withPrefix("test")
        .withDirectory(PATH.toFile())
        .build();
    raft = new RaftContext(
        "test",
        "test-1",
        mock(RaftServerProtocol.class),
        new TestStateMachine(),
        storage,
        ThreadModel.SHARED_THREAD_POOL.factory("raft-server-test-%d", 1, LoggerFactory.getLogger(RaftServer.class)),
        true);

    snapshotTaken = new AtomicBoolean();
    snapshotInstalled = new AtomicBoolean();
  }

  @After
  public void teardownContext() throws IOException {
    raft.close();
    deleteStorage();
  }

  private void deleteStorage() throws IOException {
    if (Files.exists(PATH)) {
      Files.walkFileTree(PATH, new SimpleFileVisitor<Path>() {
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

  private class TestStateMachine implements RaftStateMachine {
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
}
