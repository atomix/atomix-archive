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
package io.atomix.raft.storage.snapshot;

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
import java.util.UUID;

import io.atomix.raft.storage.RaftStorage;
import io.atomix.storage.StorageLevel;
import io.atomix.utils.time.WallClockTimestamp;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Snapshot store test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class SnapshotStoreTest {
  private String testId;

  /**
   * Returns a new snapshot store.
   */
  private SnapshotStore createSnapshotStore() {
    RaftStorage storage = RaftStorage.builder()
        .withPrefix("test")
        .withDirectory(new File(String.format("target/test-logs/%s", testId)))
        .withStorageLevel(StorageLevel.DISK)
        .build();
    return new SnapshotStore(storage);
  }

  /**
   * Tests storing and loading snapshots.
   */
  @Test
  public void testStoreLoadSnapshot() throws Exception {
    SnapshotStore store = createSnapshotStore();

    Snapshot snapshot = store.newSnapshot(2, new WallClockTimestamp());
    try (OutputStream writer = snapshot.openOutputStream()) {
      writer.write(new byte[]{1, 2, 3, 4});
      writer.write(new byte[]{2, 3, 4, 5});
      writer.write(new byte[]{3, 4, 5, 6});
      writer.write(new byte[]{4, 5, 6, 7});
      writer.flush();
    }
    snapshot.complete();
    assertNotNull(store.getSnapshot(2));
    store.close();

    store = createSnapshotStore();
    assertNotNull(store.getSnapshot(2));
    assertEquals(2, store.getSnapshot(2).index());

    try (InputStream reader = snapshot.openInputStream()) {
      byte[] bytes = new byte[1];
      assertNotEquals(-1, reader.read(bytes));
      assertEquals(1, bytes[0]);
    }
  }

  /**
   * Tests persisting and loading snapshots.
   */
  @Test
  public void testPersistLoadSnapshot() throws Exception {
    SnapshotStore store = createSnapshotStore();

    Snapshot snapshot = store.newSnapshot(2, new WallClockTimestamp());
    try (OutputStream writer = snapshot.openOutputStream()) {
      writer.write(new byte[]{1});
    }

    assertNull(store.getSnapshot(2));

    snapshot.complete();
    assertNotNull(store.getSnapshot(2));

    try (InputStream reader = snapshot.openInputStream()) {
      byte[] bytes = new byte[1];
      reader.read(bytes);
      assertEquals(1, bytes[0]);
    }

    store.close();

    store = createSnapshotStore();
    assertNotNull(store.getSnapshot(2));
    assertEquals(2, store.getSnapshot(2).index());

    snapshot = store.getSnapshot(2);
    try (InputStream reader = snapshot.openInputStream()) {
      byte[] bytes = new byte[1];
      reader.read(bytes);
      assertEquals(1, bytes[0]);
    }
  }

  /**
   * Tests writing multiple times to a snapshot designed to mimic chunked snapshots from leaders.
   */
  @Test
  public void testStreamSnapshot() throws Exception {
    SnapshotStore store = createSnapshotStore();

    Snapshot snapshot = store.newSnapshot(1, new WallClockTimestamp());
    for (byte i = 1; i <= 10; i++) {
      try (OutputStream writer = snapshot.openOutputStream()) {
        writer.write(new byte[]{i});
      }
    }
    snapshot.complete();

    snapshot = store.getSnapshot(1);
    try (InputStream reader = snapshot.openInputStream()) {
      byte[] bytes = new byte[1];
      for (byte i = 1; i <= 10; i++) {
        reader.read(bytes);
        assertEquals(i, bytes[0]);
      }
    }
  }

  /**
   * Tests writing a snapshot.
   */
  @Test
  public void testWriteSnapshotChunks() throws IOException {
    SnapshotStore store = createSnapshotStore();
    WallClockTimestamp timestamp = new WallClockTimestamp();
    Snapshot snapshot = store.newSnapshot(2, timestamp);
    assertEquals(2, snapshot.index());
    assertEquals(timestamp, snapshot.timestamp());

    assertNull(store.getSnapshot(2));

    try (OutputStream writer = snapshot.openOutputStream()) {
      writer.write(new byte[]{1});
    }

    assertNull(store.getSnapshot(2));

    try (OutputStream writer = snapshot.openOutputStream()) {
      writer.write(new byte[]{2});
    }

    assertNull(store.getSnapshot(2));

    try (OutputStream writer = snapshot.openOutputStream()) {
      writer.write(new byte[]{3});
    }

    assertNull(store.getSnapshot(2));
    snapshot.complete();

    assertEquals(2, store.getSnapshot(2).index());

    try (InputStream reader = store.getSnapshot(2).openInputStream()) {
      byte[] bytes = new byte[1];
      reader.read(bytes);
      assertEquals(1, bytes[0]);
      reader.read(bytes);
      assertEquals(2, bytes[0]);
      reader.read(bytes);
      assertEquals(3, bytes[0]);
    }
  }

  @Before
  @After
  public void cleanupStorage() throws IOException {
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
    testId = UUID.randomUUID().toString();
  }
}
