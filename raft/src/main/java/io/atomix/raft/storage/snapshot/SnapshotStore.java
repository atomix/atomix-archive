/*
 * Copyright 2015-present Open Networking Foundation
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
 * limitations under the License
 */
package io.atomix.raft.storage.snapshot;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import io.atomix.raft.storage.RaftStorage;
import io.atomix.utils.time.WallClockTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Persists server snapshots via the {@link RaftStorage} module.
 * <p>
 * The server snapshot store is responsible for persisting periodic state machine snapshots according
 * to the configured {@link RaftStorage#storageLevel() storage level}. Each server with a snapshottable state machine
 * persists the state machine state to allow commands to be removed from disk.
 * <p>
 * When a snapshot store is {@link RaftStorage#openSnapshotStore() created}, the store will load any
 * existing snapshots from disk and make them available for reading. Only snapshots that have been
 * written and {@link Snapshot#complete() completed} will be read from disk. Incomplete snapshots are
 * automatically deleted from disk when the snapshot store is opened.
 * <p>
 * <pre>
 *   {@code
 *   SnapshotStore snapshots = storage.openSnapshotStore("test");
 *   Snapshot snapshot = snapshots.snapshot(1);
 *   }
 * </pre>
 * To create a new {@link Snapshot}, use the {@link #newSnapshot(long, WallClockTimestamp)} method. Each snapshot must
 * be created with a unique {@code index} which represents the index of the server state machine at
 * the point at which the snapshot was taken. Snapshot indices are used to sort snapshots loaded from
 * disk and apply them at the correct point in the state machine.
 * <p>
 * <pre>
 *   {@code
 *   Snapshot snapshot = snapshots.create(10);
 *   try (SnapshotWriter writer = snapshot.writer()) {
 *     ...
 *   }
 *   snapshot.complete();
 *   }
 * </pre>
 * Snapshots don't necessarily represent the beginning of the log. Typical Raft implementations take a
 * snapshot of the state machine state and then clear their logs up to that point. However, in Raft
 * a snapshot may actually only represent a subset of the state machine's state.
 */
public class SnapshotStore implements AutoCloseable {
  private final Logger log = LoggerFactory.getLogger(getClass());
  final RaftStorage storage;
  private final NavigableMap<Long, Snapshot> snapshots = new ConcurrentSkipListMap<>();

  public SnapshotStore(RaftStorage storage) {
    this.storage = checkNotNull(storage, "storage cannot be null");
    open();
  }

  /**
   * Opens the snapshot manager.
   */
  private void open() {
    for (Snapshot snapshot : loadSnapshots()) {
      completeSnapshot(snapshot);
    }
  }

  /**
   * Returns the current snapshot.
   *
   * @return the current snapshot
   */
  public Snapshot getCurrentSnapshot() {
    Map.Entry<Long, Snapshot> entry = snapshots.lastEntry();
    return entry != null ? entry.getValue() : null;
  }

  /**
   * Returns the snapshot at the given index.
   *
   * @param index the index for which to lookup the snapshot
   * @return the snapshot at the given index or {@code null} if the snapshot doesn't exist
   */
  public Snapshot getSnapshot(long index) {
    return snapshots.get(index);
  }

  /**
   * Loads all available snapshots from disk.
   *
   * @return A list of available snapshots.
   */
  private Collection<Snapshot> loadSnapshots() {
    // Ensure log directories are created.
    storage.directory().mkdirs();

    List<Snapshot> snapshots = new ArrayList<>();

    // Iterate through all files in the log directory.
    for (File file : storage.directory().listFiles(File::isFile)) {

      // If the file looks like a segment file, attempt to load the segment.
      if (SnapshotFile.isSnapshotFile(file)) {
        SnapshotFile snapshotFile = new SnapshotFile(file);
        SnapshotDescriptor descriptor = null;
        try (InputStream is = new FileInputStream(file)) {
          descriptor = SnapshotDescriptor.parseDelimitedFrom(is);
        } catch (IOException e) {
        }

        if (descriptor != null) {
          log.debug("Loaded disk snapshot: {} ({})", descriptor.getIndex(), snapshotFile.file().getName());
          snapshots.add(new Snapshot(snapshotFile, descriptor, this));
        }
      } else if (SnapshotFile.isLockedSnapshotFile(file)) {
        log.debug("Deleting partial snapshot: {}", file.getName());
        try {
          Files.delete(file.toPath());
        } catch (IOException e) {
        }
      }
    }

    return snapshots;
  }

  /**
   * Creates a new snapshot.
   *
   * @param index     The snapshot index.
   * @param timestamp The snapshot timestamp.
   * @return The snapshot.
   */
  public Snapshot newSnapshot(long index, WallClockTimestamp timestamp) {
    SnapshotDescriptor descriptor = SnapshotDescriptor.newBuilder()
        .setIndex(index)
        .setTimestamp(timestamp.unixTimestamp())
        .build();
    SnapshotFile file = new SnapshotFile(SnapshotFile.createSnapshotFile(
        storage.directory(),
        storage.prefix(),
        descriptor.getIndex()));
    return new Snapshot(file, descriptor, this);
  }

  /**
   * Completes writing a snapshot.
   */
  protected synchronized void completeSnapshot(Snapshot snapshot) {
    checkNotNull(snapshot, "snapshot cannot be null");

    Map.Entry<Long, Snapshot> lastEntry = snapshots.lastEntry();
    if (lastEntry == null) {
      snapshots.put(snapshot.index(), snapshot);
    } else if (lastEntry.getValue().index() < snapshot.index()) {
      snapshots.put(snapshot.index(), snapshot);
      Snapshot lastSnapshot = lastEntry.getValue();
      lastSnapshot.close();
      lastSnapshot.delete();
    } else if (storage.isRetainStaleSnapshots()) {
      snapshots.put(snapshot.index(), snapshot);
    } else {
      snapshot.close();
      snapshot.delete();
    }
  }

  @Override
  public void close() {
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

}
