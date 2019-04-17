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

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Objects;

import io.atomix.utils.time.WallClockTimestamp;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;

/**
 * Manages reading and writing a single snapshot file.
 */
public class Snapshot implements AutoCloseable {
  private final SnapshotFile file;
  private final SnapshotDescriptor descriptor;
  private final SnapshotStore store;

  protected Snapshot(SnapshotFile file, SnapshotDescriptor descriptor, SnapshotStore store) {
    this.file = checkNotNull(file, "file cannot be null");
    this.descriptor = checkNotNull(descriptor, "descriptor cannot be null");
    this.store = checkNotNull(store, "store cannot be null");
  }

  /**
   * Returns the snapshot index.
   * <p>
   * The snapshot index is the index of the state machine at the point at which the snapshot was written.
   *
   * @return The snapshot index.
   */
  public long index() {
    return descriptor.getIndex();
  }

  /**
   * Returns the snapshot timestamp.
   * <p>
   * The timestamp is the wall clock time at the {@link #index()} at which the snapshot was taken.
   *
   * @return The snapshot timestamp.
   */
  public WallClockTimestamp timestamp() {
    return WallClockTimestamp.from(descriptor.getTimestamp());
  }

  /**
   * Opens a new snapshot output stream.
   *
   * @return A new snapshot output stream.
   * @throws IllegalStateException if a writer was already created or the snapshot is {@link #complete() complete}
   */
  public OutputStream openOutputStream() throws IOException {
    checkState(!file.file().exists(), "Cannot write to completed snapshot");
    OutputStream outputStream;
    if (file.temporaryFile().exists()) {
      outputStream = new FileOutputStream(file.temporaryFile(), true);
    } else {
      outputStream = new FileOutputStream(file.temporaryFile());
      descriptor.writeDelimitedTo(outputStream);
    }
    return outputStream;
  }

  /**
   * Opens a new snapshot input stream.
   *
   * @return A new snapshot reader.
   * @throws IllegalStateException if the snapshot is not {@link #complete() complete}
   */
  public InputStream openInputStream() throws IOException {
    checkState(file.file().exists(), "missing snapshot file: %s", file.file());
    InputStream inputStream = new FileInputStream(file.file());
    SnapshotDescriptor.parseDelimitedFrom(inputStream);
    checkState(file.file().exists(), "Cannot read from locked snapshot");
    return inputStream;
  }

  /**
   * Completes writing the snapshot to persist it and make it available for reads.
   *
   * @return The completed snapshot.
   */
  public synchronized Snapshot complete() throws IOException {
    checkState(file.temporaryFile().exists(), "Snapshot is already complete");
    Files.move(file.temporaryFile().toPath(), file.file().toPath(), ATOMIC_MOVE);
    store.completeSnapshot(this);
    return this;
  }

  /**
   * Closes the snapshot.
   */
  @Override
  public void close() {
  }

  /**
   * Deletes the snapshot.
   */
  public void delete() {
  }

  @Override
  public int hashCode() {
    return Objects.hash(index());
  }

  @Override
  public boolean equals(Object object) {
    if (object == null || getClass() != object.getClass()) {
      return false;
    }
    Snapshot snapshot = (Snapshot) object;
    return snapshot.index() == index();
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("index", index())
        .toString();
  }
}
