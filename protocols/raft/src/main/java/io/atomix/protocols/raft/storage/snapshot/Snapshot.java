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
package io.atomix.protocols.raft.storage.snapshot;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;

import io.atomix.utils.time.WallClockTimestamp;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Manages reading and writing a single snapshot file.
 */
public class Snapshot implements AutoCloseable {
  private final SnapshotFile file;
  private final SnapshotDescriptor descriptor;
  private final SnapshotStore store;
  private OutputStream output;

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
    checkOutput();
    OutputStream outputStream = new FileOutputStream(file.file());
    descriptor.writeTo(outputStream);
    return openOutputStream(outputStream, descriptor);
  }

  /**
   * Checks that the snapshot can be written.
   */
  protected void checkOutput() {
    checkState(output == null, "cannot create multiple output streams for the same snapshot");
  }

  /**
   * Opens the given snapshot output stream.
   */
  protected OutputStream openOutputStream(OutputStream output, SnapshotDescriptor descriptor) {
    checkOutput();
    checkState(!descriptor.getLocked(), "cannot write to locked snapshot descriptor");
    this.output = checkNotNull(output, "output cannot be null");
    return output;
  }

  /**
   * Closes the current snapshot output.
   */
  protected void closeOutputStream(OutputStream output) {
    this.output = null;
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
    SnapshotDescriptor descriptor = SnapshotDescriptor.parseDelimitedFrom(inputStream);
    return openInputStream(inputStream, descriptor);
  }

  /**
   * Opens the given snapshot input stream.
   */
  protected InputStream openInputStream(InputStream input, SnapshotDescriptor descriptor) {
    checkState(descriptor.getLocked(), "cannot read from unlocked snapshot descriptor");
    return input;
  }

  /**
   * Completes writing the snapshot to persist it and make it available for reads.
   *
   * @return The completed snapshot.
   */
  public Snapshot complete() {
    store.completeSnapshot(this);
    return this;
  }

  /**
   * Persists the snapshot to disk if necessary.
   * <p>
   * If the snapshot store is backed by disk, the snapshot will be persisted.
   *
   * @return The persisted snapshot.
   */
  public Snapshot persist() {
    OutputStream output = this.output;
    if (output != null) {
      try {
        output.flush();
      } catch (IOException e) {
      }
    }
    return this;
  }

  /**
   * Returns whether the snapshot is persisted.
   *
   * @return Whether the snapshot is persisted.
   */
  public boolean isPersisted() {
    return true;
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
