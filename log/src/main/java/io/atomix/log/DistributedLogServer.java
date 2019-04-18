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
package io.atomix.log;

import java.io.File;
import java.time.Duration;

import io.atomix.log.impl.DefaultDistributedLogServer;
import io.atomix.log.protocol.LogServerProtocol;
import io.atomix.storage.StorageLevel;
import io.atomix.utils.Managed;
import io.atomix.utils.concurrent.ThreadContextFactory;
import io.atomix.utils.concurrent.ThreadModel;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Log server.
 */
public interface DistributedLogServer extends Managed<DistributedLogServer> {

  /**
   * Returns a new server builder.
   *
   * @return a new server builder
   */
  static Builder builder() {
    return new DefaultDistributedLogServer.Builder();
  }

  /**
   * Log server role.
   */
  enum Role {

    /**
     * Leader role.
     */
    LEADER,

    /**
     * Follower role.
     */
    FOLLOWER,

    /**
     * None role.
     */
    NONE,
  }

  /**
   * Returns the server role.
   *
   * @return the server role
   */
  Role getRole();

  /**
   * Log server builder
   */
  abstract class Builder implements io.atomix.utils.Builder<DistributedLogServer> {
    private static final String DEFAULT_SERVER_NAME = "atomix";
    private static final String DEFAULT_DIRECTORY = System.getProperty("user.dir");
    private static final int DEFAULT_REPLICATION_FACTOR = 2;
    private static final ReplicationStrategy DEFAULT_REPLICATION_STRATEGY = ReplicationStrategy.SYNCHRONOUS;
    private static final int DEFAULT_MAX_SEGMENT_SIZE = 1024 * 1024 * 32;
    private static final int DEFAULT_MAX_ENTRY_SIZE = 1024 * 1024;
    private static final boolean DEFAULT_FLUSH_ON_COMMIT = false;
    private static final long DEFAULT_MAX_LOG_SIZE = 1024 * 1024 * 1024;
    private static final Duration DEFAULT_MAX_LOG_AGE = null;
    private static final double DEFAULT_INDEX_DENSITY = .005;

    protected String serverId = DEFAULT_SERVER_NAME;
    protected LogServerProtocol protocol;
    protected TermProvider termProvider;
    protected ThreadModel threadModel = ThreadModel.SHARED_THREAD_POOL;
    protected int threadPoolSize = Math.max(Math.min(Runtime.getRuntime().availableProcessors() * 2, 16), 4);
    protected ThreadContextFactory threadContextFactory;
    protected int replicationFactor = DEFAULT_REPLICATION_FACTOR;
    protected ReplicationStrategy replicationStrategy = DEFAULT_REPLICATION_STRATEGY;
    protected StorageLevel storageLevel = StorageLevel.DISK;
    protected File directory = new File(DEFAULT_DIRECTORY);
    protected int maxSegmentSize = DEFAULT_MAX_SEGMENT_SIZE;
    protected int maxEntrySize = DEFAULT_MAX_ENTRY_SIZE;
    protected double indexDensity = DEFAULT_INDEX_DENSITY;
    protected boolean flushOnCommit = DEFAULT_FLUSH_ON_COMMIT;
    protected long maxLogSize = DEFAULT_MAX_LOG_SIZE;
    protected Duration maxLogAge = DEFAULT_MAX_LOG_AGE;

    /**
     * Sets the server ID.
     *
     * @param serverId The server ID.
     * @return The server builder.
     * @throws NullPointerException if {@code serverId} is null
     */
    public Builder withServerId(String serverId) {
      this.serverId = checkNotNull(serverId, "server cannot be null");
      return this;
    }

    /**
     * Sets the term provider.
     *
     * @param termProvider the term provider
     * @return the server builder
     */
    public Builder withTermProvider(TermProvider termProvider) {
      this.termProvider = checkNotNull(termProvider, "termProvider cannot be null");
      return this;
    }

    /**
     * Sets the protocol.
     *
     * @param protocol the protocol
     * @return the server builder
     */
    public Builder withProtocol(LogServerProtocol protocol) {
      this.protocol = checkNotNull(protocol, "protocol cannot be null");
      return this;
    }

    /**
     * Sets the client thread model.
     *
     * @param threadModel the client thread model
     * @return the server builder
     * @throws NullPointerException if the thread model is null
     */
    public Builder withThreadModel(ThreadModel threadModel) {
      this.threadModel = checkNotNull(threadModel, "threadModel cannot be null");
      return this;
    }

    /**
     * Sets the client thread pool size.
     *
     * @param threadPoolSize The client thread pool size.
     * @return The server builder.
     * @throws IllegalArgumentException if the thread pool size is not positive
     */
    public Builder withThreadPoolSize(int threadPoolSize) {
      checkArgument(threadPoolSize > 0, "threadPoolSize must be positive");
      this.threadPoolSize = threadPoolSize;
      return this;
    }

    /**
     * Sets the client thread context factory.
     *
     * @param threadContextFactory the client thread context factory
     * @return the server builder
     * @throws NullPointerException if the factory is null
     */
    public Builder withThreadContextFactory(ThreadContextFactory threadContextFactory) {
      this.threadContextFactory = checkNotNull(threadContextFactory, "threadContextFactory cannot be null");
      return this;
    }

    /**
     * Sets the server replication factor.
     *
     * @param replicationFactor the server replication factor
     * @return the server builder
     * @throws IllegalArgumentException if the replication factor is not positive
     */
    public Builder withReplicationFactor(int replicationFactor) {
      checkArgument(replicationFactor > 0, "replicationFactor must be positive");
      this.replicationFactor = replicationFactor;
      return this;
    }

    /**
     * Sets the server replication strategy.
     *
     * @param replicationStrategy the server replication strategy
     * @return the server builder
     * @throws NullPointerException if the replication strategy is null
     */
    public Builder withReplicationStrategy(ReplicationStrategy replicationStrategy) {
      this.replicationStrategy = checkNotNull(replicationStrategy, "replicationStrategy cannot be null");
      return this;
    }

    /**
     * Sets the log storage level, returning the builder for method chaining.
     * <p>
     * The storage level indicates how individual entries should be persisted in the journal.
     *
     * @param storageLevel The log storage level.
     * @return The storage builder.
     */
    public Builder withStorageLevel(StorageLevel storageLevel) {
      this.storageLevel = checkNotNull(storageLevel, "storageLevel cannot be null");
      return this;
    }

    /**
     * Sets the log directory, returning the builder for method chaining.
     * <p>
     * The log will write segment files into the provided directory.
     *
     * @param directory The log directory.
     * @return The storage builder.
     * @throws NullPointerException If the {@code directory} is {@code null}
     */
    public Builder withDirectory(String directory) {
      return withDirectory(new File(checkNotNull(directory, "directory cannot be null")));
    }

    /**
     * Sets the log directory, returning the builder for method chaining.
     * <p>
     * The log will write segment files into the provided directory.
     *
     * @param directory The log directory.
     * @return The storage builder.
     * @throws NullPointerException If the {@code directory} is {@code null}
     */
    public Builder withDirectory(File directory) {
      this.directory = checkNotNull(directory, "directory cannot be null");
      return this;
    }

    /**
     * Sets the maximum segment size in bytes, returning the builder for method chaining.
     * <p>
     * The maximum segment size dictates when logs should roll over to new segments. As entries are written to a segment
     * of the log, once the size of the segment surpasses the configured maximum segment size, the log will create a new
     * segment and append new entries to that segment.
     * <p>
     * By default, the maximum segment size is {@code 1024 * 1024 * 32}.
     *
     * @param maxSegmentSize The maximum segment size in bytes.
     * @return The storage builder.
     * @throws IllegalArgumentException If the {@code maxSegmentSize} is not positive
     */
    public Builder withMaxSegmentSize(int maxSegmentSize) {
      this.maxSegmentSize = maxSegmentSize;
      return this;
    }

    /**
     * Sets the maximum entry size in bytes, returning the builder for method chaining.
     *
     * @param maxEntrySize the maximum entry size in bytes
     * @return the storage builder
     * @throws IllegalArgumentException if the {@code maxEntrySize} is not positive
     */
    public Builder withMaxEntrySize(int maxEntrySize) {
      checkArgument(maxEntrySize > 0, "maxEntrySize must be positive");
      this.maxEntrySize = maxEntrySize;
      return this;
    }

    /**
     * Sets the journal index density.
     * <p>
     * The index density is the frequency at which the position of entries written to the journal will be recorded in an
     * in-memory index for faster seeking.
     *
     * @param indexDensity the index density
     * @return the journal builder
     * @throws IllegalArgumentException if the density is not between 0 and 1
     */
    public Builder withIndexDensity(double indexDensity) {
      checkArgument(indexDensity > 0 && indexDensity < 1, "index density must be between 0 and 1");
      this.indexDensity = indexDensity;
      return this;
    }

    /**
     * Enables flushing buffers to disk when entries are committed to a segment, returning the builder for method
     * chaining.
     * <p>
     * When flush-on-commit is enabled, log entry buffers will be automatically flushed to disk each time an entry is
     * committed in a given segment.
     *
     * @return The storage builder.
     */
    public Builder withFlushOnCommit() {
      return withFlushOnCommit(true);
    }

    /**
     * Sets whether to flush buffers to disk when entries are committed to a segment, returning the builder for method
     * chaining.
     * <p>
     * When flush-on-commit is enabled, log entry buffers will be automatically flushed to disk each time an entry is
     * committed in a given segment.
     *
     * @param flushOnCommit Whether to flush buffers to disk when entries are committed to a segment.
     * @return The storage builder.
     */
    public Builder withFlushOnCommit(boolean flushOnCommit) {
      this.flushOnCommit = flushOnCommit;
      return this;
    }

    /**
     * Sets the maximum log size.
     *
     * @param maxLogSize the maximum log size
     * @return the log server builder
     */
    public Builder withMaxLogSize(long maxLogSize) {
      this.maxLogSize = maxLogSize;
      return this;
    }

    /**
     * Sets the maximum log age.
     *
     * @param maxLogAge the maximum log age
     * @return the log server builder
     */
    public Builder withMaxLogAge(Duration maxLogAge) {
      this.maxLogAge = maxLogAge;
      return this;
    }
  }
}
