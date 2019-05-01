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
 * limitations under the License.
 */
package io.atomix.raft.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.google.common.collect.Maps;
import io.atomix.raft.RaftCommand;
import io.atomix.raft.RaftException;
import io.atomix.raft.RaftOperation;
import io.atomix.raft.RaftQuery;
import io.atomix.raft.RaftServer;
import io.atomix.raft.RaftStateMachine;
import io.atomix.raft.storage.log.RaftLog;
import io.atomix.raft.storage.log.RaftLogEntry;
import io.atomix.raft.storage.log.RaftLogReader;
import io.atomix.raft.storage.snapshot.Snapshot;
import io.atomix.storage.StorageLevel;
import io.atomix.storage.journal.Indexed;
import io.atomix.utils.stream.StreamHandler;
import io.atomix.utils.concurrent.ComposableFuture;
import io.atomix.utils.concurrent.OrderedFuture;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import io.atomix.utils.time.WallClockTimestamp;
import org.slf4j.Logger;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Internal server state machine.
 */
public class RaftStateMachineManager implements RaftStateMachine.Context, AutoCloseable {
  private static final Duration SNAPSHOT_INTERVAL = Duration.ofSeconds(10);
  private static final Duration SNAPSHOT_COMPLETION_DELAY = Duration.ofSeconds(10);
  private static final Duration COMPACT_DELAY = Duration.ofSeconds(10);

  private static final int SEGMENT_BUFFER_FACTOR = 5;

  private final Logger logger;
  private final RaftContext raft;
  private final RaftStateMachine stateMachine;
  private final ThreadContext stateContext;
  private final RaftLog log;
  private final RaftLogReader reader;
  private final Map<Long, CompletableFuture> futures = Maps.newHashMap();
  private final Map<Long, StreamHandler<byte[]>> handlers = Maps.newHashMap();
  private volatile CompletableFuture<Void> compactFuture;
  private RaftOperation.Type operationType;
  private long lastIndex;
  private long lastTimestamp;
  private long lastEnqueued;
  private long lastCompacted;

  public RaftStateMachineManager(RaftContext raft, RaftStateMachine stateMachine, ThreadContext stateContext) {
    this.raft = checkNotNull(raft, "state cannot be null");
    this.stateMachine = checkNotNull(stateMachine, "stateMachine cannot be null");
    this.log = raft.getLog();
    this.reader = log.openReader(1, RaftLogReader.Mode.COMMITS);
    this.stateContext = stateContext;
    this.logger = ContextualLoggerFactory.getLogger(getClass(), LoggerContext.builder(RaftServer.class)
        .addValue(raft.getName())
        .build());
    this.lastEnqueued = reader.getFirstIndex() - 1;
    scheduleSnapshots();
    stateMachine.init(this);
  }

  @Override
  public long getIndex() {
    return lastIndex;
  }

  @Override
  public long getTimestamp() {
    return lastTimestamp;
  }

  @Override
  public RaftOperation.Type getOperationType() {
    return operationType;
  }

  @Override
  public Logger getLogger() {
    return logger;
  }

  /**
   * Returns a boolean indicating whether the node is running out of disk space.
   */
  private boolean isRunningOutOfDiskSpace() {
    // If there's not enough space left to allocate two log segments
    return raft.getStorage().statistics().getUsableSpace() < raft.getStorage().maxLogSegmentSize() * SEGMENT_BUFFER_FACTOR
        // Or the used disk percentage has surpassed the free disk buffer percentage
        || raft.getStorage().statistics().getUsableSpace() / (double) raft.getStorage().statistics().getTotalSpace() < raft.getStorage().freeDiskBuffer();
  }

  /**
   * Returns a boolean indicating whether the node is running out of memory.
   */
  private boolean isRunningOutOfMemory() {
    StorageLevel level = raft.getStorage().storageLevel();
    if (level == StorageLevel.MEMORY || level == StorageLevel.MAPPED) {
      long freeMemory = raft.getStorage().statistics().getFreeMemory();
      long totalMemory = raft.getStorage().statistics().getTotalMemory();
      if (freeMemory > 0 && totalMemory > 0) {
        return freeMemory / (double) totalMemory < raft.getStorage().freeMemoryBuffer();
      }
    }
    return false;
  }

  /**
   * Schedules a snapshot iteration.
   */
  private void scheduleSnapshots() {
    raft.getThreadContext().schedule(SNAPSHOT_INTERVAL, () -> takeSnapshots(true, false));
  }

  /**
   * Compacts Raft logs.
   *
   * @return a future to be completed once logs have been compacted
   */
  public CompletableFuture<Void> compact() {
    return takeSnapshots(false, true);
  }

  /**
   * Takes a snapshot of all services and compacts logs if the server is not under high load or disk needs to be freed.
   */
  private CompletableFuture<Void> takeSnapshots(boolean rescheduleAfterCompletion, boolean force) {
    // If compaction is already in progress, return the existing future and reschedule if this is a scheduled compaction.
    if (compactFuture != null) {
      if (rescheduleAfterCompletion) {
        compactFuture.whenComplete((r, e) -> scheduleSnapshots());
      }
      return compactFuture;
    }

    long lastApplied = raft.getLastApplied();

    // Only take snapshots if segments can be removed from the log below the lastApplied index.
    if (raft.getLog().isCompactable(lastApplied) && raft.getLog().getCompactableIndex(lastApplied) > lastCompacted) {

      // Determine whether the node is running out of disk space.
      boolean runningOutOfDiskSpace = isRunningOutOfDiskSpace();

      // Determine whether the node is running out of memory.
      boolean runningOutOfMemory = isRunningOutOfMemory();

      // If compaction is not already being forced...
      if (!force
          // And the node isn't running out of memory (we need to free up memory if it is)...
          && !runningOutOfMemory
          // And dynamic compaction is enabled (we need to compact immediately if it's disabled)...
          && raft.getStorage().dynamicCompaction()
          // And the node isn't running out of disk space (we need to compact immediately if it is)...
          && !runningOutOfDiskSpace
          // And the server is under high load (we can skip compaction at this point)...
          && raft.getLoadMonitor().isUnderHighLoad()) {
        // We can skip taking a snapshot for now.
        logger.debug("Skipping compaction due to high load");
        if (rescheduleAfterCompletion) {
          scheduleSnapshots();
        }
        return CompletableFuture.completedFuture(null);
      }

      logger.debug("Snapshotting services");

      // Update the index at which the log was last compacted.
      this.lastCompacted = lastApplied;

      // We need to ensure that callbacks added to the compaction future are completed in the order in which they
      // were added in order to preserve the order of retries when appending to the log.
      compactFuture = new OrderedFuture<>();

      // Wait for snapshots in all state machines to be completed before compacting the log at the last applied index.
      takeSnapshots().whenComplete((snapshot, error) -> {
        if (error == null) {
          scheduleCompletion(snapshot);
        }
      });

      // Reschedule snapshots after completion if necessary.
      if (rescheduleAfterCompletion) {
        compactFuture.whenComplete((r, e) -> scheduleSnapshots());
      }
      return compactFuture;
    }
    // Otherwise, if the log can't be compacted anyways, just reschedule snapshots.
    else {
      if (rescheduleAfterCompletion) {
        scheduleSnapshots();
      }
      return CompletableFuture.completedFuture(null);
    }
  }

  /**
   * Takes and persists snapshots of provided services.
   *
   * @return future to be completed once all snapshots have been completed
   */
  private CompletableFuture<Snapshot> takeSnapshots() {
    ComposableFuture<Snapshot> future = new ComposableFuture<>();
    stateContext.execute(() -> {
      try {
        future.complete(snapshot());
      } catch (Exception e) {
        future.completeExceptionally(e);
      }
    });
    return future;
  }

  /**
   * Schedules a completion check for the snapshot at the given index.
   *
   * @param snapshot the snapshot to complete
   */
  private void scheduleCompletion(Snapshot snapshot) {
    stateContext.schedule(SNAPSHOT_COMPLETION_DELAY, () -> {
      if (stateMachine.canDelete(snapshot.index())) {
        logger.debug("Completing snapshot {}", snapshot.index());
        try {
          snapshot.complete();
        } catch (IOException e) {
          logger.error("Failed to persist snapshot", e);
        }
        // If log compaction is being forced, immediately compact the logs.
        if (!raft.getLoadMonitor().isUnderHighLoad() || isRunningOutOfDiskSpace() || isRunningOutOfMemory()) {
          compactLogs(snapshot.index());
        } else {
          scheduleCompaction(snapshot.index());
        }
      } else {
        scheduleCompletion(snapshot);
      }
    });
  }

  /**
   * Schedules a log compaction.
   *
   * @param lastApplied the last applied index at the start of snapshotting. This represents the highest index
   *                    before which segments can be safely removed from disk
   */
  private void scheduleCompaction(long lastApplied) {
    // Schedule compaction after a randomized delay to discourage snapshots on multiple nodes at the same time.
    logger.trace("Scheduling compaction in {}", COMPACT_DELAY);
    stateContext.schedule(COMPACT_DELAY, () -> compactLogs(lastApplied));
  }

  /**
   * Compacts logs up to the given index.
   *
   * @param compactIndex the index to which to compact logs
   */
  private void compactLogs(long compactIndex) {
    raft.getThreadContext().execute(() -> {
      logger.debug("Compacting logs up to index {}", compactIndex);
      try {
        raft.getLog().compact(compactIndex);
      } catch (Exception e) {
        logger.error("An exception occurred during log compaction: {}", e);
      } finally {
        this.compactFuture.complete(null);
        this.compactFuture = null;
        // Immediately attempt to take new snapshots since compaction is already run after a time interval.
        takeSnapshots(false, false);
      }
    });
  }

  /**
   * Applies all commits up to the given index.
   * <p>
   * Calls to this method are assumed not to expect a result. This allows some optimizations to be made internally since
   * linearizable events don't have to be waited to complete the command.
   *
   * @param index The index up to which to apply commits.
   */
  public void applyAll(long index) {
    enqueueBatch(index);
  }

  /**
   * Applies the entry at the given index to the state machine.
   * <p>
   * Calls to this method are assumed to expect a result. This means linearizable session events triggered by the
   * application of the command at the given index will be awaited before completing the returned future.
   *
   * @param index The index to apply.
   * @return A completable future to be completed once the commit has been applied.
   */
  @SuppressWarnings("unchecked")
  public <T> CompletableFuture<T> apply(long index) {
    CompletableFuture<T> future = futures.computeIfAbsent(index, i -> new CompletableFuture<T>());
    enqueueBatch(index);
    return future;
  }

  /**
   * Applies the entry at the given index to the state machine.
   * <p>
   * Calls to this method are assumed to expect a result. This means linearizable session events triggered by the
   * application of the command at the given index will be awaited before completing the returned future.
   *
   * @param index The index to apply.
   * @param handler The stream handler
   * @return A completable future to be completed once the commit has been applied.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<Void> apply(long index, StreamHandler<byte[]> handler) {
    CompletableFuture<Void> future = futures.computeIfAbsent(index, i -> new CompletableFuture<Void>());
    handlers.put(index, handler);
    enqueueBatch(index);
    return future;
  }

  /**
   * Applies all entries up to the given index.
   *
   * @param index the index up to which to apply entries
   */
  private void enqueueBatch(long index) {
    while (lastEnqueued < index) {
      enqueueIndex(++lastEnqueued);
    }
  }

  /**
   * Enqueues an index to be applied to the state machine.
   *
   * @param index the index to be applied to the state machine
   */
  private void enqueueIndex(long index) {
    raft.getThreadContext().execute(() -> applyIndex(index));
  }

  /**
   * Applies the next entry in the log up to the given index.
   *
   * @param index the index up to which to apply the entry
   */
  @SuppressWarnings("unchecked")
  private void applyIndex(long index) {
    // Apply entries prior to this entry.
    if (reader.hasNext() && reader.getNextIndex() == index) {
      // Read the entry from the log. If the entry is non-null then apply it, otherwise
      // simply update the last applied index and return a null result.
      Indexed<RaftLogEntry> entry = reader.next();
      try {
        if (entry.index() != index) {
          throw new IllegalStateException("inconsistent index applying entry " + index + ": " + entry);
        }
        CompletableFuture future = futures.remove(index);
        StreamHandler<byte[]> handler = handlers.remove(index);
        if (handler != null) {
          apply(entry, handler).whenComplete((r, e) -> {
            raft.setLastApplied(index);
            if (future != null) {
              if (e == null) {
                future.complete(r);
              } else {
                future.completeExceptionally(e);
              }
            }
          });
        } else {
          apply(entry).whenComplete((r, e) -> {
            raft.setLastApplied(index);
            if (future != null) {
              if (e == null) {
                future.complete(r);
              } else {
                future.completeExceptionally(e);
              }
            }
          });
        }
      } catch (Exception e) {
        logger.error("Failed to apply {}: {}", entry, e);
      }
    } else {
      CompletableFuture future = futures.remove(index);
      if (future != null) {
        logger.error("Cannot apply index " + index);
        future.completeExceptionally(new IndexOutOfBoundsException("Cannot apply index " + index));
      }
    }
  }

  /**
   * Applies an entry to the state machine.
   * <p>
   * Calls to this method are assumed to expect a result. This means linearizable session events triggered by the
   * application of the given entry will be awaited before completing the returned future.
   *
   * @param entry The entry to apply.
   * @return A completable future to be completed with the result.
   */
  @SuppressWarnings("unchecked")
  public <T> CompletableFuture<T> apply(Indexed<RaftLogEntry> entry) {
    CompletableFuture<T> future = new CompletableFuture<>();
    stateContext.execute(() -> {
      logger.trace("Applying {}", entry);
      try {
        if (entry.entry().hasQuery()) {
          applyQuery(entry).whenComplete((r, e) -> {
            if (e != null) {
              future.completeExceptionally(e);
            } else {
              future.complete((T) r);
            }
          });
        } else {
          // Get the current snapshot. If the snapshot is for a higher index then skip this operation.
          // If the snapshot is for the prior index, install it.
          Snapshot snapshot = raft.getSnapshotStore().getCurrentSnapshot();
          if (snapshot != null) {
            if (snapshot.index() >= entry.index()) {
              future.complete(null);
              return;
            } else if (snapshot.index() == entry.index() - 1) {
              install(snapshot);
            }
          }

          if (entry.entry().hasCommand()) {
            if (entry.entry().getCommand().getStream()) {
              applyCommand(entry, new StreamHandler<byte[]>() {
                @Override
                public void next(byte[] value) {

                }

                @Override
                public void complete() {

                }

                @Override
                public void error(Throwable error) {

                }
              }).whenComplete((r, e) -> {
                if (e != null) {
                  future.completeExceptionally(e);
                } else {
                  future.complete((T) r);
                }
              });
            } else {
              applyCommand(entry).whenComplete((r, e) -> {
                if (e != null) {
                  future.completeExceptionally(e);
                } else {
                  future.complete((T) r);
                }
              });
            }
          } else if (entry.entry().hasInitialize()) {
            future.complete((T) applyInitialize(entry));
          } else if (entry.entry().hasConfiguration()) {
            future.complete((T) applyConfiguration(entry));
          } else {
            future.completeExceptionally(new RaftException.ProtocolException("Unknown entry type"));
          }
        }
      } catch (Exception e) {
        future.completeExceptionally(e);
      }
    });
    return future;
  }

  /**
   * Applies an entry to the state machine.
   * <p>
   * Calls to this method are assumed to expect a result. This means linearizable session events triggered by the
   * application of the given entry will be awaited before completing the returned future.
   *
   * @param entry The entry to apply.
   * @return A completable future to be completed with the result.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<Void> apply(Indexed<RaftLogEntry> entry, StreamHandler<byte[]> handler) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    stateContext.execute(() -> {
      logger.trace("Applying {}", entry);
      try {
        if (entry.entry().hasQuery()) {
          applyQuery(entry, handler).whenComplete((r, e) -> {
            if (e != null) {
              future.completeExceptionally(e);
            } else {
              future.complete(null);
            }
          });
        } else {
          // Get the current snapshot. If the snapshot is for a higher index then skip this operation.
          // If the snapshot is for the prior index, install it.
          Snapshot snapshot = raft.getSnapshotStore().getCurrentSnapshot();
          if (snapshot != null) {
            if (snapshot.index() >= entry.index()) {
              future.complete(null);
              return;
            } else if (snapshot.index() == entry.index() - 1) {
              install(snapshot);
            }
          }

          if (entry.entry().hasCommand()) {
            applyCommand(entry, handler).whenComplete((r, e) -> {
              if (e != null) {
                future.completeExceptionally(e);
              } else {
                future.complete(null);
              }
            });
          } else {
            future.completeExceptionally(new RaftException.ProtocolException("Unknown entry type"));
          }
        }
      } catch (Exception e) {
        future.completeExceptionally(e);
      }
    });
    return future;
  }

  /**
   * Takes snapshots for the given index.
   */
  Snapshot snapshot() {
    Snapshot snapshot = raft.getSnapshotStore().newSnapshot(raft.getLastApplied(), new WallClockTimestamp());
    try (OutputStream output = snapshot.openOutputStream()) {
      stateMachine.snapshot(output);
    } catch (IOException e) {
      snapshot.close();
      logger.error("Failed to snapshot services", e);
    }
    return snapshot;
  }

  /**
   * Prepares sessions for the given index.
   *
   * @param snapshot the snapshot to install
   */
  void install(Snapshot snapshot) {
    logger.debug("Installing snapshot {}", snapshot);
    try (InputStream input = snapshot.openInputStream()) {
      stateMachine.install(input);
    } catch (IOException e) {
      logger.error("Failed to read snapshot", e);
    }
  }

  /**
   * Applies an initialize entry.
   * <p>
   * Initialize entries are used only at the beginning of a new leader's term to force the commitment of entries from
   * prior terms, therefore no logic needs to take place.
   */
  private CompletableFuture<Void> applyInitialize(Indexed<RaftLogEntry> entry) {
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Applies a configuration entry to the internal state machine.
   * <p>
   * Configuration entries are applied to internal server state when written to the log. Thus, no significant logic
   * needs to take place in the handling of configuration entries. We simply release the previous configuration entry
   * since it was overwritten by a more recent committed configuration entry.
   */
  private CompletableFuture<Void> applyConfiguration(Indexed<RaftLogEntry> entry) {
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Applies a command entry to the state machine.
   */
  private CompletableFuture<OperationResult> applyCommand(Indexed<RaftLogEntry> entry) {
    raft.getLoadMonitor().recordEvent();
    operationType = RaftOperation.Type.COMMAND;
    lastIndex = entry.index();
    lastTimestamp = Math.max(lastTimestamp, entry.entry().getTimestamp());
    RaftCommand command = new RaftCommand(
        lastIndex,
        lastTimestamp,
        entry.entry().getCommand().getValue().toByteArray());
    return stateMachine.apply(command)
        .thenApply(OperationResult::succeeded)
        .exceptionally(OperationResult::failed);
  }

  /**
   * Applies a command entry to the state machine.
   */
  private CompletableFuture<Void> applyCommand(Indexed<RaftLogEntry> entry, StreamHandler<byte[]> handler) {
    raft.getLoadMonitor().recordEvent();
    operationType = RaftOperation.Type.COMMAND;
    lastIndex = entry.index();
    lastTimestamp = Math.max(lastTimestamp, entry.entry().getTimestamp());
    RaftCommand command = new RaftCommand(
        lastIndex,
        lastTimestamp,
        entry.entry().getCommand().getValue().toByteArray());
    return stateMachine.apply(command, handler);
  }

  /**
   * Applies a query entry to the state machine.
   */
  private CompletableFuture<OperationResult> applyQuery(Indexed<RaftLogEntry> entry) {
    operationType = RaftOperation.Type.QUERY;
    RaftQuery query = new RaftQuery(
        lastIndex,
        lastTimestamp,
        entry.entry().getQuery().getValue().toByteArray());
    return stateMachine.apply(query)
        .thenApply(OperationResult::succeeded)
        .exceptionally(OperationResult::failed);
  }

  /**
   * Applies a query entry to the state machine.
   */
  private CompletableFuture<Void> applyQuery(Indexed<RaftLogEntry> entry, StreamHandler<byte[]> handler) {
    operationType = RaftOperation.Type.QUERY;
    RaftQuery query = new RaftQuery(
        lastIndex,
        lastTimestamp,
        entry.entry().getQuery().getValue().toByteArray());
    return stateMachine.apply(query, handler);
  }

  @Override
  public void close() {
    // Don't close the thread context here since state machines can be reused.
  }
}
