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
package io.atomix.raft.cluster.impl;

import java.time.Instant;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Consumer;

import com.google.common.hash.Hashing;
import io.atomix.raft.RaftException;
import io.atomix.raft.cluster.RaftMember;
import io.atomix.raft.protocol.RaftError;
import io.atomix.raft.protocol.ReconfigureRequest;
import io.atomix.raft.protocol.ResponseStatus;
import io.atomix.raft.storage.system.RaftConfiguration;
import io.atomix.utils.concurrent.Scheduled;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Cluster member.
 */
public final class DefaultRaftMember implements RaftMember, AutoCloseable {
  private final String id;
  private final int hash;
  private Type type;
  private Instant updated;
  private transient Scheduled configureTimeout;
  private transient RaftClusterContext cluster;
  private final transient Set<Consumer<Type>> typeChangeListeners = new CopyOnWriteArraySet<>();

  public DefaultRaftMember(String id, Type type, Instant updated) {
    this.id = checkNotNull(id, "id cannot be null");
    this.hash = Hashing.murmur3_32()
        .hashUnencodedChars(id)
        .asInt();
    this.type = checkNotNull(type, "type cannot be null");
    this.updated = checkNotNull(updated, "updated cannot be null");
  }

  /**
   * Sets the member's parent cluster.
   */
  DefaultRaftMember setCluster(RaftClusterContext cluster) {
    this.cluster = cluster;
    return this;
  }

  /**
   * Sets the member type.
   *
   * @param type the member type
   */
  void setType(Type type) {
    this.type = type;
  }

  @Override
  public String memberId() {
    return id;
  }

  @Override
  public int hash() {
    return hash;
  }

  @Override
  public RaftMember.Type getType() {
    return type;
  }

  @Override
  public Instant getLastUpdated() {
    return updated;
  }

  @Override
  public void addTypeChangeListener(Consumer<Type> listener) {
    typeChangeListeners.add(listener);
  }

  @Override
  public void removeTypeChangeListener(Consumer<Type> listener) {
    typeChangeListeners.remove(listener);
  }

  @Override
  public CompletableFuture<Void> promote() {
    if (Type.values().length > type.ordinal() + 1) {
      return configure(Type.values()[type.ordinal() + 1]);
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> promote(Type type) {
    return configure(type);
  }

  @Override
  public CompletableFuture<Void> demote() {
    if (type.ordinal() > 0) {
      return configure(Type.values()[type.ordinal() - 1]);
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> demote(Type type) {
    return configure(type);
  }

  @Override
  public CompletableFuture<Void> remove() {
    return configure(Type.INACTIVE);
  }

  /**
   * Updates the member type.
   *
   * @param type The member type.
   * @return The member.
   */
  public DefaultRaftMember update(RaftMember.Type type, Instant time) {
    if (this.type != type) {
      this.type = checkNotNull(type, "type cannot be null");
      if (time.isAfter(updated)) {
        this.updated = checkNotNull(time, "time cannot be null");
      }
      if (typeChangeListeners != null) {
        typeChangeListeners.forEach(l -> l.accept(type));
      }
    }
    return this;
  }

  /**
   * Demotes the server to the given type.
   */
  private CompletableFuture<Void> configure(RaftMember.Type type) {
    if (type == this.type) {
      return CompletableFuture.completedFuture(null);
    }
    CompletableFuture<Void> future = new CompletableFuture<>();
    cluster.getContext().getThreadContext().execute(() -> configure(type, future));
    return future;
  }

  /**
   * Recursively reconfigures the cluster.
   */
  private void configure(RaftMember.Type type, CompletableFuture<Void> future) {
    // Set a timer to retry the attempt to leave the cluster.
    configureTimeout = cluster.getContext().getThreadContext().schedule(cluster.getContext().getElectionTimeout(), () -> {
      configure(type, future);
    });

    // Attempt to leave the cluster by submitting a LeaveRequest directly to the server state.
    // Non-leader states should forward the request to the leader if there is one. Leader states
    // will log, replicate, and commit the reconfiguration.
    cluster.getContext().getRaftRole().onReconfigure(ReconfigureRequest.newBuilder()
        .setIndex(cluster.getConfiguration().getIndex())
        .setTerm(cluster.getConfiguration().getTerm())
        .setMember(io.atomix.raft.protocol.RaftMember.newBuilder()
            .setMemberId(id)
            .setType(io.atomix.raft.protocol.RaftMember.Type.valueOf(type.name()))
            .setUpdated(updated.toEpochMilli())
            .build())
        .build()).whenComplete((response, error) -> {
          if (error == null) {
            if (response.getStatus() == ResponseStatus.OK) {
              cancelConfigureTimer();
              cluster.configure(RaftConfiguration.newBuilder()
                  .setIndex(response.getIndex())
                  .setTerm(response.getTerm())
                  .setTimestamp(response.getTimestamp())
                  .addAllMembers(response.getMembersList())
                  .build());
              future.complete(null);
            } else if (response.getError() == null
                || response.getError() == RaftError.UNAVAILABLE
                || response.getError() == RaftError.PROTOCOL_ERROR
                || response.getError() == RaftError.NO_LEADER) {
              cancelConfigureTimer();
              configureTimeout = cluster.getContext().getThreadContext().schedule(cluster.getContext().getElectionTimeout().multipliedBy(2), () -> configure(type, future));
            } else {
              cancelConfigureTimer();
              future.completeExceptionally(new RaftException.Unavailable("Cluster is unavailable"));
            }
          } else {
            future.completeExceptionally(error);
          }
        });
  }

  /**
   * Cancels the configure timeout.
   */
  private void cancelConfigureTimer() {
    if (configureTimeout != null) {
      configureTimeout.cancel();
      configureTimeout = null;
    }
  }

  @Override
  public void close() {
    cancelConfigureTimer();
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), id);
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof DefaultRaftMember && ((DefaultRaftMember) object).id.equals(id);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("id", id)
        .add("type", type)
        .add("updated", updated)
        .toString();
  }

}
