/*
 * Copyright 2016-present Open Networking Foundation
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
package io.atomix.core.election.impl;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import com.google.common.collect.Sets;
import io.atomix.core.election.AsyncLeaderElection;
import io.atomix.core.election.Leader;
import io.atomix.core.election.LeaderElection;
import io.atomix.core.election.Leadership;
import io.atomix.core.election.LeadershipEvent;
import io.atomix.core.election.LeadershipEventListener;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.impl.SessionEnabledAsyncPrimitive;
import io.atomix.utils.stream.StreamHandler;

/**
 * Distributed resource providing the {@link AsyncLeaderElection} primitive.
 */
public class DefaultAsyncLeaderElection
    extends SessionEnabledAsyncPrimitive<LeaderElectionProxy, AsyncLeaderElection<String>>
    implements AsyncLeaderElection<String> {

  private final Set<LeadershipEventListener<String>> eventListeners = Sets.newCopyOnWriteArraySet();
  private volatile long streamId;

  public DefaultAsyncLeaderElection(LeaderElectionProxy proxy, Duration timeout, PrimitiveManagementService managementService) {
    super(proxy, timeout, managementService);
  }

  @Override
  public CompletableFuture<Leadership<String>> run(String id) {
    return execute(LeaderElectionProxy::enter, EnterRequest.newBuilder()
        .setId(id)
        .build())
        .thenApply(response -> new Leadership<>(
            new Leader<>(response.getLeader(), response.getTerm(), response.getTimestamp()),
            response.getCandidatesList()));
  }

  @Override
  public CompletableFuture<Void> withdraw(String id) {
    return execute(LeaderElectionProxy::withdraw, WithdrawRequest.newBuilder()
        .setId(id)
        .build())
        .thenApply(response -> null);
  }

  @Override
  public CompletableFuture<Boolean> anoint(String id) {
    return execute(LeaderElectionProxy::anoint, AnointRequest.newBuilder()
        .setId(id)
        .build())
        .thenApply(response -> response.getSucceeded());
  }

  @Override
  public CompletableFuture<Boolean> promote(String id) {
    return execute(LeaderElectionProxy::promote, PromoteRequest.newBuilder()
        .setId(id)
        .build())
        .thenApply(response -> response.getSucceeded());
  }

  @Override
  public CompletableFuture<Void> evict(String id) {
    return execute(LeaderElectionProxy::evict, EvictRequest.newBuilder()
        .setId(id)
        .build())
        .thenApply(response -> null);
  }

  @Override
  public CompletableFuture<Leadership<String>> getLeadership() {
    return execute(LeaderElectionProxy::getLeadership, GetLeadershipRequest.newBuilder().build())
        .thenApply(response -> new Leadership<>(
            new Leader<>(response.getLeader(), response.getTerm(), response.getTimestamp()),
            response.getCandidatesList()));
  }

  @Override
  public synchronized CompletableFuture<Void> addListener(LeadershipEventListener<String> listener) {
    if (eventListeners.isEmpty()) {
      eventListeners.add(listener);
      return execute(LeaderElectionProxy::listen, ListenRequest.newBuilder().build(), new StreamHandler<ListenResponse>() {
        @Override
        public void next(ListenResponse response) {
          eventListeners.forEach(l -> l.event(new LeadershipEvent<>(
              LeadershipEvent.Type.valueOf(response.getType().name()),
              new Leadership<>(new Leader<>(response.getLeader(), response.getTerm(), response.getTimestamp()), response.getCandidatesList()))));
        }

        @Override
        public void complete() {

        }

        @Override
        public void error(Throwable error) {

        }
      }).thenApply(streamId -> {
        this.streamId = streamId;
        return null;
      });
    } else {
      eventListeners.add(listener);
      return CompletableFuture.completedFuture(null);
    }
  }

  @Override
  public synchronized CompletableFuture<Void> removeListener(LeadershipEventListener<String> listener) {
    eventListeners.remove(listener);
    if (eventListeners.isEmpty()) {
      return execute(LeaderElectionProxy::unlisten, UnlistenRequest.newBuilder().setStreamId(streamId).build())
          .thenApply(response -> null);
    } else {
      return CompletableFuture.completedFuture(null);
    }
  }

  @Override
  public LeaderElection<String> sync(Duration operationTimeout) {
    return new BlockingLeaderElection<>(this, operationTimeout.toMillis());
  }
}