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
package io.atomix.server.impl;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.base.Strings;
import com.google.protobuf.ByteString;
import io.atomix.primitive.partition.GroupMember;
import io.atomix.primitive.partition.PrimaryElection;
import io.atomix.primitive.partition.PrimaryElectionEvent;
import io.atomix.primitive.partition.PrimaryTerm;
import io.atomix.primitive.session.impl.CloseSessionRequest;
import io.atomix.primitive.session.impl.KeepAliveRequest;
import io.atomix.primitive.session.impl.OpenSessionRequest;
import io.atomix.primitive.session.impl.SessionCommandContext;
import io.atomix.primitive.session.impl.SessionQueryContext;
import io.atomix.primitive.session.impl.SessionStreamContext;
import io.atomix.server.election.EnterRequest;
import io.atomix.server.election.GetLeadershipRequest;
import io.atomix.server.election.LeaderElectionProxy;
import io.atomix.server.election.ListenRequest;
import io.atomix.server.election.ListenResponse;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.stream.StreamHandler;
import org.apache.commons.lang3.tuple.Pair;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Leader elector based primary election.
 */
public class PrimitivePrimaryElection implements PrimaryElection {
  private final LeaderElectionProxy election;
  private final ThreadContext context;
  private volatile long sessionId;

  PrimitivePrimaryElection(LeaderElectionProxy election, ThreadContext context) {
    this.election = checkNotNull(election);
    this.context = checkNotNull(context);
  }

  @Override
  public CompletableFuture<PrimaryTerm> enter(GroupMember member) {
    return election.enter(
        SessionCommandContext.newBuilder()
            .setSessionId(sessionId)
            .build(),
        EnterRequest.newBuilder()
            .setId(member.toByteString().toStringUtf8())
            .build())
        .thenApply(response -> {
          try {
            return PrimaryTerm.newBuilder()
                .setTerm(response.getRight().getTerm())
                .setPrimary(!Strings.isNullOrEmpty(response.getRight().getLeader())
                    ? GroupMember.parseFrom(ByteString.copyFromUtf8(response.getRight().getLeader()))
                    : GroupMember.getDefaultInstance())
                .addAllCandidates(response.getRight().getCandidatesList().stream()
                    .map(candidate -> {
                      try {
                        return GroupMember.parseFrom(ByteString.copyFromUtf8(candidate));
                      } catch (IOException e) {
                        throw new RuntimeException(e);
                      }
                    })
                    .collect(Collectors.toList()))
                .build();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }

  @Override
  public CompletableFuture<PrimaryTerm> getTerm() {
    return election.getLeadership(
        SessionQueryContext.newBuilder()
            .setSessionId(sessionId)
            .build(),
        GetLeadershipRequest.newBuilder().build())
        .thenApply(response -> {
          try {
            return PrimaryTerm.newBuilder()
                .setTerm(response.getRight().getTerm())
                .setPrimary(!Strings.isNullOrEmpty(response.getRight().getLeader())
                    ? GroupMember.parseFrom(ByteString.copyFromUtf8(response.getRight().getLeader()))
                    : GroupMember.getDefaultInstance())
                .addAllCandidates(response.getRight().getCandidatesList().stream()
                    .map(candidate -> {
                      try {
                        return GroupMember.parseFrom(ByteString.copyFromUtf8(candidate));
                      } catch (IOException e) {
                        throw new RuntimeException(e);
                      }
                    })
                    .collect(Collectors.toList()))
                .build();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }

  @Override
  public synchronized CompletableFuture<Void> addListener(Consumer<PrimaryElectionEvent> listener) {
    return election.listen(
        SessionCommandContext.newBuilder()
            .setSessionId(sessionId)
            .build(),
        ListenRequest.newBuilder().build(),
        new StreamHandler<Pair<SessionStreamContext, ListenResponse>>() {
          @Override
          public void next(Pair<SessionStreamContext, ListenResponse> response) {
            try {
              listener.accept(PrimaryElectionEvent.newBuilder()
                  .setTerm(PrimaryTerm.newBuilder()
                      .setTerm(response.getRight().getTerm())
                      .setPrimary(!Strings.isNullOrEmpty(response.getRight().getLeader())
                          ? GroupMember.parseFrom(ByteString.copyFromUtf8(response.getRight().getLeader()))
                          : GroupMember.getDefaultInstance())
                      .addAllCandidates(response.getRight().getCandidatesList().stream()
                          .map(candidate -> {
                            try {
                              return GroupMember.parseFrom(ByteString.copyFromUtf8(candidate));
                            } catch (IOException e) {
                              throw new RuntimeException(e);
                            }
                          })
                          .collect(Collectors.toList()))
                      .build())
                  .build());
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }

          @Override
          public void complete() {

          }

          @Override
          public void error(Throwable error) {

          }
        })
        .thenApply(response -> null);
  }

  @Override
  public synchronized CompletableFuture<Void> removeListener(Consumer<PrimaryElectionEvent> listener) {
    // TODO: Support removing listeners
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Connects the election.
   *
   * @return a future to be completed once the election has been connected
   */
  public CompletableFuture<PrimaryElection> connect() {
    return election.openSession(OpenSessionRequest.newBuilder()
        .setTimeout(Duration.ofSeconds(5).toMillis())
        .build())
        .thenApply(response -> {
          sessionId = response.getSessionId();
          context.schedule(Duration.ofSeconds(0), Duration.ofSeconds(2), () -> keepAlive());
          return this;
        });
  }

  /**
   * Sends a keep alive request to the cluster.
   */
  private void keepAlive() {
    election.keepAlive(KeepAliveRequest.newBuilder()
        .setSessionId(sessionId)
        .build());
  }

  /**
   * Closes the election.
   *
   * @return a future to be completed once the election has been closed
   */
  public CompletableFuture<Void> close() {
    return election.closeSession(CloseSessionRequest.newBuilder()
        .setSessionId(sessionId)
        .build())
        .thenApply(response -> null);
  }
}
