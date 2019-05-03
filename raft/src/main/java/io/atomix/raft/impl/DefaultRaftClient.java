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
package io.atomix.raft.impl;

import java.net.ConnectException;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Throwables;
import com.google.protobuf.ByteString;
import io.atomix.raft.CommunicationStrategy;
import io.atomix.raft.RaftClient;
import io.atomix.raft.RaftException;
import io.atomix.raft.ReadConsistency;
import io.atomix.raft.protocol.CommandRequest;
import io.atomix.raft.protocol.CommandResponse;
import io.atomix.raft.protocol.QueryRequest;
import io.atomix.raft.protocol.QueryResponse;
import io.atomix.raft.protocol.RaftClientProtocol;
import io.atomix.raft.protocol.RaftError;
import io.atomix.raft.protocol.ResponseStatus;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.concurrent.ThreadContextFactory;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import io.atomix.utils.stream.StreamHandler;
import org.slf4j.Logger;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Default Raft client implementation.
 */
public class DefaultRaftClient implements RaftClient {
  private final String clientId;
  private final Collection<String> cluster;
  private final RaftClientProtocol protocol;
  private final ThreadContextFactory threadContextFactory;
  private final boolean closeThreadFactoryOnClose;
  private final ThreadContext threadContext;
  private final MemberSelector selector;
  private String current;
  private volatile long term;

  public DefaultRaftClient(
      String clientId,
      Collection<String> cluster,
      RaftClientProtocol protocol,
      CommunicationStrategy communicationStrategy,
      ThreadContextFactory threadContextFactory,
      boolean closeThreadFactoryOnClose) {
    this.clientId = checkNotNull(clientId, "clientId cannot be null");
    this.cluster = checkNotNull(cluster, "cluster cannot be null");
    this.protocol = checkNotNull(protocol, "protocol cannot be null");
    this.threadContextFactory = checkNotNull(threadContextFactory, "threadContextFactory cannot be null");
    this.threadContext = threadContextFactory.createContext();
    this.closeThreadFactoryOnClose = closeThreadFactoryOnClose;
    this.selector = new MemberSelector(null, cluster, communicationStrategy);
  }

  @Override
  public String clientId() {
    return clientId;
  }

  @Override
  public long term() {
    return term;
  }

  @Override
  public String leader() {
    return selector.leader();
  }

  @Override
  public CompletableFuture<byte[]> write(byte[] value) {
    String member = selector.leader();
    if (member == null) {
      member = next();
    }
    CompletableFuture<byte[]> future = new CompletableFuture<>();
    protocol.command(member, CommandRequest.newBuilder()
        .setValue(ByteString.copyFrom(value))
        .build())
        .whenComplete((response, error) -> {
          if (error == null) {
            if (response.getStatus() == ResponseStatus.OK) {
              term = response.getTerm();
              String leader = selector.leader();
              Collection<String> members = response.getMembersList();
              if (leader == null && !response.getLeader().equals("")) {
                selector.reset(response.getLeader(), members);
              } else if (leader != null && response.getLeader().equals("")) {
                selector.reset(null, members);
              } else if (leader != null && !response.getLeader().equals("") && !response.getLeader().equals(leader)) {
                selector.reset(response.getLeader(), members);
              }
              future.complete(response.getOutput().toByteArray());
            } else {
              if (response.getError() == RaftError.NO_LEADER) {
                current = null;
              }
              future.completeExceptionally(createException(response.getError(), response.getMessage()));
            }
          } else {
            if (Throwables.getRootCause(error) instanceof ConnectException) {
              current = null;
            }
            future.completeExceptionally(error);
          }
        });
    return future;
  }

  @Override
  public CompletableFuture<Void> write(byte[] value, StreamHandler<byte[]> handler) {
    String member = selector.leader();
    if (member == null) {
      member = next();
    }
    return protocol.commandStream(member, CommandRequest.newBuilder()
            .setValue(ByteString.copyFrom(value))
            .build(),
        new StreamHandler<CommandResponse>() {
          private final AtomicBoolean complete = new AtomicBoolean();

          @Override
          public void next(CommandResponse response) {
            if (response.getStatus() == ResponseStatus.OK) {
              term = response.getTerm();
              String leader = selector.leader();
              Collection<String> members = response.getMembersList();
              if (leader == null && !response.getLeader().equals("")) {
                selector.reset(response.getLeader(), members);
              } else if (leader != null && response.getLeader().equals("")) {
                selector.reset(null, members);
              } else if (leader != null && !response.getLeader().equals("") && !response.getLeader().equals(leader)) {
                selector.reset(response.getLeader(), members);
              }

              if (!complete.get()) {
                handler.next(response.getOutput().toByteArray());
              }
            } else {
              if (response.getError() == RaftError.NO_LEADER) {
                current = null;
              }
              if (complete.compareAndSet(false, true)) {
                handler.error(createException(response.getError(), response.getMessage()));
              }
            }
          }

          @Override
          public void complete() {
            if (complete.compareAndSet(false, true)) {
              handler.complete();
            }
          }

          @Override
          public void error(Throwable error) {
            if (Throwables.getRootCause(error) instanceof ConnectException) {
              current = null;
            }
            if (complete.compareAndSet(false, true)) {
              handler.error(error);
            }
          }
        });
  }

  @Override
  public CompletableFuture<byte[]> read(byte[] value, ReadConsistency consistency) {
    CompletableFuture<byte[]> future = new CompletableFuture<>();
    protocol.query(next(), QueryRequest.newBuilder()
        .setValue(ByteString.copyFrom(value))
        .setReadConsistency(io.atomix.raft.protocol.ReadConsistency.valueOf(consistency.name()))
        .build())
        .whenComplete((response, error) -> {
          if (error == null) {
            if (response.getStatus() == ResponseStatus.OK) {
              future.complete(response.getOutput().toByteArray());
            } else {
              if (response.getError() == RaftError.NO_LEADER) {
                current = null;
              }
              future.completeExceptionally(createException(response.getError(), response.getMessage()));
            }
          } else {
            if (Throwables.getRootCause(error) instanceof ConnectException) {
              current = null;
            }
            future.completeExceptionally(error);
          }
        });
    return future;
  }

  @Override
  public CompletableFuture<Void> read(byte[] value, ReadConsistency consistency, StreamHandler<byte[]> handler) {
    return protocol.queryStream(next(), QueryRequest.newBuilder()
            .setValue(ByteString.copyFrom(value))
            .setReadConsistency(io.atomix.raft.protocol.ReadConsistency.valueOf(consistency.name()))
            .build(),
        new StreamHandler<QueryResponse>() {
          private final AtomicBoolean complete = new AtomicBoolean();

          @Override
          public void next(QueryResponse response) {
            if (response.getStatus() == ResponseStatus.OK) {
              handler.next(response.getOutput().toByteArray());
            } else {
              if (response.getError() == RaftError.NO_LEADER) {
                current = null;
              }
              handler.error(createException(response.getError(), response.getMessage()));
            }
          }

          @Override
          public void complete() {
            if (complete.compareAndSet(false, true)) {
              handler.complete();
            }
          }

          @Override
          public void error(Throwable error) {
            if (Throwables.getRootCause(error) instanceof ConnectException) {
              current = null;
            }
            if (complete.compareAndSet(false, true)) {
              handler.error(error);
            }
          }
        });
  }

  /**
   * Creates a new exception from an error response.
   *
   * @param error   the Raft error
   * @param message the error message
   * @return the exception
   */
  private Throwable createException(RaftError error, String message) {
    switch (error) {
      case NO_LEADER:
        return new RaftException.NoLeader(message);
      case COMMAND_FAILURE:
        return new RaftException.CommandFailure(message);
      case QUERY_FAILURE:
        return new RaftException.QueryFailure(message);
      case APPLICATION_ERROR:
        return new RaftException.ApplicationException(message);
      case UNKNOWN_SERVICE:
        return new RaftException.UnknownService(message);
      case UNKNOWN_SESSION:
        return new RaftException.UnknownSession(message);
      case ILLEGAL_MEMBER_STATE:
        return new RaftException.IllegalMemberState(message);
      case PROTOCOL_ERROR:
        return new RaftException.ProtocolException(message);
      case CONFIGURATION_ERROR:
        return new RaftException.ConfigurationException(message);
      case UNAVAILABLE:
        return new RaftException.Unavailable(message);
      default:
        return new RaftException.Unavailable(message);
    }
  }

  /**
   * Connects to the cluster.
   */
  protected String next() {
    // If a connection was already established then use that connection.
    if (current != null) {
      return current;
    }

    if (!selector.hasNext()) {
      if (selector.leader() != null) {
        selector.reset(null, selector.members());
        this.current = selector.next();
        return current;
      } else {
        selector.reset();
        return null;
      }
    } else {
      this.current = selector.next();
      return current;
    }
  }

  @Override
  public synchronized CompletableFuture<RaftClient> connect(Collection<String> cluster) {
    // If the provided cluster list is null or empty, use the default list.
    if (cluster == null || cluster.isEmpty()) {
      cluster = this.cluster;
    }

    // If the default list is null or empty, use the default host:port.
    if (cluster == null || cluster.isEmpty()) {
      throw new IllegalArgumentException("No cluster specified");
    }

    selector.reset(null, cluster);
    return CompletableFuture.completedFuture(this);
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    if (closeThreadFactoryOnClose) {
      threadContextFactory.close();
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("id", clientId)
        .toString();
  }

  /**
   * Default Raft client builder.
   */
  public static class Builder extends RaftClient.Builder {
    public Builder(Collection<String> cluster) {
      super(cluster);
    }

    @Override
    public RaftClient build() {
      Logger log = ContextualLoggerFactory.getLogger(DefaultRaftClient.class, LoggerContext.builder(RaftClient.class)
          .addValue(clientId)
          .build());

      // If a ThreadContextFactory was not provided, create one and ensure it's closed when the client is stopped.
      boolean closeThreadFactoryOnClose;
      ThreadContextFactory threadContextFactory;
      if (this.threadContextFactory == null) {
        threadContextFactory = threadModel.factory("raft-client-" + clientId + "-%d", threadPoolSize, log);
        closeThreadFactoryOnClose = true;
      } else {
        threadContextFactory = this.threadContextFactory;
        closeThreadFactoryOnClose = false;
      }

      return new DefaultRaftClient(clientId, cluster, protocol, communicationStrategy, threadContextFactory, closeThreadFactoryOnClose);
    }
  }
}
