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
package io.atomix.protocols.raft.session.impl;

import java.net.ConnectException;
import java.net.SocketException;
import java.nio.channels.ClosedChannelException;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Predicate;

import com.google.protobuf.Message;
import io.atomix.cluster.MemberId;
import io.atomix.primitive.PrimitiveException;
import io.atomix.protocols.raft.RaftException;
import io.atomix.protocols.raft.protocol.CloseSessionRequest;
import io.atomix.protocols.raft.protocol.CloseSessionResponse;
import io.atomix.protocols.raft.protocol.KeepAliveRequest;
import io.atomix.protocols.raft.protocol.KeepAliveResponse;
import io.atomix.protocols.raft.protocol.MetadataRequest;
import io.atomix.protocols.raft.protocol.MetadataResponse;
import io.atomix.protocols.raft.protocol.OpenSessionRequest;
import io.atomix.protocols.raft.protocol.OpenSessionResponse;
import io.atomix.protocols.raft.protocol.OperationRequest;
import io.atomix.protocols.raft.protocol.OperationResponse;
import io.atomix.protocols.raft.protocol.RaftClientProtocol;
import io.atomix.protocols.raft.protocol.RaftError;
import io.atomix.protocols.raft.protocol.ResponseStatus;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import org.slf4j.Logger;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Client connection that recursively connects to servers in the cluster and attempts to submit requests.
 */
public class RaftSessionConnection {
  private static final Predicate<Message> COMPLETE_PREDICATE = response -> {
    Object status = response.getField(response.getDescriptorForType().findFieldByName("status"));
    if (status == ResponseStatus.OK) {
      return true;
    }

    RaftError error = (RaftError) response.getField(response.getDescriptorForType().findFieldByName("error"));
    return error == RaftError.COMMAND_FAILURE
        || error == RaftError.QUERY_FAILURE
        || error == RaftError.APPLICATION_ERROR
        || error == RaftError.UNKNOWN_CLIENT
        || error == RaftError.UNKNOWN_SESSION
        || error == RaftError.UNKNOWN_SERVICE
        || error == RaftError.PROTOCOL_ERROR;
  };

  private final Logger log;
  private final RaftClientProtocol protocol;
  private final MemberSelector selector;
  private final ThreadContext context;
  private MemberId currentNode;
  private int selectionId;

  public RaftSessionConnection(RaftClientProtocol protocol, MemberSelector selector, ThreadContext context, LoggerContext loggerContext) {
    this.protocol = checkNotNull(protocol, "protocol cannot be null");
    this.selector = checkNotNull(selector, "selector cannot be null");
    this.context = checkNotNull(context, "context cannot be null");
    this.log = ContextualLoggerFactory.getLogger(getClass(), loggerContext);
  }

  /**
   * Resets the member selector.
   */
  public void reset() {
    selector.reset();
  }

  /**
   * Resets the member selector.
   *
   * @param leader  the selector leader
   * @param servers the selector servers
   */
  public void reset(MemberId leader, Collection<MemberId> servers) {
    selector.reset(leader, servers);
  }

  /**
   * Returns the current selector leader.
   *
   * @return The current selector leader.
   */
  public MemberId leader() {
    return selector.leader();
  }

  /**
   * Returns the current set of members.
   *
   * @return The current set of members.
   */
  public Collection<MemberId> members() {
    return selector.members();
  }

  /**
   * Sends an open session request to the given node.
   *
   * @param request the request to send
   * @return a future to be completed with the response
   */
  public CompletableFuture<OpenSessionResponse> openSession(OpenSessionRequest request) {
    CompletableFuture<OpenSessionResponse> future = new CompletableFuture<>();
    if (context.isCurrentContext()) {
      sendRequest(request, protocol::openSession, future);
    } else {
      context.execute(() -> sendRequest(request, protocol::openSession, future));
    }
    return future;
  }

  /**
   * Sends a close session request to the given node.
   *
   * @param request the request to send
   * @return a future to be completed with the response
   */
  public CompletableFuture<CloseSessionResponse> closeSession(CloseSessionRequest request) {
    CompletableFuture<CloseSessionResponse> future = new CompletableFuture<>();
    if (context.isCurrentContext()) {
      sendRequest(request, protocol::closeSession, future);
    } else {
      context.execute(() -> sendRequest(request, protocol::closeSession, future));
    }
    return future;
  }

  /**
   * Sends a keep alive request to the given node.
   *
   * @param request the request to send
   * @return a future to be completed with the response
   */
  public CompletableFuture<KeepAliveResponse> keepAlive(KeepAliveRequest request) {
    CompletableFuture<KeepAliveResponse> future = new CompletableFuture<>();
    if (context.isCurrentContext()) {
      sendRequest(request, protocol::keepAlive, future);
    } else {
      context.execute(() -> sendRequest(request, protocol::keepAlive, future));
    }
    return future;
  }

  /**
   * Sends a query request to the given node.
   *
   * @param request the request to send
   * @return a future to be completed with the response
   */
  public CompletableFuture<OperationResponse> query(OperationRequest request) {
    CompletableFuture<OperationResponse> future = new CompletableFuture<>();
    if (context.isCurrentContext()) {
      sendRequest(request, protocol::query, future);
    } else {
      context.execute(() -> sendRequest(request, protocol::query, future));
    }
    return future;
  }

  /**
   * Sends a command request to the given node.
   *
   * @param request the request to send
   * @return a future to be completed with the response
   */
  public CompletableFuture<OperationResponse> command(OperationRequest request) {
    CompletableFuture<OperationResponse> future = new CompletableFuture<>();
    if (context.isCurrentContext()) {
      sendRequest(request, protocol::command, future);
    } else {
      context.execute(() -> sendRequest(request, protocol::command, future));
    }
    return future;
  }

  /**
   * Sends a metadata request to the given node.
   *
   * @param request the request to send
   * @return a future to be completed with the response
   */
  public CompletableFuture<MetadataResponse> metadata(MetadataRequest request) {
    CompletableFuture<MetadataResponse> future = new CompletableFuture<>();
    if (context.isCurrentContext()) {
      sendRequest(request, protocol::metadata, future);
    } else {
      context.execute(() -> sendRequest(request, protocol::metadata, future));
    }
    return future;
  }

  /**
   * Sends the given request attempt to the cluster.
   */
  protected <T extends Message, U extends Message> void sendRequest(T request, BiFunction<MemberId, T, CompletableFuture<U>> sender, CompletableFuture<U> future) {
    sendRequest(request, sender, 0, future);
  }

  /**
   * Sends the given request attempt to the cluster.
   */
  protected <T extends Message, U extends Message> void sendRequest(T request, BiFunction<MemberId, T, CompletableFuture<U>> sender, int count, CompletableFuture<U> future) {
    MemberId node = next();
    if (node != null) {
      log.trace("Sending {} to {}", request, node);
      int selectionId = this.selectionId;
      sender.apply(node, request).whenCompleteAsync((r, e) -> {
        if (e != null || r != null) {
          handleResponse(request, sender, count, selectionId, node, r, e, future);
        } else {
          future.complete(null);
        }
      }, context);
    } else {
      future.completeExceptionally(new ConnectException("Failed to connect to the cluster"));
    }
  }

  /**
   * Resends a request due to a request failure, resetting the connection if necessary.
   */
  @SuppressWarnings("unchecked")
  protected <T extends Message> void retryRequest(Throwable cause, T request, BiFunction sender, int count, int selectionId, CompletableFuture future) {
    // If the connection has not changed, reset it and connect to the next server.
    if (this.selectionId == selectionId) {
      log.trace("Resetting connection. Reason: {}", cause.getMessage());
      this.currentNode = null;
    }

    // Attempt to send the request again.
    sendRequest(request, sender, count, future);
  }

  /**
   * Handles a response from the cluster.
   */
  @SuppressWarnings("unchecked")
  protected <T extends Message> void handleResponse(T request, BiFunction sender, int count, int selectionId, MemberId node, Message response, Throwable error, CompletableFuture future) {
    if (error == null) {
      log.trace("Received {} from {}", response, node);
      if (COMPLETE_PREDICATE.test(response)) {
        future.complete(response);
        selector.reset();
      } else {
        retryRequest(createException(response), request, sender, count + 1, selectionId, future);
      }
    } else {
      if (error instanceof CompletionException) {
        error = error.getCause();
      }
      log.debug("{} failed! Reason: {}", request, error);
      if (error instanceof SocketException || error instanceof TimeoutException || error instanceof ClosedChannelException) {
        if (count < selector.members().size() + 1) {
          retryRequest(error, request, sender, count + 1, selectionId, future);
        } else {
          future.completeExceptionally(error);
        }
      } else {
        future.completeExceptionally(error);
      }
    }
  }

  private Throwable createException(Message response) {
    RaftError error = (RaftError) response.getField(response.getDescriptorForType().findFieldByName("error"));
    switch (error) {
      case NO_LEADER:
        return new RaftException.NoLeader("No leader found");
      case QUERY_FAILURE:
        return new RaftException.QueryFailure("Query failed");
      case COMMAND_FAILURE:
        return new RaftException.CommandFailure("Command failed");
      case APPLICATION_ERROR:
        return new RaftException.ApplicationException("An application error occurred");
      case ILLEGAL_MEMBER_STATE:
        return new RaftException.IllegalMemberState("Illegal member state");
      case UNKNOWN_CLIENT:
        return new RaftException.UnknownClient("Unknown client");
      case UNKNOWN_SERVICE:
        return new PrimitiveException.UnknownService("Unknown service");
      case UNKNOWN_SESSION:
        return new PrimitiveException.UnknownSession("Unknown session");
      case CLOSED_SESSION:
        return new RaftException.ClosedSession("Closed session");
      case PROTOCOL_ERROR:
        return new RaftException.ProtocolException("A protocol error occurred");
      case CONFIGURATION_ERROR:
        return new RaftException.ConfigurationException("A configuration error occurred");
      case UNAVAILABLE:
        return new RaftException.Unavailable("The cluster is currently unavailable");
      default:
        return new AssertionError();
    }
  }

  /**
   * Connects to the cluster.
   */
  protected MemberId next() {
    // If a connection was already established then use that connection.
    if (currentNode != null) {
      return currentNode;
    }

    if (!selector.hasNext()) {
      if (selector.leader() != null) {
        selector.reset(null, selector.members());
        this.currentNode = selector.next();
        this.selectionId++;
        return currentNode;
      } else {
        log.debug("Failed to connect to the cluster");
        selector.reset();
        return null;
      }
    } else {
      this.currentNode = selector.next();
      this.selectionId++;
      return currentNode;
    }
  }
}
