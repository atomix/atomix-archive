/*
 * Copyright 2019-present Open Networking Foundation
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
package io.atomix.primitive.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.CompletableFuture;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.MemberId;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.PrimitiveId;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.PrimitiveTypeRegistry;
import io.atomix.primitive.operation.OperationMetadata;
import io.atomix.primitive.operation.OperationType;
import io.atomix.primitive.operation.PrimitiveOperation;
import io.atomix.primitive.service.CloseSessionRequest;
import io.atomix.primitive.service.CloseSessionResponse;
import io.atomix.primitive.service.Command;
import io.atomix.primitive.service.KeepAliveRequest;
import io.atomix.primitive.service.KeepAliveResponse;
import io.atomix.primitive.service.OpenSessionRequest;
import io.atomix.primitive.service.OpenSessionResponse;
import io.atomix.primitive.service.OperationRequest;
import io.atomix.primitive.service.OperationResponse;
import io.atomix.primitive.service.PrimitiveRequest;
import io.atomix.primitive.service.PrimitiveResponse;
import io.atomix.primitive.service.Query;
import io.atomix.primitive.service.ServiceSnapshot;
import io.atomix.primitive.service.StateMachine;
import io.atomix.primitive.session.SessionId;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.concurrent.ThreadContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Default state machine.
 */
public class PrimitiveStateMachine implements StateMachine {
  private final Logger log = LoggerFactory.getLogger(getClass());
  private final PrimitiveTypeRegistry registry;
  private final ClusterMembershipService membershipService;
  private final ClusterCommunicationService communicationService;
  private final ThreadContext threadContext;
  private final ThreadContextFactory threadContextFactory;
  private final PrimitiveServiceRegistry services = new PrimitiveServiceRegistry();
  private final PrimitiveSessionRegistry sessions = new PrimitiveSessionRegistry();
  private Context context;

  public PrimitiveStateMachine(
      PrimitiveTypeRegistry registry,
      ClusterMembershipService membershipService,
      ClusterCommunicationService communicationService,
      ThreadContext threadContext,
      ThreadContextFactory threadContextFactory) {
    this.registry = checkNotNull(registry);
    this.membershipService = checkNotNull(membershipService);
    this.communicationService = checkNotNull(communicationService);
    this.threadContext = checkNotNull(threadContext);
    this.threadContextFactory = checkNotNull(threadContextFactory);
  }

  @Override
  public void init(Context context) {
    this.context = context;
  }

  @Override
  public void backup(OutputStream output) {
    for (PrimitiveServiceContext service : services) {
      backupService(output, service);
    }
  }

  /**
   * Takes a snapshot of the given service.
   *
   * @param output  the snapshot output
   * @param service the service to snapshot
   */
  private void backupService(OutputStream output, PrimitiveServiceContext service) {
    try {
      service.takeSnapshot().writeDelimitedTo(output);
    } catch (IOException e) {
      log.error("Failed to snapshot service {}", service.serviceName(), e);
    }
  }

  @Override
  public void restore(InputStream input) {
    try {
      while (input.available() > 0) {
        restoreService(input);
      }
    } catch (IOException e) {
      log.error("Failed to read snapshot", e);
    }
  }

  /**
   * Restores the service associated with the given snapshot.
   *
   * @param input the snapshot input
   */
  private void restoreService(InputStream input) throws IOException {
    ServiceSnapshot snapshot = ServiceSnapshot.parseDelimitedFrom(input);
    PrimitiveServiceContext service = initializeService(
        PrimitiveId.from(snapshot.getId()),
        registry.getPrimitiveType(snapshot.getType()),
        snapshot.getName());
    if (service != null) {
      try {
        service.installSnapshot(snapshot);
      } catch (Exception e) {
        log.error("Failed to install snapshot for service {}", snapshot.getName(), e);
      }
    }
  }

  /**
   * Gets or initializes a service context.
   */
  private PrimitiveServiceContext getOrInitializeService(PrimitiveId primitiveId, PrimitiveType primitiveType, String serviceName) {
    // Get the state machine executor or create one if it doesn't already exist.
    PrimitiveServiceContext service = services.getService(serviceName);
    if (service == null) {
      service = initializeService(primitiveId, primitiveType, serviceName);
    }
    return service;
  }

  /**
   * Initializes a new service.
   */
  @SuppressWarnings("unchecked")
  private PrimitiveServiceContext initializeService(PrimitiveId primitiveId, PrimitiveType primitiveType, String serviceName) {
    PrimitiveServiceContext oldService = services.getService(serviceName);
    PrimitiveServiceContext service = new PrimitiveServiceContext(
        membershipService.getLocalMember().id(),
        primitiveId,
        serviceName,
        primitiveType,
        primitiveType.newService(),
        sessions,
        communicationService,
        context,
        threadContext,
        threadContextFactory);
    services.registerService(service);

    // If a service with this name was already registered, remove all of its sessions.
    if (oldService != null) {
      sessions.removeSessions(oldService.serviceId());
    }
    return service;
  }

  @Override
  public boolean canDelete(long index) {
    // Compute the lowest completed index for all sessions that belong to this state machine.
    long lastCompleted = index;
    for (PrimitiveSession session : sessions.getSessions()) {
      lastCompleted = Math.min(lastCompleted, session.getLastCompleted());
    }
    return lastCompleted >= index;
  }

  private static PrimitiveRequest parseRequest(byte[] bytes) {
    try {
      return PrimitiveRequest.parseFrom(bytes);
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public CompletableFuture<byte[]> apply(Command<byte[]> command) {
    PrimitiveRequest request = parseRequest(command.value());
    if (request.hasOperation() && request.getOperation().hasCommand()) {
      return applyCommand(command.map(value -> request.getOperation()))
          .thenApply(response -> PrimitiveResponse.newBuilder()
              .setOperation(response)
              .build())
          .thenApply(PrimitiveResponse::toByteArray);
    } else if (request.hasKeepAlive()) {
      return applyKeepAlive(command.map(value -> request.getKeepAlive()))
          .thenApply(response -> PrimitiveResponse.newBuilder()
              .setKeepAlive(response)
              .build())
          .thenApply(PrimitiveResponse::toByteArray);
    } else if (request.hasOpenSession()) {
      return applyOpenSession(command.map(value -> request.getOpenSession()))
          .thenApply(response -> PrimitiveResponse.newBuilder()
              .setOpenSession(response)
              .build())
          .thenApply(PrimitiveResponse::toByteArray);
    } else if (request.hasCloseSession()) {
      return applyCloseSession(command.map(value -> request.getCloseSession()))
          .thenApply(response -> PrimitiveResponse.newBuilder()
              .setCloseSession(response)
              .build())
          .thenApply(PrimitiveResponse::toByteArray);
    } else {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }
  }

  @Override
  public CompletableFuture<byte[]> apply(Query<byte[]> query) {
    PrimitiveRequest request = parseRequest(query.value());
    if (request.hasOperation() && request.getOperation().hasQuery()) {
      return applyQuery(query.map(value -> request.getOperation()))
          .thenApply(response -> PrimitiveResponse.newBuilder()
              .setOperation(response)
              .build())
          .thenApply(PrimitiveResponse::toByteArray);
    } else {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }
  }

  private CompletableFuture<OperationResponse> applyCommand(Command<OperationRequest> command) {
    // First check to ensure that the session exists.
    PrimitiveSession session = sessions.getSession(command.value().getSessionId());

    // If the session is null, return an UnknownSessionException. Commands applied to the state machine must
    // have a session. We ensure that session register/unregister entries are not compacted from the log
    // until all associated commands have been cleaned.
    // Note that it's possible for a session to be unknown if a later snapshot has been taken, so we don't want
    // to log warnings here.
    if (session == null) {
      log.debug("Unknown session: " + command.value().getSessionId());
      throw new PrimitiveException.UnknownSession("unknown session: " + command.value().getSessionId());
    }

    // Execute the command using the state machine associated with the session.
    return session.getService()
        .executeCommand(
            command.index(),
            command.value().getSequenceNumber(),
            command.timestamp(),
            session,
            PrimitiveOperation.newBuilder()
                .setMetadata(OperationMetadata.newBuilder()
                    .setType(OperationType.COMMAND)
                    .setName(command.value().getCommand().getName())
                    .build())
                .setValue(command.value().getCommand().getCommand())
                .build())
        .thenApply(result -> OperationResponse.newBuilder()
            .setIndex(result.index())
            .setEventIndex(result.eventIndex())
            .setOutput(ByteString.copyFrom(result.result()))
            .build());
  }

  private CompletableFuture<OpenSessionResponse> applyOpenSession(Command<OpenSessionRequest> openSession) {
    PrimitiveType primitiveType = registry.getPrimitiveType(openSession.value().getPrimitiveType());

    // Get the state machine executor or create one if it doesn't already exist.
    PrimitiveServiceContext service = getOrInitializeService(
        PrimitiveId.from(openSession.index()),
        primitiveType,
        openSession.value().getPrimitiveName());

    if (service == null) {
      throw new PrimitiveException.UnknownService("Unknown service type " + openSession.value().getPrimitiveType());
    }

    SessionId sessionId = SessionId.from(openSession.index());
    PrimitiveSession session = sessions.addSession(new PrimitiveSession(
        sessionId,
        MemberId.from(openSession.value().getMemberId()),
        openSession.value().getPrimitiveName(),
        primitiveType,
        openSession.value().getTimeout(),
        openSession.timestamp(),
        communicationService,
        service,
        threadContextFactory));

    return CompletableFuture.completedFuture(service.openSession(openSession.index(), openSession.timestamp(), session))
        .thenApply(id -> OpenSessionResponse.newBuilder()
            .setSessionId(id)
            .setTimeout(openSession.value().getTimeout())
            .build());

  }

  private CompletableFuture<KeepAliveResponse> applyKeepAlive(Command<KeepAliveRequest> command) {
    PrimitiveSession session = sessions.getSession(command.value().getSessionId());

    // If the server session is null, the session either never existed or already expired.
    if (session == null) {
      throw new PrimitiveException.UnknownSession("Unknown session: " + command.value().getSessionId());
    }

    if (!session.getService().keepAlive(command.index(), command.timestamp(), session, command.value().getCommandSequence(), command.value().getEventIndex())) {
      throw new PrimitiveException.UnknownService("Unknown service " + session.getService().serviceName());
    }

    expireOrphanSessions(command.timestamp());

    return CompletableFuture.completedFuture(KeepAliveResponse.newBuilder().build());
  }

  /**
   * Expires sessions that have timed out.
   */
  private void expireOrphanSessions(long timestamp) {
    // Iterate through registered sessions.
    for (PrimitiveSession session : sessions.getSessions()) {
      if (session.getService().deleted() && session.isTimedOut(timestamp)) {
        log.debug("Orphaned session expired in {} milliseconds: {}", timestamp - session.getLastUpdated(), session);
        session = sessions.removeSession(session.sessionId());
        if (session != null) {
          session.expire();
        }
      }
    }
  }

  private CompletableFuture<CloseSessionResponse> applyCloseSession(Command<CloseSessionRequest> command) {
    PrimitiveSession session = sessions.getSession(command.value().getSessionId());

    // If the server session is null, the session either never existed or already expired.
    if (session == null) {
      throw new PrimitiveException.UnknownSession("Unknown session: " + command.value().getSessionId());
    }

    PrimitiveServiceContext service = session.getService();
    service.closeSession(command.index(), command.timestamp(), session);

    // If this is a delete, unregister the service.
    if (command.value().getDelete()) {
      services.unregisterService(service);
      service.close();
    }

    return CompletableFuture.completedFuture(CloseSessionResponse.newBuilder().build());
  }

  private CompletableFuture<OperationResponse> applyQuery(Query<OperationRequest> query) {
    PrimitiveSession session = sessions.getSession(query.value().getSessionId());

    // If the session is null then that indicates that the session already timed out or it never existed.
    // Return with an UnknownSessionException.
    if (session == null) {
      log.warn("Unknown session: " + query.value().getSessionId());
      return Futures.exceptionalFuture(new PrimitiveException.UnknownSession("unknown session " + query.value().getSessionId()));
    }

    // Execute the query using the state machine associated with the session.
    return session.getService()
        .executeQuery(
            query.value().getQuery().getIndex(),
            query.value().getSequenceNumber(),
            query.timestamp(),
            session,
            PrimitiveOperation.newBuilder()
                .setMetadata(OperationMetadata.newBuilder()
                    .setType(OperationType.QUERY)
                    .setName(query.value().getQuery().getName())
                    .build())
                .setValue(query.value().getQuery().getQuery())
                .build())
        .thenApply(result -> OperationResponse.newBuilder()
            .setIndex(result.index())
            .setEventIndex(result.eventIndex())
            .setOutput(ByteString.copyFrom(result.result()))
            .build());
  }
}
