package io.atomix.primitive.service;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;
import io.atomix.cluster.MemberId;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PartitionManagementService;
import io.atomix.primitive.session.Session;
import io.atomix.primitive.session.SessionId;
import io.atomix.primitive.session.SessionServerProtocol;
import io.atomix.primitive.session.impl.CloseSessionRequest;
import io.atomix.primitive.session.impl.CloseSessionResponse;
import io.atomix.primitive.session.impl.KeepAliveRequest;
import io.atomix.primitive.session.impl.KeepAliveResponse;
import io.atomix.primitive.session.impl.OpenSessionRequest;
import io.atomix.primitive.session.impl.OpenSessionResponse;
import io.atomix.primitive.session.impl.PrimitiveSession;
import io.atomix.primitive.session.impl.ServiceSession;
import io.atomix.primitive.session.impl.SessionCommandRequest;
import io.atomix.primitive.session.impl.SessionCommandResponse;
import io.atomix.primitive.session.impl.SessionContext;
import io.atomix.primitive.session.impl.SessionManagedServiceSnapshot;
import io.atomix.primitive.session.impl.SessionQueryRequest;
import io.atomix.primitive.session.impl.SessionQueryResponse;
import io.atomix.primitive.session.impl.SessionRequest;
import io.atomix.primitive.session.impl.SessionResponse;
import io.atomix.primitive.util.ByteArrayDecoder;
import io.atomix.utils.concurrent.Futures;

/**
 * Session managed primitive service.
 */
public abstract class SessionManagedPrimitiveService extends SimplePrimitiveService {
  private final SessionServerProtocol protocol;
  private final Map<SessionId, PrimitiveSession> sessions = new ConcurrentHashMap<>();
  private PrimitiveSession currentSession;
  private Context context;

  protected SessionManagedPrimitiveService(PartitionId partitionId, PartitionManagementService managementService) {
    this.protocol = managementService.getSessionProtocolService().getServerProtocol(partitionId);
  }

  @Override
  public void init(Context context) {
    super.init(context);
    this.context = context;
  }

  @Override
  public void snapshot(OutputStream output) throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    super.snapshot(outputStream);
    SessionManagedServiceSnapshot.newBuilder()
        .addAllSessions(sessions.values().stream()
            .map(session -> ServiceSession.newBuilder()
                .setSessionId(session.sessionId().id())
                .setMemberId(session.memberId().id())
                .setTimeout(session.timeout())
                .setTimestamp(session.getLastUpdated())
                .setCommandSequence(session.getCommandSequence())
                .setEventIndex(session.getEventIndex())
                .build())
            .collect(Collectors.toList()))
        .setSnapshot(ByteString.copyFrom(outputStream.toByteArray()))
        .build()
        .writeTo(output);
  }

  @Override
  public void install(InputStream input) throws IOException {
    SessionManagedServiceSnapshot snapshot = SessionManagedServiceSnapshot.parseFrom(input);
    sessions.values().forEach(session -> session.close());
    sessions.clear();
    snapshot.getSessionsList().forEach(session -> {
      SessionId sessionId = SessionId.from(session.getSessionId());
      sessions.put(sessionId, new PrimitiveSession(
          sessionId,
          MemberId.from(session.getMemberId()),
          session.getTimeout(),
          session.getTimestamp(),
          protocol,
          context));
    });
    ByteArrayInputStream inputStream = new ByteArrayInputStream(snapshot.getSnapshot().toByteArray());
    super.install(inputStream);
  }

  /**
   * Called when a session is opened.
   *
   * @param session the session that was opened
   */
  protected void onOpen(Session session) {

  }

  /**
   * Called when a session is expired.
   *
   * @param session the session that was expired
   */
  protected void onExpire(Session session) {

  }

  /**
   * Called when a session is closed.
   *
   * @param session the session that was closed
   */
  protected void onClose(Session session) {

  }

  /**
   * Returns the current session.
   *
   * @return the current session
   */
  protected Session getCurrentSession() {
    return currentSession;
  }

  /**
   * Sets the current session.
   *
   * @param session the current session
   */
  private void setCurrentSession(PrimitiveSession session) {
    this.currentSession = session;
  }

  /**
   * Returns the session with the given identifier.
   *
   * @param sessionId the session identifier
   * @return the primitive session
   */
  protected Session getSession(long sessionId) {
    return getSession(SessionId.from(sessionId));
  }

  /**
   * Returns the session with the given identifier.
   *
   * @param sessionId the session identifier
   * @return the primitive session
   */
  protected Session getSession(SessionId sessionId) {
    return sessions.get(sessionId);
  }

  /**
   * Returns the collection of open sessions.
   *
   * @return the collection of open sessions
   */
  @SuppressWarnings("unchecked")
  protected Collection<Session> getSessions() {
    return (Collection) sessions.values();
  }

  @Override
  public boolean canDelete(long index) {
    // Compute the lowest completed index for all sessions that belong to this state machine.
    long lastCompleted = index;
    for (PrimitiveSession session : sessions.values()) {
      long sessionLastCompleted = session.getLastCompleted();
      if (sessionLastCompleted > 0) {
        lastCompleted = Math.min(lastCompleted, sessionLastCompleted);
      }
    }
    return lastCompleted >= index;
  }

  @Override
  public CompletableFuture<byte[]> apply(Command<byte[]> command) {
    CompletableFuture<byte[]> future = applySessionRequest(command.map(bytes -> ByteArrayDecoder.decode(bytes, SessionRequest::parseFrom)))
        .thenApply(SessionResponse::toByteArray);
    future.whenComplete((response, error) -> commit());
    return future;
  }

  private CompletableFuture<SessionResponse> applySessionRequest(Command<SessionRequest> command) {
    if (command.value().hasCommand()) {
      return applySessionCommand(command.map(SessionRequest::getCommand))
          .thenApply(response -> SessionResponse.newBuilder()
              .setCommand(response)
              .build());
    } else if (command.value().hasOpenSession()) {
      return applyOpenSession(command.map(SessionRequest::getOpenSession))
          .thenApply(response -> SessionResponse.newBuilder()
              .setOpenSession(response)
              .build());
    } else if (command.value().hasKeepAlive()) {
      return applyKeepAlive(command.map(SessionRequest::getKeepAlive))
          .thenApply(response -> SessionResponse.newBuilder()
              .setKeepAlive(response)
              .build());
    } else if (command.value().hasCloseSession()) {
      return applyCloseSession(command.map(SessionRequest::getCloseSession))
          .thenApply(response -> SessionResponse.newBuilder()
              .setCloseSession(response)
              .build());
    } else {
      return Futures.exceptionalFuture(new PrimitiveException.ServiceException());
    }
  }

  private CompletableFuture<OpenSessionResponse> applyOpenSession(Command<OpenSessionRequest> openSession) {
    SessionId sessionId = SessionId.from(openSession.value().getSessionId());
    sessions.computeIfAbsent(sessionId, id -> {
      PrimitiveSession session = new PrimitiveSession(
          sessionId,
          MemberId.from(openSession.value().getMemberId()),
          openSession.value().getTimeout(),
          getCurrentTimestamp(),
          protocol,
          context);
      session.open();
      onOpen(session);
      return session;
    });
    return CompletableFuture.completedFuture(OpenSessionResponse.newBuilder().build());
  }

  private CompletableFuture<KeepAliveResponse> applyKeepAlive(Command<KeepAliveRequest> keepAlive) {
    PrimitiveSession session = sessions.get(SessionId.from(keepAlive.value().getSessionId()));
    if (session == null) {
      return Futures.exceptionalFuture(new PrimitiveException.UnknownSession());
    }

    // Update the session's timestamp to prevent it from being expired.
    session.setLastUpdated(getCurrentTimestamp());

    // Clear results cached in the session.
    session.clearResults(keepAlive.value().getCommandSequence());

    // Resend missing events starting from the last received event index.
    session.resendEvents(keepAlive.value().getEventIndex());

    // Expire sessions that have timed out.
    expireSessions();

    return CompletableFuture.completedFuture(KeepAliveResponse.newBuilder().build());
  }

  /**
   * Commits the application of a command to the state machine.
   */
  @SuppressWarnings("unchecked")
  private void commit() {
    for (PrimitiveSession session : sessions.values()) {
      session.commit(getCurrentIndex());
    }
  }

  /**
   * Expires sessions that have timed out.
   */
  private void expireSessions() {
    // Iterate through registered sessions.
    Iterator<Map.Entry<SessionId, PrimitiveSession>> iterator = sessions.entrySet().iterator();
    while (iterator.hasNext()) {
      PrimitiveSession session = iterator.next().getValue();
      if (session.isTimedOut(getCurrentTimestamp())) {
        getLogger().debug("Session expired in {} milliseconds: {}", getCurrentTimestamp() - session.getLastUpdated(), session);
        iterator.remove();
        session.expire();
        onExpire(session);
      }
    }
  }

  private CompletableFuture<CloseSessionResponse> applyCloseSession(Command<CloseSessionRequest> closeSession) {
    PrimitiveSession session = sessions.remove(SessionId.from(closeSession.value().getSessionId()));
    if (session == null) {
      return Futures.exceptionalFuture(new PrimitiveException.UnknownSession());
    }

    session.close();
    onClose(session);

    return CompletableFuture.completedFuture(CloseSessionResponse.newBuilder().build());
  }

  private CompletableFuture<SessionCommandResponse> applySessionCommand(Command<SessionCommandRequest> command) {
    PrimitiveSession session = sessions.get(SessionId.from(command.value().getSession().getSessionId()));
    if (session == null) {
      return Futures.exceptionalFuture(new PrimitiveException.UnknownSession());
    }

    long sequenceNumber = command.value().getSession().getSequenceNumber();

    // If the sequence number aligns with the next command sequence number, immediately execute the command.
    // Otherwise, enqueue the command for later.
    if (sequenceNumber == 0 || sequenceNumber == session.nextCommandSequence()) {
      CompletableFuture<SessionCommandResponse> future = new CompletableFuture<>();
      applySessionCommand(command, future);
      return future;
    } else if (sequenceNumber > session.nextCommandSequence()) {
      CompletableFuture<SessionCommandResponse> future = new CompletableFuture<>();
      session.registerSequenceCommand(sequenceNumber, () -> applySessionCommand(command, future));
      return future;
    } else {
      CompletableFuture<SessionCommandResponse> future = session.getResultFuture(sequenceNumber);
      if (future == null) {
        return Futures.exceptionalFuture(new PrimitiveException.CommandFailure());
      }
      return future;
    }
  }

  private void applySessionCommand(Command<SessionCommandRequest> command, CompletableFuture<SessionCommandResponse> future) {
    PrimitiveSession session = sessions.get(SessionId.from(command.value().getSession().getSessionId()));
    if (session == null) {
      future.completeExceptionally(new PrimitiveException.UnknownSession());
      return;
    }

    setCurrentSession(session);

    applyCommand(command.map(SessionCommandRequest::getRequest))
        .thenApply(response -> SessionCommandResponse.newBuilder()
            .setSession(SessionContext.newBuilder()
                .setIndex(getCurrentIndex())
                .setEventIndex(session.getEventIndex())
                .build())
            .setResponse(response)
            .build())
        .whenComplete((response, error) -> {
          long sequenceNumber = command.value().getSession().getSequenceNumber();
          session.registerResultFuture(sequenceNumber, future);
          session.setCommandSequence(sequenceNumber);
          if (error == null) {
            future.complete(response);
          } else {
            future.completeExceptionally(error);
          }
        });
  }

  @Override
  public CompletableFuture<byte[]> apply(Query<byte[]> query) {
    return applySessionRequest(query.map(bytes -> ByteArrayDecoder.decode(bytes, SessionRequest::parseFrom)))
        .thenApply(SessionResponse::toByteArray);
  }

  private CompletableFuture<SessionResponse> applySessionRequest(Query<SessionRequest> query) {
    return applySessionQuery(query.map(SessionRequest::getQuery))
        .thenApply(response -> SessionResponse.newBuilder()
            .setQuery(response)
            .build());
  }

  private CompletableFuture<SessionQueryResponse> applySessionQuery(Query<SessionQueryRequest> query) {
    PrimitiveSession session = sessions.get(SessionId.from(query.value().getSession().getSessionId()));
    if (session == null) {
      return Futures.exceptionalFuture(new PrimitiveException.UnknownSession());
    }

    CompletableFuture<SessionQueryResponse> future = new CompletableFuture<>();
    long sequenceNumber = query.value().getSession().getSequenceNumber();
    if (sequenceNumber > session.getCommandSequence()) {
      session.registerSequenceQuery(sequenceNumber, () -> applySessionQuery(query, future));
    } else {
      applySessionQuery(query, future);
    }
    return future;
  }

  private void applySessionQuery(Query<SessionQueryRequest> query, CompletableFuture<SessionQueryResponse> future) {
    PrimitiveSession session = sessions.get(SessionId.from(query.value().getSession().getSessionId()));
    if (session == null) {
      future.completeExceptionally(new PrimitiveException.UnknownSession());
      return;
    }

    setCurrentSession(session);
    applyQuery(query.map(SessionQueryRequest::getRequest))
        .thenApply(response -> SessionQueryResponse.newBuilder()
            .setSession(SessionContext.newBuilder()
                .setEventIndex(session.getEventIndex())
                .build())
            .setResponse(response)
            .build())
        .whenComplete((response, error) -> {
          if (error == null) {
            future.complete(response);
          } else {
            future.completeExceptionally(error);
          }
        });
  }
}
