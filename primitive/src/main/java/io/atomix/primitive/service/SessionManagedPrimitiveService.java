package io.atomix.primitive.service;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.operation.CommandId;
import io.atomix.primitive.operation.OperationType;
import io.atomix.primitive.operation.QueryId;
import io.atomix.primitive.operation.StreamType;
import io.atomix.primitive.service.impl.DefaultServiceExecutor;
import io.atomix.primitive.service.impl.DefaultServiceScheduler;
import io.atomix.primitive.service.impl.OperationCodec;
import io.atomix.primitive.service.impl.ServiceCodec;
import io.atomix.primitive.session.Session;
import io.atomix.primitive.session.SessionId;
import io.atomix.primitive.session.SessionStreamHandler;
import io.atomix.primitive.session.StreamId;
import io.atomix.primitive.session.impl.CloseSessionRequest;
import io.atomix.primitive.session.impl.CloseSessionResponse;
import io.atomix.primitive.session.impl.KeepAliveRequest;
import io.atomix.primitive.session.impl.KeepAliveResponse;
import io.atomix.primitive.session.impl.OpenSessionRequest;
import io.atomix.primitive.session.impl.OpenSessionResponse;
import io.atomix.primitive.session.impl.PrimitiveSession;
import io.atomix.primitive.session.impl.PrimitiveSessionStream;
import io.atomix.primitive.session.impl.SessionCommandRequest;
import io.atomix.primitive.session.impl.SessionCommandResponse;
import io.atomix.primitive.session.impl.SessionManagedServiceSnapshot;
import io.atomix.primitive.session.impl.SessionQueryRequest;
import io.atomix.primitive.session.impl.SessionQueryResponse;
import io.atomix.primitive.session.impl.SessionRequest;
import io.atomix.primitive.session.impl.SessionResponse;
import io.atomix.primitive.session.impl.SessionResponseContext;
import io.atomix.primitive.session.impl.SessionSnapshot;
import io.atomix.primitive.session.impl.SessionStreamContext;
import io.atomix.primitive.session.impl.SessionStreamResponse;
import io.atomix.primitive.session.impl.SessionStreamSnapshot;
import io.atomix.primitive.util.ByteArrayDecoder;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.stream.EncodingStreamHandler;
import io.atomix.utils.stream.StreamHandler;

/**
 * Session managed primitive service.
 */
public abstract class SessionManagedPrimitiveService extends AbstractPrimitiveService implements PrimitiveService {
  private final Map<SessionId, PrimitiveSession> sessions = new ConcurrentHashMap<>();
  private PrimitiveSession currentSession;
  private ServiceCodec codec;
  private AbstractPrimitiveService.Context context;
  private DefaultServiceExecutor executor;
  private DefaultServiceScheduler scheduler;

  @Override
  public void init(StateMachine.Context context) {
    this.codec = new ServiceCodec();
    this.context = new AbstractPrimitiveService.Context(context);
    this.executor = new DefaultServiceExecutor(codec, this.context.getLogger());
    this.scheduler = new DefaultServiceScheduler(this.context);
    super.init(this.context, scheduler, scheduler);
    configure(executor);
  }

  @Override
  public void snapshot(OutputStream output) throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    super.snapshot(outputStream);
    SessionManagedServiceSnapshot.newBuilder()
        .addAllSessions(sessions.values().stream()
            .map(session -> SessionSnapshot.newBuilder()
                .setSessionId(session.sessionId().id())
                .setTimeout(session.timeout())
                .setTimestamp(session.getLastUpdated())
                .setCommandSequence(session.getCommandSequence())
                .setLastApplied(session.getLastApplied())
                .addAllStreams(session.getStreams().stream()
                    .map(stream -> SessionStreamSnapshot.newBuilder()
                        .setStreamId(stream.id().streamId())
                        .setType(stream.type().id())
                        .setSequenceNumber(stream.getCurrentSequence())
                        .setLastCompleted(stream.getCompleteSequence())
                        .build())
                    .collect(Collectors.toList()))
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
    snapshot.getSessionsList().forEach(sessionSnapshot -> {
      SessionId sessionId = SessionId.from(sessionSnapshot.getSessionId());
      PrimitiveSession session = new PrimitiveSession(
          sessionId,
          sessionSnapshot.getTimeout(),
          sessionSnapshot.getTimestamp(),
          codec,
          context);
      session.setCommandSequence(sessionSnapshot.getCommandSequence());
      session.setLastApplied(sessionSnapshot.getLastApplied());

      for (SessionStreamSnapshot streamSnapshot : sessionSnapshot.getStreamsList()) {
        StreamType<?> type = new StreamType<>(streamSnapshot.getType());
        PrimitiveSessionStream stream = session.addStream(streamSnapshot.getStreamId(), type);
        stream.setCurrentSequence(streamSnapshot.getSequenceNumber());
        stream.setCompleteSequence(streamSnapshot.getLastCompleted());
      }
      sessions.put(sessionId, session);
    });
    ByteArrayInputStream inputStream = new ByteArrayInputStream(snapshot.getSnapshot().toByteArray());
    super.install(inputStream);
  }

  /**
   * Returns the collection of streams for all sessions.
   *
   * @return a collection of open streams for all sessions
   */
  protected Collection<StreamHandler> getStreams() {
    return sessions.values().stream()
        .flatMap(session -> session.getStreams().stream())
        .collect(Collectors.toList());
  }

  /**
   * Returns the given stream.
   *
   * @param streamId the stream ID
   * @param <T>      the stream type
   * @return the stream handler
   */
  protected <T> StreamHandler<T> getStream(StreamId streamId) {
    return sessions.get(streamId.sessionId()).getStream(streamId.streamId());
  }

  /**
   * Returns the streams for the given session.
   *
   * @param sessionId the session ID
   * @return the streams for the given session
   */
  @SuppressWarnings("unchecked")
  protected Collection<SessionStreamHandler> getStreams(SessionId sessionId) {
    return (Collection) sessions.get(sessionId).getStreams();
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
      for (PrimitiveSessionStream stream : session.getStreams()) {
        lastCompleted = Math.min(lastCompleted, stream.getCompleteIndex());
      }
    }
    return lastCompleted >= index;
  }

  @Override
  public CompletableFuture<byte[]> apply(Command<byte[]> command) {
    // Set the operation context to ensure scheduling is allowed.
    context.setOperationType(OperationType.COMMAND);

    // Run tasks scheduled to execute prior to this command.
    scheduler.runScheduledTasks(command.timestamp());

    try {
      return applySessionRequest(
          command.map(bytes -> ByteArrayDecoder.decode(bytes, SessionRequest::parseFrom)))
          .thenApply(SessionResponse::toByteArray);
    } finally {
      // Run tasks pending to be executed after this command.
      scheduler.runPendingTasks();
    }
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
          openSession.value().getTimeout(),
          getCurrentTimestamp(),
          codec,
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
    keepAlive.value().getStreamsMap().forEach((streamId, streamSequence) -> {
      PrimitiveSessionStream stream = session.getStream(streamId);
      if (stream != null) {
        stream.resendEvents(streamSequence);
      }
    });

    // Expire sessions that have timed out.
    expireSessions();

    return CompletableFuture.completedFuture(KeepAliveResponse.newBuilder().build());
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
    PrimitiveSession session = sessions.get(SessionId.from(command.value().getContext().getSessionId()));
    if (session == null) {
      return Futures.exceptionalFuture(new PrimitiveException.UnknownSession());
    }

    // If the sequence number is specified and is less than the current command sequence number for the session,
    // that indicates the command has already been executed and we can return a cached result.
    long sequenceNumber = command.value().getContext().getSequenceNumber();
    if (sequenceNumber != 0 && sequenceNumber <= session.getCommandSequence()) {
      return session.getResultFuture(sequenceNumber);
    }

    // If we've made it this far, apply the command in sequential order.
    CompletableFuture<SessionCommandResponse> future = new CompletableFuture<>();
    applySequenceCommand(command, session, future);
    return future;
  }

  private void applySequenceCommand(
      Command<SessionCommandRequest> command,
      PrimitiveSession session,
      CompletableFuture<SessionCommandResponse> future) {
    long sequenceNumber = command.value().getContext().getSequenceNumber();

    // If the sequence number aligns with the next command sequence number, immediately execute the command.
    // Otherwise, enqueue the command for later.
    if (sequenceNumber > session.nextCommandSequence()) {
      session.registerSequenceCommand(sequenceNumber, () -> applySessionCommand(command, session, future));
    } else {
      applySessionCommand(command, session, future);
    }
  }

  private void applySessionCommand(
      Command<SessionCommandRequest> command,
      PrimitiveSession session,
      CompletableFuture<SessionCommandResponse> future) {
    // Set the current session for usage in the service.
    setCurrentSession(session);

    // Create stream contexts prior to executing the command to ensure we're
    // sending the stream state prior to this command.
    List<SessionStreamContext> streams = session.getStreams()
        .stream()
        .map(stream -> SessionStreamContext.newBuilder()
            .setStreamId(stream.id().streamId())
            .setIndex(stream.getLastIndex())
            .setSequence(stream.getCurrentSequence())
            .build())
        .collect(Collectors.toList());

    // Create the operation and apply it.
    OperationExecutor operation = executor.getExecutor(new CommandId<>(command.value().getName()));

    byte[] output;
    try {
      output = operation.execute(command.value().getInput().toByteArray());
    } catch (Exception e) {
      long sequenceNumber = command.value().getContext().getSequenceNumber();
      session.registerResultFuture(sequenceNumber, future);
      session.setCommandSequence(sequenceNumber);
      session.setLastApplied(getCurrentIndex());
      future.completeExceptionally(e);
      return;
    }

    long sequenceNumber = command.value().getContext().getSequenceNumber();
    session.registerResultFuture(sequenceNumber, future);
    session.setCommandSequence(sequenceNumber);
    session.setLastApplied(getCurrentIndex());

    future.complete(SessionCommandResponse.newBuilder()
        .setContext(SessionResponseContext.newBuilder()
            .setIndex(getCurrentIndex())
            .setSequence(session.getCommandSequence())
            .addAllStreams(streams)
            .build())
        .setOutput(ByteString.copyFrom(output))
        .build());
  }

  @Override
  public CompletableFuture<Void> apply(Command<byte[]> command, StreamHandler<byte[]> handler) {
    return applySessionCommand(
        command.map(bytes -> ByteArrayDecoder.decode(bytes, SessionRequest::parseFrom).getCommand()),
        new EncodingStreamHandler<>(handler, SessionResponse::toByteArray));
  }

  private CompletableFuture<Void> applySessionCommand(
      Command<SessionCommandRequest> command,
      StreamHandler<SessionResponse> handler) {
    PrimitiveSession session = sessions.get(SessionId.from(command.value().getContext().getSessionId()));
    if (session == null) {
      return Futures.exceptionalFuture(new PrimitiveException.UnknownSession());
    }

    // If the sequence number is specified and is less than the current command sequence number for the session,
    // that indicates the command has already been executed and we can return a cached stream.
    long sequenceNumber = command.value().getContext().getSequenceNumber();
    if (sequenceNumber != 0 && sequenceNumber <= session.getCommandSequence()) {
      CompletableFuture<SessionCommandResponse> future = session.getResultFuture(sequenceNumber);
      PrimitiveSessionStream stream = session.getStream(sequenceNumber);
      if (future != null && stream != null) {
        stream.handler(new EncodingStreamHandler<SessionStreamResponse, SessionResponse>(
            handler,
            response -> SessionResponse.newBuilder()
                .setStream(response)
                .build()));
        return future.thenAccept(response -> handler.next(SessionResponse.newBuilder()
            .setCommand(response)
            .build()));
      }
    }

    // If we've made it this far, apply the command in sequential order.
    CompletableFuture<Void> future = new CompletableFuture<>();
    applySequenceCommand(command, session, handler, future);
    return future;
  }

  private void applySequenceCommand(
      Command<SessionCommandRequest> command,
      PrimitiveSession session,
      StreamHandler<SessionResponse> handler,
      CompletableFuture<Void> future) {
    long sequenceNumber = command.value().getContext().getSequenceNumber();

    // If the sequence number aligns with the next command sequence number, immediately execute the command.
    // Otherwise, enqueue the command for later.
    if (sequenceNumber > session.nextCommandSequence()) {
      session.registerSequenceCommand(sequenceNumber, () -> applySessionCommand(command, session, handler, future));
    } else {
      applySessionCommand(command, session, handler, future);
    }
  }

  @SuppressWarnings("unchecked")
  private void applySessionCommand(
      Command<SessionCommandRequest> command,
      PrimitiveSession session,
      StreamHandler<SessionResponse> handler,
      CompletableFuture<Void> future) {
    // Set the current session for usage in the service.
    setCurrentSession(session);

    // Create the operation and apply it.
    long sequenceNumber = command.value().getContext().getSequenceNumber();

    // Create stream contexts prior to executing the command to ensure we're
    // sending the stream state prior to this command.
    // Prepend the stream to the list of streams so it's easily identifiable by the client.
    List<SessionStreamContext> streams = new LinkedList<>();
    streams.add(SessionStreamContext.newBuilder()
        .setStreamId(sequenceNumber)
        .setIndex(getCurrentIndex())
        .build());
    session.getStreams().forEach(stream -> streams.add(SessionStreamContext.newBuilder()
        .setStreamId(stream.id().streamId())
        .setIndex(stream.getLastIndex())
        .setSequence(stream.getCurrentSequence())
        .build()));

    // Create the stream.
    CommandId<?, ?> commandId = new CommandId<>(command.value().getName());
    OperationExecutor operation = executor.getExecutor(commandId);
    OperationCodec codec = this.codec.getOperation(commandId);
    StreamType<?> streamType = executor.getStreamType(commandId);
    PrimitiveSessionStream stream = session.addStream(sequenceNumber, streamType);

    // Add the stream handler to the stream.
    stream.handler(new EncodingStreamHandler<SessionStreamResponse, SessionResponse>(
        handler,
        response -> SessionResponse.newBuilder()
            .setStream(response)
            .build()));

    // Initialize the stream handler with the session response.
    SessionCommandResponse response = SessionCommandResponse.newBuilder()
        .setContext(SessionResponseContext.newBuilder()
            .setIndex(getCurrentIndex())
            .setSequence(sequenceNumber)
            .addAllStreams(streams)
            .build())
        .build();

    // Immediately send the command response/context on the stream.
    handler.next(SessionResponse.newBuilder()
        .setCommand(response)
        .build());

    try {
      // Execute the command.
      operation.execute(codec.decode(command.value().getInput().toByteArray()), stream);
    } catch (Exception e) {
      future.completeExceptionally(e);
    }

    // Update the session's sequence number and last applied index.
    session.setCommandSequence(sequenceNumber);
    session.setLastApplied(getCurrentIndex());
    session.registerResultFuture(sequenceNumber, CompletableFuture.completedFuture(response));
    future.complete(null);
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
    PrimitiveSession session = sessions.get(SessionId.from(query.value().getContext().getSessionId()));
    if (session == null) {
      return Futures.exceptionalFuture(new PrimitiveException.UnknownSession());
    }

    CompletableFuture<SessionQueryResponse> future = new CompletableFuture<>();
    applyIndexQuery(query, session, future);
    return future;
  }

  private void applyIndexQuery(
      Query<SessionQueryRequest> query,
      PrimitiveSession session,
      CompletableFuture<SessionQueryResponse> future) {
    long lastIndex = query.value().getContext().getLastIndex();
    if (lastIndex > getCurrentIndex()) {
      session.registerIndexQuery(lastIndex, () -> applySequenceQuery(query, session, future));
    } else {
      applySequenceQuery(query, session, future);
    }
  }

  private void applySequenceQuery(
      Query<SessionQueryRequest> query,
      PrimitiveSession session,
      CompletableFuture<SessionQueryResponse> future) {
    long lastSequenceNumber = query.value().getContext().getLastSequenceNumber();
    if (lastSequenceNumber > session.getCommandSequence()) {
      session.registerSequenceQuery(lastSequenceNumber, () -> applySessionQuery(query, session, future));
    } else {
      applySessionQuery(query, session, future);
    }
  }

  private void applySessionQuery(
      Query<SessionQueryRequest> query,
      PrimitiveSession session,
      CompletableFuture<SessionQueryResponse> future) {
    setCurrentSession(session);

    // Create stream contexts prior to executing the command to ensure we're
    // sending the stream state prior to this command.
    List<SessionStreamContext> streams = session.getStreams()
        .stream()
        .map(stream -> SessionStreamContext.newBuilder()
            .setStreamId(stream.id().streamId())
            .setIndex(stream.getLastIndex())
            .setSequence(stream.getCurrentSequence())
            .build())
        .collect(Collectors.toList());

    // Get the query executor.
    QueryId<?, ?> queryId = new QueryId<>(query.value().getName());
    OperationExecutor operation = executor.getExecutor(queryId);

    // Execute the query.
    byte[] output;
    try {
      output = operation.execute(query.value().getInput().toByteArray());
    } catch (Exception e) {
      future.completeExceptionally(e);
      return;
    }

    // Complete the query future to send the response.
    future.complete(SessionQueryResponse.newBuilder()
        .setContext(SessionResponseContext.newBuilder()
            .setIndex(getCurrentIndex())
            .setSequence(session.getCommandSequence())
            .addAllStreams(streams)
            .build())
        .setOutput(ByteString.copyFrom(output))
        .build());
  }

  @Override
  public CompletableFuture<Void> apply(Query<byte[]> query, StreamHandler<byte[]> handler) {
    return applySessionQuery(
        query.map(bytes -> ByteArrayDecoder.decode(bytes, SessionRequest::parseFrom).getQuery()),
        new EncodingStreamHandler<>(handler, SessionResponse::toByteArray));
  }

  private CompletableFuture<Void> applySessionQuery(
      Query<SessionQueryRequest> query,
      StreamHandler<SessionResponse> handler) {
    PrimitiveSession session = sessions.get(SessionId.from(query.value().getContext().getSessionId()));
    if (session == null) {
      return Futures.exceptionalFuture(new PrimitiveException.UnknownSession());
    }

    CompletableFuture<Void> future = new CompletableFuture<>();
    applyIndexQuery(query, session, handler, future);
    return future;
  }

  private void applyIndexQuery(
      Query<SessionQueryRequest> query,
      PrimitiveSession session,
      StreamHandler<SessionResponse> handler,
      CompletableFuture<Void> future) {
    long lastIndex = query.value().getContext().getLastIndex();
    if (lastIndex > getCurrentIndex()) {
      session.registerIndexQuery(lastIndex, () -> applySequenceQuery(query, session, handler, future));
    } else {
      applySequenceQuery(query, session, handler, future);
    }
  }

  private void applySequenceQuery(
      Query<SessionQueryRequest> query,
      PrimitiveSession session,
      StreamHandler<SessionResponse> handler,
      CompletableFuture<Void> future) {
    long lastSequenceNumber = query.value().getContext().getLastSequenceNumber();
    if (lastSequenceNumber > session.getCommandSequence()) {
      session.registerSequenceQuery(lastSequenceNumber, () -> applySessionQuery(query, session, handler, future));
    } else {
      applySessionQuery(query, session, handler, future);
    }
  }

  private void applySessionQuery(
      Query<SessionQueryRequest> query,
      PrimitiveSession session,
      StreamHandler<SessionResponse> handler,
      CompletableFuture<Void> future) {
    setCurrentSession(session);

    // Initialize the stream handler with the query response.
    handler.next(SessionResponse.newBuilder()
        .setQuery(SessionQueryResponse.newBuilder()
            .setContext(SessionResponseContext.newBuilder()
                .setIndex(getCurrentIndex())
                .setSequence(session.getCommandSequence())
                .addAllStreams(session.getStreams().stream()
                    .map(s -> SessionStreamContext.newBuilder()
                        .setStreamId(s.id().streamId())
                        .setIndex(s.getLastIndex())
                        .setSequence(s.getCurrentSequence())
                        .build())
                    .collect(Collectors.toList()))
                .build())
            .build())
        .build());

    // Execute the operation and wrap stream events in a SessionResponse.
    QueryId<?, ?> queryId = new QueryId<>(query.value().getName());
    OperationExecutor operation = executor.getExecutor(queryId);
    operation.execute(
        query.value().getInput().toByteArray(),
        new EncodingStreamHandler<byte[], SessionResponse>(handler, response -> SessionResponse.newBuilder()
            .setStream(SessionStreamResponse.newBuilder()
                .setContext(SessionStreamContext.newBuilder()
                    .setIndex(getCurrentIndex())
                    .build())
                .setValue(ByteString.copyFrom(response))
                .build())
            .build()));
    future.complete(null);
  }
}
