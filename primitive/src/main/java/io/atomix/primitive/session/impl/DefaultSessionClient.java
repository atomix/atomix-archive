package io.atomix.primitive.session.impl;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import com.google.protobuf.Message;
import io.atomix.primitive.event.EventType;
import io.atomix.primitive.operation.CommandId;
import io.atomix.primitive.operation.QueryId;
import io.atomix.primitive.partition.PartitionClient;
import io.atomix.primitive.service.impl.ListenRequest;
import io.atomix.primitive.service.impl.ServiceId;
import io.atomix.primitive.service.impl.ServiceRequest;
import io.atomix.primitive.service.impl.ServiceResponse;
import io.atomix.primitive.session.SessionClient;
import io.atomix.primitive.session.SessionClientProtocol;
import io.atomix.primitive.util.ByteArrayDecoder;
import io.atomix.primitive.util.ByteBufferDecoder;
import io.atomix.primitive.util.ByteStringEncoder;
import io.atomix.utils.concurrent.ThreadContext;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Default session client.
 */
public class DefaultSessionClient implements SessionClient {
  private final ServiceId serviceId;
  private final PartitionClient client;
  private final ThreadContext context;
  private final SessionClientProtocol protocol;
  private final Map<Long, SessionEventListener> eventListeners = new ConcurrentHashMap<>();

  public DefaultSessionClient(
      ServiceId serviceId,
      PartitionClient client,
      SessionClientProtocol protocol,
      ThreadContext context) {
    this.serviceId = serviceId;
    this.client = client;
    this.protocol = protocol;
    this.context = context;
  }

  @Override
  public CompletableFuture<OpenSessionResponse> openSession(OpenSessionRequest request) {
    return command(SessionRequest.newBuilder()
        .setOpenSession(request)
        .build())
        .thenApply(response -> response.getOpenSession());
  }

  @Override
  public CompletableFuture<KeepAliveResponse> keepAlive(KeepAliveRequest request) {
    return command(SessionRequest.newBuilder()
        .setKeepAlive(request)
        .build())
        .thenApply(response -> response.getKeepAlive());
  }

  @Override
  public CompletableFuture<CloseSessionResponse> closeSession(CloseSessionRequest request) {
    return command(SessionRequest.newBuilder()
        .setCloseSession(request)
        .build())
        .thenApply(response -> response.getCloseSession());
  }

  @Override
  public <T extends Message, U extends Message> CompletableFuture<Pair<SessionContext, U>> execute(
      CommandId<T, U> command, SessionCommandContext context, T request, ByteStringEncoder<T> encoder, ByteBufferDecoder<U> decoder) {
    return command(SessionRequest.newBuilder()
        .setCommand(SessionCommandRequest.newBuilder()
            .setContext(context)
            .setName(command.id())
            .setInput(ByteStringEncoder.encode(request, encoder))
            .build())
        .build())
        .thenApply(response -> response.getCommand())
        .thenApply(response -> Pair.of(response.getSession(), ByteBufferDecoder.decode(response.getOutput().asReadOnlyByteBuffer(), decoder)));
  }

  @Override
  public <T extends Message, U extends Message> CompletableFuture<Pair<SessionContext, U>> execute(
      QueryId<T, U> query, SessionQueryContext context, T request, ByteStringEncoder<T> encoder, ByteBufferDecoder<U> decoder) {
    return query(SessionRequest.newBuilder()
        .setQuery(SessionQueryRequest.newBuilder()
            .setContext(context)
            .setName(query.id())
            .setInput(ByteStringEncoder.encode(request, encoder))
            .build())
        .build())
        .thenApply(response -> response.getQuery())
        .thenApply(response -> Pair.of(response.getSession(), ByteBufferDecoder.decode(response.getOutput().asReadOnlyByteBuffer(), decoder)));
  }

  private CompletableFuture<SessionResponse> command(SessionRequest request) {
    return client.command(ServiceRequest.newBuilder()
        .setId(serviceId)
        .setRequest(request.toByteString())
        .build()
        .toByteArray())
        .thenApply(response -> ByteArrayDecoder.decode(response, ServiceResponse::parseFrom))
        .thenApply(response -> ByteBufferDecoder.decode(response.getResponse().asReadOnlyByteBuffer(), SessionResponse::parseFrom));
  }

  private CompletableFuture<SessionResponse> query(SessionRequest request) {
    return client.query(ServiceRequest.newBuilder()
        .setId(serviceId)
        .setRequest(request.toByteString())
        .build()
        .toByteArray())
        .thenApply(response -> ByteArrayDecoder.decode(response, ServiceResponse::parseFrom))
        .thenApply(response -> ByteBufferDecoder.decode(response.getResponse().asReadOnlyByteBuffer(), SessionResponse::parseFrom));
  }

  @Override
  public synchronized <T> void addEventListener(EventType eventType, SessionEventContext context, BiConsumer<EventContext, T> listener, ByteBufferDecoder<T> decoder) {
    SessionEventListener eventListener = eventListeners.computeIfAbsent(context.getSessionId(), SessionEventListener::new);
    eventListener.addListener(eventType, listener, decoder);
  }

  @Override
  public synchronized void removeEventListener(EventType eventType, SessionEventContext context) {
    SessionEventListener eventListener = eventListeners.get(context.getSessionId());
    if (eventListener != null) {
      eventListener.removeListener(eventType);
    }
  }

  private class SessionEventListener implements Consumer<EventRequest> {
    private final long sessionId;
    private final Map<String, BiConsumer<EventContext, PrimitiveEvent>> listeners = new ConcurrentHashMap<>();

    public SessionEventListener(long sessionId) {
      this.sessionId = sessionId;
    }

    synchronized <T> void addListener(EventType eventType, BiConsumer<EventContext, T> listener, ByteBufferDecoder<T> decoder) {
      listeners.put(eventType.id(), (context, event) ->
          listener.accept(context, ByteBufferDecoder.decode(event.getValue().asReadOnlyByteBuffer(), decoder)));
      if (listeners.size() == 1) {
        protocol.registerEventConsumer(ListenRequest.newBuilder().setSessionId(sessionId).build(), this, context);
      }
    }

    synchronized void removeListener(EventType eventType) {
      listeners.remove(eventType.id());
      if (listeners.isEmpty()) {
        protocol.unregisterEventConsumer(ListenRequest.newBuilder().setSessionId(sessionId).build());
      }
      eventListeners.remove(sessionId);
    }

    @Override
    public void accept(EventRequest eventRequest) {

    }
  }
}
