package io.atomix.primitive.session;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import com.google.protobuf.Message;
import io.atomix.primitive.event.EventType;
import io.atomix.primitive.operation.CommandId;
import io.atomix.primitive.operation.QueryId;
import io.atomix.primitive.session.impl.CloseSessionRequest;
import io.atomix.primitive.session.impl.CloseSessionResponse;
import io.atomix.primitive.session.impl.EventContext;
import io.atomix.primitive.session.impl.KeepAliveRequest;
import io.atomix.primitive.session.impl.KeepAliveResponse;
import io.atomix.primitive.session.impl.OpenSessionRequest;
import io.atomix.primitive.session.impl.OpenSessionResponse;
import io.atomix.primitive.session.impl.SessionCommandContext;
import io.atomix.primitive.session.impl.SessionContext;
import io.atomix.primitive.session.impl.SessionEventContext;
import io.atomix.primitive.session.impl.SessionQueryContext;
import io.atomix.primitive.util.ByteBufferDecoder;
import io.atomix.primitive.util.ByteStringEncoder;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Session client.
 */
public interface SessionClient {

  <T extends Message, U extends Message> CompletableFuture<Pair<SessionContext, U>> execute(
      CommandId<T, U> command, SessionCommandContext context, T request, ByteStringEncoder<T> encoder, ByteBufferDecoder<U> decoder);

  <T extends Message, U extends Message> CompletableFuture<Pair<SessionContext, U>> execute(
      QueryId<T, U> query, SessionQueryContext context, T request, ByteStringEncoder<T> encoder, ByteBufferDecoder<U> decoder);

  CompletableFuture<OpenSessionResponse> openSession(OpenSessionRequest request);

  CompletableFuture<KeepAliveResponse> keepAlive(KeepAliveRequest request);

  CompletableFuture<CloseSessionResponse> closeSession(CloseSessionRequest request);

  <T> void addEventListener(EventType eventType, SessionEventContext context, BiConsumer<EventContext, T> listener, ByteBufferDecoder<T> decoder);

  void removeEventListener(EventType eventType, SessionEventContext context);

}
