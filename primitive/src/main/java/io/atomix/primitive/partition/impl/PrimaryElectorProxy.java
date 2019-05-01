package io.atomix.primitive.partition.impl;

import java.util.concurrent.CompletableFuture;

import io.atomix.primitive.partition.PrimaryElectionEvent;
import io.atomix.primitive.proxy.SessionEnabledPrimitiveProxy;
import io.atomix.primitive.session.impl.SessionCommandContext;
import io.atomix.primitive.session.impl.SessionQueryContext;
import io.atomix.primitive.session.impl.SessionResponseContext;
import io.atomix.primitive.session.impl.SessionStreamContext;
import io.atomix.utils.stream.StreamHandler;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Primary election proxy.
 */
public class PrimaryElectorProxy extends SessionEnabledPrimitiveProxy {
  PrimaryElectorProxy(Context context) {
    super(context);
  }

  public CompletableFuture<Pair<SessionResponseContext, EnterResponse>> enter(
      SessionCommandContext context, EnterRequest request) {
    return getClient().execute(
        PrimaryElectorOperations.ENTER,
        context,
        request,
        EnterRequest::toByteString,
        EnterResponse::parseFrom);
  }

  public CompletableFuture<Pair<SessionResponseContext, GetTermResponse>> getTerm(
      SessionQueryContext context, GetTermRequest request) {
    return getClient().execute(
        PrimaryElectorOperations.GET_TERM,
        context,
        request,
        GetTermRequest::toByteString,
        GetTermResponse::parseFrom);
  }

  public CompletableFuture<SessionResponseContext> stream(
      SessionCommandContext context,
      ListenRequest request,
      StreamHandler<Pair<SessionStreamContext, PrimaryElectionEvent>> handler) {
    return getClient().execute(
        PrimaryElectorOperations.STREAM_EVENTS,
        PrimaryElectorOperations.EVENT_STREAM,
        context,
        request,
        handler,
        ListenRequest::toByteString,
        PrimaryElectionEvent::parseFrom);
  }
}
