package io.atomix.primitive.impl;

import java.util.concurrent.CompletableFuture;

import io.atomix.primitive.proxy.SessionEnabledPrimitiveProxy;
import io.atomix.primitive.session.impl.SessionCommandContext;
import io.atomix.primitive.session.impl.SessionResponseContext;
import io.atomix.primitive.session.impl.SessionStreamContext;
import io.atomix.utils.stream.StreamHandler;
import org.apache.commons.lang3.tuple.Pair;

@FunctionalInterface
public interface SessionCommandStreamFunction<P extends SessionEnabledPrimitiveProxy, T, U> {
  CompletableFuture<SessionResponseContext> execute(P proxy, SessionCommandContext session, T request, StreamHandler<Pair<SessionStreamContext, U>> handler);
}