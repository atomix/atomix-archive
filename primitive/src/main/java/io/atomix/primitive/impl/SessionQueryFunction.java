package io.atomix.primitive.impl;

import java.util.concurrent.CompletableFuture;

import io.atomix.primitive.proxy.SessionEnabledPrimitiveProxy;
import io.atomix.primitive.session.impl.SessionQueryContext;
import io.atomix.primitive.session.impl.SessionResponseContext;
import org.apache.commons.lang3.tuple.Pair;

@FunctionalInterface
public interface SessionQueryFunction<P extends SessionEnabledPrimitiveProxy, T, U> {
  CompletableFuture<Pair<SessionResponseContext, U>> execute(P proxy, SessionQueryContext session, T request);
}