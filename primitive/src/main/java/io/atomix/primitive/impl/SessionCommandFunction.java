package io.atomix.primitive.impl;

import java.util.concurrent.CompletableFuture;

import io.atomix.primitive.proxy.SessionEnabledPrimitiveProxy;
import io.atomix.primitive.session.impl.SessionCommandContext;
import io.atomix.primitive.session.impl.SessionResponseContext;
import org.apache.commons.lang3.tuple.Pair;

@FunctionalInterface
public interface SessionCommandFunction<P extends SessionEnabledPrimitiveProxy, T, U> {
  CompletableFuture<Pair<SessionResponseContext, U>> execute(P proxy, SessionCommandContext session, T request);
}