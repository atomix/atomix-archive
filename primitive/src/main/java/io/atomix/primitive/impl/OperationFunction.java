package io.atomix.primitive.impl;

import java.util.concurrent.CompletableFuture;

import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.primitive.service.impl.RequestContext;
import io.atomix.primitive.service.impl.ResponseContext;
import org.apache.commons.lang3.tuple.Pair;

@FunctionalInterface
public interface OperationFunction<P extends PrimitiveProxy, T, U> {
  CompletableFuture<Pair<ResponseContext, U>> execute(P proxy, RequestContext context, T request);
}