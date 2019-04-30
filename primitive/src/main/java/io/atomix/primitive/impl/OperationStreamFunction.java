package io.atomix.primitive.impl;

import java.util.concurrent.CompletableFuture;

import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.primitive.service.impl.RequestContext;
import io.atomix.primitive.service.impl.StreamContext;
import io.atomix.utils.stream.StreamHandler;
import org.apache.commons.lang3.tuple.Pair;

@FunctionalInterface
public interface OperationStreamFunction<P extends PrimitiveProxy, T, U> {
  CompletableFuture<Void> execute(P proxy, RequestContext context, T request, StreamHandler<Pair<StreamContext, U>> handler);
}