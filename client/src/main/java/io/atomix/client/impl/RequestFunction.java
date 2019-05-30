package io.atomix.client.impl;

import io.grpc.stub.StreamObserver;

/**
 * Request function.
 */
@FunctionalInterface
public interface RequestFunction<S, T, U> {
  void apply(S service, T header, StreamObserver<U> observer);
}
