package io.atomix.core.impl;

import java.util.function.Function;

import io.grpc.stub.StreamObserver;

/**
 * Transcoding stream observer.
 */
public class TranscodingStreamObserver<T, U> implements StreamObserver<T> {
  private final StreamObserver<U> handler;
  private final Function<T, U> transcoder;

  public TranscodingStreamObserver(StreamObserver<U> handler, Function<T, U> transcoder) {
    this.handler = handler;
    this.transcoder = transcoder;
  }

  @Override
  public void onNext(T value) {
    handler.onNext(transcoder.apply(value));
  }

  @Override
  public void onCompleted() {
    handler.onCompleted();
  }

  @Override
  public void onError(Throwable error) {
    handler.onError(error);
  }
}
