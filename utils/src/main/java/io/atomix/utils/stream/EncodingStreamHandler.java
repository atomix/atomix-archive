package io.atomix.utils.stream;

import java.util.function.Function;

import io.atomix.utils.stream.StreamHandler;

/**
 * Transcoding stream handler.
 */
public class EncodingStreamHandler<T, U> implements StreamHandler<T> {
  private final StreamHandler<U> handler;
  private final Function<T, U> transcoder;

  public EncodingStreamHandler(StreamHandler<U> handler, Function<T, U> transcoder) {
    this.handler = handler;
    this.transcoder = transcoder;
  }

  @Override
  public void next(T value) {
    handler.next(transcoder.apply(value));
  }

  @Override
  public void complete() {
    handler.complete();
  }

  @Override
  public void error(Throwable error) {
    handler.error(error);
  }
}
