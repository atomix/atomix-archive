package io.atomix.utils.stream;

/**
 * Message stream.
 */
public interface StreamHandler<T> {

  /**
   * Called to handle the next value.
   *
   * @param value the value to handle
   */
  void next(T value);

  /**
   * Called when the stream is complete.
   */
  void complete();

  /**
   * Called when a stream error occurs.
   *
   * @param error the stream error
   */
  void error(Throwable error);

}
