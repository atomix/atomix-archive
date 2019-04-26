package io.atomix.utils.stream;

/**
 * Stream function.
 */
public interface StreamFunction<T, U> {

  /**
   * Called to handle the next value.
   *
   * @param value the value to handle
   */
  void next(T value);

  /**
   * Called when the stream is complete.
   *
   * @return the result
   */
  U complete();

  /**
   * Called when a stream error occurs.
   *
   * @param error the stream error
   * @return the result
   */
  U error(Throwable error);

}
