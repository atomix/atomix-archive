package io.atomix.primitive.service;

import io.atomix.utils.stream.StreamHandler;

/**
 * Operation executor.
 */
public interface OperationExecutor<T, U> {

  /**
   * Executes the operation.
   *
   * @param request the encoded request
   * @return the encoded response
   */
  byte[] execute(byte[] request);

  /**
   * Executes the operation.
   *
   * @param request the encoded request
   * @param handler the response handler
   */
  void execute(byte[] request, StreamHandler<byte[]> handler);

  /**
   * Executes the operation.
   *
   * @param request the request
   * @return the response
   */
  U execute(T request);

  /**
   * Executes the operation.
   *
   * @param request the request
   * @param handler the response handler
   */
  void execute(T request, StreamHandler<U> handler);

}
