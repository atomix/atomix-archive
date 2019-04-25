package io.atomix.primitive.service.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.operation.OperationId;
import io.atomix.primitive.service.Command;
import io.atomix.primitive.service.Query;
import io.atomix.primitive.service.ServiceOperationRegistry;
import io.atomix.primitive.util.ByteArrayDecoder;
import io.atomix.primitive.util.ByteArrayEncoder;
import io.atomix.utils.StreamHandler;
import org.slf4j.Logger;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.atomix.primitive.util.ByteArrayDecoder.decode;
import static io.atomix.primitive.util.ByteArrayEncoder.encode;

public class DefaultServiceExecutor extends DefaultServiceScheduler implements ServiceOperationRegistry {
  private final Logger log;
  private final Map<String, Function<byte[], byte[]>> operations = new HashMap<>();
  private final Map<String, BiConsumer<byte[], StreamHandler<byte[]>>> streamOperations = new HashMap<>();

  public DefaultServiceExecutor(Logger log) {
    super(log);
    this.log = log;
  }

  /**
   * Applies the given command to the service.
   *
   * @param operationId the command ID
   * @param command     the command
   * @return the command result
   */
  public byte[] apply(OperationId operationId, Command<byte[]> command) {
    log.trace("Executing {}", command);
    startCommand(command.timestamp());
    try {
      return apply(operationId, command.value());
    } finally {
      completeCommand();
    }
  }

  /**
   * Applies the given command to the service.
   *
   * @param operationId the command ID
   * @param command     the command
   * @param handler     the stream handler
   */
  public void apply(OperationId operationId, Command<byte[]> command, StreamHandler<byte[]> handler) {
    log.trace("Executing {}", command);
    startCommand(command.timestamp());
    try {
      apply(operationId, command.value(), handler);
    } finally {
      completeCommand();
    }
  }

  /**
   * Applies the given query to the service.
   *
   * @param operationId the query ID
   * @param query       the query
   * @return the query result
   */
  public byte[] apply(OperationId operationId, Query<byte[]> query) {
    log.trace("Executing {}", query);
    startQuery();
    try {
      return apply(operationId, query.value());
    } finally {
      completeQuery();
    }
  }

  /**
   * Applies the given query to the service.
   *
   * @param operationId the query ID
   * @param query       the query
   * @param handler     the stream handler
   */
  public void apply(OperationId operationId, Query<byte[]> query, StreamHandler<byte[]> handler) {
    log.trace("Executing {}", query);
    startQuery();
    try {
      apply(operationId, query.value(), handler);
    } finally {
      completeQuery();
    }
  }

  /**
   * Applies the given value to the given operation.
   *
   * @param operationId the operation ID
   * @param value       the operation value
   * @return the operation result
   */
  private byte[] apply(OperationId operationId, byte[] value) {
    // Look up the registered callback for the operation.
    Function<byte[], byte[]> operation = operations.get(operationId.id());

    if (operation == null) {
      throw new IllegalStateException("Unknown state machine operation: " + operationId);
    } else {
      // Execute the operation. If the operation return value is a Future, await the result,
      // otherwise immediately complete the execution future.
      try {
        return operation.apply(value);
      } catch (Exception e) {
        log.warn("State machine operation failed: {}", e.getMessage());
        throw new PrimitiveException.ServiceException(e);
      }
    }
  }

  /**
   * Applies the given value to the given operation.
   *
   * @param operationId the operation ID
   * @param value       the operation value
   * @param handler     the stream handler
   */
  private void apply(OperationId operationId, byte[] value, StreamHandler<byte[]> handler) {
    // Look up the registered callback for the operation.
    BiConsumer<byte[], StreamHandler<byte[]>> operation = streamOperations.get(operationId.id());

    if (operation == null) {
      throw new IllegalStateException("Unknown state machine operation: " + operationId);
    } else {
      // Execute the operation. If the operation return value is a Future, await the result,
      // otherwise immediately complete the execution future.
      try {
        operation.accept(value, handler);
      } catch (Exception e) {
        log.warn("State machine operation failed: {}", e.getMessage());
        throw new PrimitiveException.ServiceException(e);
      }
    }
  }

  private void handle(OperationId operationId, Function<byte[], byte[]> callback) {
    checkNotNull(operationId, "operationId cannot be null");
    checkNotNull(callback, "callback cannot be null");
    operations.put(operationId.id(), callback);
    log.trace("Registered operation callback {}", operationId);
  }

  @Override
  public void register(OperationId operationId, Runnable callback) {
    checkNotNull(operationId, "operationId cannot be null");
    checkNotNull(callback, "callback cannot be null");
    handle(operationId, commit -> {
      callback.run();
      return null;
    });
  }

  @Override
  public <R> void register(OperationId operationId, Supplier<R> callback, ByteArrayEncoder<R> encoder) {
    checkNotNull(operationId, "operationId cannot be null");
    checkNotNull(callback, "callback cannot be null");
    handle(operationId, commit -> encode(callback.get(), encoder));
  }

  @Override
  public <T> void register(OperationId operationId, Consumer<T> callback, ByteArrayDecoder<T> decoder) {
    checkNotNull(operationId, "operationId cannot be null");
    checkNotNull(callback, "callback cannot be null");
    handle(operationId, bytes -> {
      callback.accept(decode(bytes, decoder));
      return null;
    });
  }

  @Override
  public <T, R> void register(OperationId operationId, Function<T, R> callback, ByteArrayDecoder<T> decoder, ByteArrayEncoder<R> encoder) {
    checkNotNull(operationId, "operationId cannot be null");
    checkNotNull(callback, "callback cannot be null");
    handle(operationId, bytes -> encode(callback.apply(decode(bytes, decoder)), encoder));
  }

  @Override
  public <R> void register(OperationId operationId, Consumer<StreamHandler<R>> callback, ByteArrayEncoder<R> encoder) {
    checkNotNull(operationId, "operationId cannot be null");
    checkNotNull(callback, "callback cannot be null");
    streamOperations.put(operationId.id(), (bytes, handler) -> callback.accept(new StreamHandler<R>() {
      @Override
      public void next(R value) {
        handler.next(encode(value, encoder));
      }

      @Override
      public void complete() {
        handler.complete();
      }

      @Override
      public void error(Throwable error) {
        handler.error(error);
      }
    }));
    log.trace("Registered operation callback {}", operationId);
  }

  @Override
  public <T, R> void register(OperationId operationId, BiConsumer<T, StreamHandler<R>> callback, ByteArrayDecoder<T> decoder, ByteArrayEncoder<R> encoder) {
    checkNotNull(operationId, "operationId cannot be null");
    checkNotNull(callback, "callback cannot be null");
    streamOperations.put(operationId.id(), (bytes, handler) -> callback.accept(decode(bytes, decoder), new StreamHandler<R>() {
      @Override
      public void next(R value) {
        handler.next(encode(value, encoder));
      }

      @Override
      public void complete() {
        handler.complete();
      }

      @Override
      public void error(Throwable error) {
        handler.error(error);
      }
    }));
    log.trace("Registered operation callback {}", operationId);
  }
}