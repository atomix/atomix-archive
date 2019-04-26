package io.atomix.primitive.service.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.operation.CommandId;
import io.atomix.primitive.operation.OperationId;
import io.atomix.primitive.operation.OperationType;
import io.atomix.primitive.operation.QueryId;
import io.atomix.primitive.service.Command;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.Query;
import io.atomix.primitive.service.ServiceOperationRegistry;
import io.atomix.primitive.service.StateMachine;
import io.atomix.primitive.util.ByteArrayDecoder;
import io.atomix.primitive.util.ByteArrayEncoder;
import io.atomix.utils.stream.StreamHandler;
import org.slf4j.Logger;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.atomix.primitive.util.ByteArrayDecoder.decode;
import static io.atomix.primitive.util.ByteArrayEncoder.encode;

public class DefaultServiceExecutor extends DefaultServiceScheduler implements ServiceOperationRegistry, PrimitiveService.Context {
  private final StateMachine.Context context;
  private final Map<String, Function<byte[], byte[]>> operations = new HashMap<>();
  private final Map<String, BiConsumer<byte[], StreamHandler<byte[]>>> streamOperations = new HashMap<>();
  private final Map<String, ByteArrayEncoder> encoders = new HashMap<>();
  private OperationId operationId;

  public DefaultServiceExecutor(StateMachine.Context context) {
    super(context.getLogger());
    this.context = context;
  }

  @Override
  public OperationId getOperationId() {
    return operationId;
  }

  @Override
  public long getIndex() {
    return context.getIndex();
  }

  @Override
  public long getTimestamp() {
    return context.getTimestamp();
  }

  @Override
  public OperationType getOperationType() {
    return operationId.type();
  }

  @Override
  public Logger getLogger() {
    return context.getLogger();
  }

  /**
   * Applies the given command to the service.
   *
   * @param commandId the command ID
   * @param command     the command
   * @return the command result
   */
  public byte[] apply(CommandId commandId, Command<byte[]> command) {
    getLogger().trace("Executing {}", command);
    startCommand(commandId, command.timestamp());
    try {
      return apply(commandId, command.value());
    } finally {
      completeCommand();
    }
  }

  /**
   * Applies the given command to the service.
   *
   * @param commandId the command ID
   * @param command     the command
   * @param handler     the stream handler
   */
  public void apply(CommandId commandId, Command<byte[]> command, StreamHandler<byte[]> handler) {
    getLogger().trace("Executing {}", command);
    startCommand(commandId, command.timestamp());
    try {
      apply(commandId, command.value(), handler);
    } finally {
      completeCommand();
    }
  }

  protected void startCommand(CommandId commandId, long timestamp) {
    operationId = commandId;
    startCommand(timestamp);
  }

  @Override
  protected void completeCommand() {
    operationId = null;
  }

  /**
   * Applies the given query to the service.
   *
   * @param queryId the query ID
   * @param query       the query
   * @return the query result
   */
  public byte[] apply(QueryId queryId, Query<byte[]> query) {
    getLogger().trace("Executing {}", query);
    startQuery(queryId);
    try {
      return apply(queryId, query.value());
    } finally {
      completeQuery();
    }
  }

  /**
   * Applies the given query to the service.
   *
   * @param queryId the query ID
   * @param query       the query
   * @param handler     the stream handler
   */
  public void apply(QueryId queryId, Query<byte[]> query, StreamHandler<byte[]> handler) {
    getLogger().trace("Executing {}", query);
    startQuery(queryId);
    try {
      apply(queryId, query.value(), handler);
    } finally {
      completeQuery();
    }
  }

  protected void startQuery(QueryId queryId) {
    this.operationId = queryId;
  }

  @Override
  protected void completeQuery() {
    operationId = null;
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
        getLogger().warn("State machine operation failed: {}", e.getMessage());
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
        getLogger().warn("State machine operation failed: {}", e.getMessage());
        throw new PrimitiveException.ServiceException(e);
      }
    }
  }

  private void handle(OperationId operationId, Function<byte[], byte[]> callback) {
    checkNotNull(operationId, "operationId cannot be null");
    checkNotNull(callback, "callback cannot be null");
    operations.put(operationId.id(), callback);
    getLogger().trace("Registered operation callback {}", operationId);
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
    encoders.put(operationId.id(), encoder);
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
    getLogger().trace("Registered operation callback {}", operationId);
  }

  @Override
  public <T, R> void register(OperationId operationId, BiConsumer<T, StreamHandler<R>> callback, ByteArrayDecoder<T> decoder, ByteArrayEncoder<R> encoder) {
    checkNotNull(operationId, "operationId cannot be null");
    checkNotNull(callback, "callback cannot be null");
    encoders.put(operationId.id(), encoder);
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
    getLogger().trace("Registered operation callback {}", operationId);
  }

  /**
   * Returns the encoder for the given operation.
   *
   * @param operationId the operation ID
   * @param <T>         the operation type
   * @return the operation encoder
   */
  @SuppressWarnings("unchecked")
  public <T> ByteArrayEncoder<T> encoder(OperationId operationId) {
    return encoders.get(operationId.id());
  }
}