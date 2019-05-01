package io.atomix.protocols.log.partition.impl;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import io.atomix.primitive.log.LogRecord;
import io.atomix.primitive.log.LogSession;
import io.atomix.primitive.operation.OperationType;
import io.atomix.primitive.partition.PartitionClient;
import io.atomix.primitive.service.Command;
import io.atomix.primitive.service.Query;
import io.atomix.primitive.service.StateMachine;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.stream.StreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Distributed log session client.
 */
public class LogPartitionClient implements PartitionClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(LogPartitionClient.class);
  private final LogSession client;
  private final StateMachine stateMachine;
  private final ThreadContext stateContext;
  private final ThreadContext threadContext;
  private final Map<Long, StreamHandler<byte[]>> streams = new ConcurrentHashMap<>();
  private final Map<Long, CompletableFuture> futures = new ConcurrentHashMap<>();
  private final AtomicLong currentIndex = new AtomicLong();
  private final AtomicLong currentTimestamp = new AtomicLong();
  private volatile OperationType currentOperationType;

  public LogPartitionClient(
      LogSession client,
      StateMachine stateMachine,
      ThreadContext stateContext,
      ThreadContext threadContext) {
    this.client = client;
    this.stateMachine = stateMachine;
    this.stateContext = stateContext;
    this.threadContext = threadContext;
    stateContext.execute(() -> stateMachine.init(new Context()));
    client.consumer().consume(record -> stateContext.execute(() -> consume(record)));
  }

  @Override
  public CompletableFuture<byte[]> command(byte[] value) {
    CompletableFuture<byte[]> future = new CompletableFuture<>();
    client.producer().append(value).thenAccept(index -> futures.put(index, future));
    return future;
  }

  @Override
  public CompletableFuture<Void> command(byte[] value, StreamHandler<byte[]> handler) {
    return client.producer().append(value)
        .thenAccept(index -> streams.put(index, handler));
  }

  @Override
  public CompletableFuture<byte[]> query(byte[] value) {
    CompletableFuture<byte[]> future = new CompletableFuture<>();
    stateContext.execute(() -> {
      currentOperationType = OperationType.QUERY;
      stateMachine.apply(new Query<>(currentIndex.get(), currentTimestamp.get(), value))
          .whenComplete((result, error) -> {
            threadContext.execute(() -> {
              if (error == null) {
                future.complete(result);
              } else {
                future.completeExceptionally(error);
              }
            });
          });
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> query(byte[] value, StreamHandler<byte[]> handler) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    stateContext.execute(() -> {
      currentOperationType = OperationType.QUERY;
      stateMachine.apply(new Query<>(currentIndex.get(), currentTimestamp.get(), value), handler)
          .whenComplete((result, error) -> {
            if (error == null) {
              future.complete(result);
            } else {
              future.completeExceptionally(error);
            }
          });
    });
    return future;
  }

  /**
   * Consumes a record from the log.
   *
   * @param record the record to consume
   */
  @SuppressWarnings("unchecked")
  private void consume(LogRecord record) {
    currentIndex.set(record.getIndex());
    long timestamp = currentTimestamp.accumulateAndGet(record.getTimestamp(), Math::max);
    currentOperationType = OperationType.COMMAND;
    StreamHandler<byte[]> stream = streams.get(record.getIndex());
    if (stream != null) {
      stateMachine.apply(new Command<>(record.getIndex(), timestamp, record.getValue().toByteArray()), stream)
          .whenComplete((result, error) -> {
            CompletableFuture<Void> future = futures.remove(record.getIndex());
            if (future != null) {
              threadContext.execute(() -> {
                if (error == null) {
                  future.complete(null);
                } else {
                  future.completeExceptionally(error);
                }
              });
            }
          });
    } else {
      stateMachine.apply(new Command<>(record.getIndex(), timestamp, record.getValue().toByteArray()))
          .whenComplete((result, error) -> {
            CompletableFuture<byte[]> future = futures.remove(record.getIndex());
            if (future != null) {
              threadContext.execute(() -> {
                if (error == null) {
                  future.complete(result);
                } else {
                  future.completeExceptionally(error);
                }
              });
            }
          });
    }
  }

  /**
   * Log state machine context.
   */
  private class Context implements StateMachine.Context {
    @Override
    public long getIndex() {
      return currentIndex.get();
    }

    @Override
    public long getTimestamp() {
      return currentTimestamp.get();
    }

    @Override
    public OperationType getOperationType() {
      return currentOperationType;
    }

    @Override
    public Logger getLogger() {
      return LOGGER;
    }
  }
}
