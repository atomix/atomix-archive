package io.atomix.protocols.log.partition.impl;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import io.atomix.primitive.log.LogRecord;
import io.atomix.primitive.log.LogSession;
import io.atomix.primitive.partition.PartitionClient;
import io.atomix.primitive.service.Command;
import io.atomix.primitive.service.Query;
import io.atomix.primitive.service.StateMachine;
import io.atomix.utils.StreamHandler;
import io.atomix.utils.concurrent.ThreadContext;

/**
 * Distributed log session client.
 */
public class LogPrimitiveClient implements PartitionClient {
  private final LogSession client;
  private final StateMachine stateMachine;
  private final ThreadContext stateContext;
  private final ThreadContext threadContext;
  private final Map<Long, StreamHandler<byte[]>> streams = new ConcurrentHashMap<>();
  private final Map<Long, CompletableFuture<byte[]>> futures = new ConcurrentHashMap<>();
  private final AtomicLong currentIndex = new AtomicLong();
  private final AtomicLong currentTimestamp = new AtomicLong();

  public LogPrimitiveClient(
      LogSession client,
      StateMachine stateMachine,
      ThreadContext stateContext,
      ThreadContext threadContext) {
    this.client = client;
    this.stateMachine = stateMachine;
    this.stateContext = stateContext;
    this.threadContext = threadContext;
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
    stateContext.execute(() -> stateMachine.apply(new Query<>(currentIndex.get(), currentTimestamp.get(), value))
        .whenComplete((result, error) -> {
          threadContext.execute(() -> {
            if (error == null) {
              future.complete(result);
            } else {
              future.completeExceptionally(error);
            }
          });
        }));
    return future;
  }

  @Override
  public CompletableFuture<Void> query(byte[] value, StreamHandler<byte[]> handler) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    stateContext.execute(() -> stateMachine.apply(new Query<>(currentIndex.get(), currentTimestamp.get(), value), handler)
        .whenComplete((result, error) -> {
          if (error == null) {
            future.complete(result);
          } else {
            future.completeExceptionally(error);
          }
        }));
    return future;
  }

  /**
   * Consumes a record from the log.
   *
   * @param record the record to consume
   */
  private void consume(LogRecord record) {
    currentIndex.set(record.getIndex());
    long timestamp = currentTimestamp.accumulateAndGet(record.getTimestamp(), Math::max);
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
