package io.atomix.primitive.service;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import com.google.protobuf.ByteString;
import io.atomix.primitive.util.ByteArrayDecoder;
import io.atomix.primitive.operation.OperationId;
import io.atomix.primitive.operation.OperationType;
import io.atomix.primitive.operation.impl.DefaultOperationId;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.service.impl.CommandRequest;
import io.atomix.primitive.service.impl.CommandResponse;
import io.atomix.primitive.service.impl.DefaultServiceExecutor;
import io.atomix.primitive.service.impl.QueryRequest;
import io.atomix.primitive.service.impl.QueryResponse;
import io.atomix.utils.concurrent.Scheduler;
import org.slf4j.Logger;

/**
 * Simple primitive service.
 */
public abstract class SimplePrimitiveService implements PrimitiveService {
  private Context context;
  private ServiceExecutor executor;
  private final Map<Long, List<Runnable>> indexQueries = new HashMap<>();

  protected SimplePrimitiveService(PartitionId partitionId) {
  }

  @Override
  public void init(Context context) {
    this.context = context;
    this.executor = new DefaultServiceExecutor(context.getLogger());
  }

  /**
   * Configures the service operations.
   *
   * @param executor the service executor
   */
  protected abstract void configure(ServiceExecutor executor);

  /**
   * Returns the current index.
   *
   * @return the current index
   */
  protected long getCurrentIndex() {
    return context.getIndex();
  }

  /**
   * Returns the current timestamp.
   *
   * @return the current timestamp
   */
  protected long getCurrentTimestamp() {
    return context.getTimestamp();
  }

  /**
   * Returns the deterministic service scheduler.
   *
   * @return the deterministic service scheduler
   */
  protected Scheduler getScheduler() {
    return executor;
  }

  /**
   * Returns the deterministic service executor.
   *
   * @return the deterministic service executor
   */
  protected Executor getExecutor() {
    return executor;
  }

  /**
   * Returns the service logger.
   *
   * @return the service logger
   */
  protected Logger getLogger() {
    return context.getLogger();
  }

  @Override
  public void snapshot(OutputStream output) throws IOException {
    backup(output);
  }

  @Override
  public void install(InputStream input) throws IOException {
    restore(input);
  }

  /**
   * Backs up the state of the service.
   *
   * @param output the output to which to backup the service
   */
  protected abstract void backup(OutputStream output) throws IOException;

  /**
   * Restores the state of the service.
   *
   * @param input the input from which to restore the service
   */
  protected abstract void restore(InputStream input) throws IOException;

  @Override
  public boolean canDelete(long index) {
    return true;
  }

  @Override
  public CompletableFuture<byte[]> apply(Command<byte[]> command) {
    return applyCommand(command.map(bytes -> ByteArrayDecoder.decode(bytes, CommandRequest::parseFrom)))
        .thenApply(CommandResponse::toByteArray);
  }

  protected CompletableFuture<CommandResponse> applyCommand(Command<CommandRequest> command) {
    OperationId operationId = new DefaultOperationId(command.value().getName(), OperationType.COMMAND);
    byte[] output = executor.apply(operationId, command.map(r -> r.getCommand().toByteArray()));
    return CompletableFuture.completedFuture(CommandResponse.newBuilder()
        .setIndex(getCurrentIndex())
        .setOutput(ByteString.copyFrom(output))
        .build());
  }

  @Override
  public CompletableFuture<byte[]> apply(Query<byte[]> query) {
    return applyQuery(query.map(bytes -> ByteArrayDecoder.decode(bytes, QueryRequest::parseFrom)))
        .thenApply(QueryResponse::toByteArray);
  }

  protected CompletableFuture<QueryResponse> applyQuery(Query<QueryRequest> query) {
    CompletableFuture<QueryResponse> future = new CompletableFuture<>();
    if (query.value().getIndex() > getCurrentIndex()) {
      indexQueries.computeIfAbsent(query.value().getIndex(), index -> new LinkedList<>())
          .add(() -> applyQuery(query, future));
    } else {
      applyQuery(query, future);
    }
    return future;
  }

  private void applyQuery(Query<QueryRequest> request, CompletableFuture<QueryResponse> future) {
    OperationId operationId = new DefaultOperationId(request.value().getName(), OperationType.QUERY);
    byte[] output = executor.apply(operationId, request.map(r -> r.getQuery().toByteArray()));
    future.complete(QueryResponse.newBuilder()
        .setIndex(getCurrentIndex())
        .setOutput(ByteString.copyFrom(output))
        .build());
  }
}
