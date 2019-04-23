package io.atomix.primitive.service;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.google.protobuf.ByteString;
import io.atomix.primitive.operation.OperationId;
import io.atomix.primitive.operation.OperationType;
import io.atomix.primitive.operation.impl.DefaultOperationId;
import io.atomix.primitive.service.impl.CommandRequest;
import io.atomix.primitive.service.impl.CommandResponse;
import io.atomix.primitive.service.impl.DefaultServiceExecutor;
import io.atomix.primitive.service.impl.QueryRequest;
import io.atomix.primitive.service.impl.QueryResponse;
import io.atomix.primitive.util.ByteArrayDecoder;

/**
 * Simple primitive service.
 */
public abstract class SimplePrimitiveService extends AbstractPrimitiveService implements PrimitiveService {
  private Context context;
  private ServiceExecutor executor;
  private final Map<Long, List<Runnable>> indexQueries = new HashMap<>();

  @Override
  public void init(Context context) {
    this.context = context;
    this.executor = new DefaultServiceExecutor(context.getLogger());
  }

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
