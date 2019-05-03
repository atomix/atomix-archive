package io.atomix.protocols.raft.partition.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.operation.OperationType;
import io.atomix.primitive.service.Command;
import io.atomix.primitive.service.Query;
import io.atomix.primitive.service.StateMachine;
import io.atomix.raft.RaftCommand;
import io.atomix.raft.RaftException;
import io.atomix.raft.RaftOperation;
import io.atomix.raft.RaftQuery;
import io.atomix.raft.RaftStateMachine;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.stream.StreamHandler;
import org.slf4j.Logger;

/**
 * Raft partition state machine.
 */
public class RaftPartitionStateMachine implements RaftStateMachine {
  private final StateMachine stateMachine;

  RaftPartitionStateMachine(StateMachine stateMachine) {
    this.stateMachine = stateMachine;
  }

  @Override
  public void init(RaftStateMachine.Context context) {
    stateMachine.init(new Context(context));
  }

  @Override
  public void snapshot(OutputStream output) throws IOException {
    stateMachine.snapshot(output);
  }

  @Override
  public void install(InputStream input) throws IOException {
    stateMachine.install(input);
  }

  @Override
  public boolean canDelete(long index) {
    return stateMachine.canDelete(index);
  }

  private Throwable convertException(Throwable error) {
    if (error instanceof CompletionException) {
      error = error.getCause();
    }
    if (error instanceof PrimitiveException.ServiceException) {
      return new RaftException.ApplicationException(error.getMessage());
    } else if (error instanceof PrimitiveException.CommandFailure) {
      return new RaftException.CommandFailure(error.getMessage());
    } else if (error instanceof PrimitiveException.QueryFailure) {
      return new RaftException.QueryFailure(error.getMessage());
    } else if (error instanceof PrimitiveException.UnknownSession) {
      return new RaftException.UnknownSession(error.getMessage());
    } else if (error instanceof PrimitiveException.UnknownService) {
      return new RaftException.UnknownService(error.getMessage());
    } else {
      return error;
    }
  }

  @Override
  public CompletableFuture<byte[]> apply(RaftCommand command) {
    return Futures.transformExceptions(
        stateMachine.apply(new Command<>(command.index(), command.timestamp(), command.value())),
        this::convertException);
  }

  @Override
  public CompletableFuture<Void> apply(RaftCommand command, StreamHandler<byte[]> handler) {
    return Futures.transformExceptions(
        stateMachine.apply(new Command<>(command.index(), command.timestamp(), command.value()), handler),
        this::convertException);
  }

  @Override
  public CompletableFuture<byte[]> apply(RaftQuery query) {
    return Futures.transformExceptions(
        stateMachine.apply(new Query<>(query.index(), query.timestamp(), query.value())),
        this::convertException);
  }

  @Override
  public CompletableFuture<Void> apply(RaftQuery query, StreamHandler<byte[]> handler) {
    return Futures.transformExceptions(
        stateMachine.apply(new Query<>(query.index(), query.timestamp(), query.value()), handler),
        this::convertException);
  }

  private static class Context implements StateMachine.Context {
    private final RaftStateMachine.Context context;

    Context(RaftStateMachine.Context context) {
      this.context = context;
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
      return context.getOperationType() == RaftOperation.Type.COMMAND
          ? OperationType.COMMAND
          : OperationType.QUERY;
    }

    @Override
    public Logger getLogger() {
      return context.getLogger();
    }
  }
}
