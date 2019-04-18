package io.atomix.protocols.raft.partition.impl;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.CompletableFuture;

import io.atomix.primitive.service.Command;
import io.atomix.primitive.service.Query;
import io.atomix.primitive.service.Role;
import io.atomix.primitive.service.StateMachine;
import io.atomix.raft.RaftCommand;
import io.atomix.raft.RaftQuery;
import io.atomix.raft.RaftStateMachine;

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
  public void snapshot(OutputStream output) {
    stateMachine.backup(output);
  }

  @Override
  public void install(InputStream input) {
    stateMachine.restore(input);
  }

  @Override
  public boolean canDelete(long index) {
    return stateMachine.canDelete(index);
  }

  @Override
  public CompletableFuture<byte[]> apply(RaftCommand command) {
    return stateMachine.apply(new Command<>(command.index(), command.timestamp(), command.value()));
  }

  @Override
  public CompletableFuture<byte[]> apply(RaftQuery query) {
    return stateMachine.apply(new Query<>(query.index(), query.timestamp(), query.value()));
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
    public Role getRole() {
      switch (context.getRole()) {
        case LEADER:
          return Role.PRIMARY;
        default:
          return Role.SECONDARY;
      }
    }
  }
}
