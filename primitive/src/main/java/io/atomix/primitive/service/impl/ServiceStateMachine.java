package io.atomix.primitive.service.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

import com.google.protobuf.ByteString;
import io.atomix.primitive.operation.OperationType;
import io.atomix.primitive.service.Command;
import io.atomix.primitive.service.PrimitiveService;
import io.atomix.primitive.service.Query;
import io.atomix.primitive.service.Role;
import io.atomix.primitive.service.StateMachine;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import org.slf4j.Logger;

/**
 * Service state machine.
 */
public class ServiceStateMachine implements StateMachine {
  private final String name;
  private final String type;
  private final StateMachine stateMachine;
  private final Logger log;
  private final TreeMap<Long, Long> snapshotIndexes = new TreeMap<>();
  private Context context;
  private long index;

  public ServiceStateMachine(String name, String type, StateMachine stateMachine) {
    this.name = name;
    this.type = type;
    this.stateMachine = stateMachine;
    this.log = ContextualLoggerFactory.getLogger(getClass(), LoggerContext.builder(PrimitiveService.class)
        .add("name", name)
        .add("type", type)
        .build());
  }

  /**
   * Returns the state machine context.
   *
   * @return the state machine context
   */
  public Context context() {
    return context;
  }

  @Override
  public void init(StateMachine.Context context) {
    this.context = new Context(context);
    stateMachine.init(this.context);
  }

  @Override
  public void snapshot(OutputStream output) throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    stateMachine.snapshot(outputStream);

    ServiceSnapshot.newBuilder()
        .setIndex(index)
        .setSnapshot(ByteString.copyFrom(outputStream.toByteArray()))
        .build()
        .writeDelimitedTo(output);
    snapshotIndexes.put(context.parent.getIndex(), context.getIndex());
  }

  @Override
  public void install(InputStream input) throws IOException {
    ServiceSnapshot snapshot = ServiceSnapshot.parseDelimitedFrom(input);
    index = snapshot.getIndex();
    stateMachine.install(new ByteArrayInputStream(snapshot.getSnapshot().toByteArray()));
    snapshotIndexes.clear();
  }

  @Override
  public boolean canDelete(long index) {
    Long snapshotIndex = snapshotIndexes.ceilingKey(index);
    return snapshotIndex == null || stateMachine.canDelete(snapshotIndex);
  }

  @Override
  public CompletableFuture<byte[]> apply(Command<byte[]> command) {
    return stateMachine.apply(new Command<>(++index, command.timestamp(), command.value()));
  }

  @Override
  public CompletableFuture<byte[]> apply(Query<byte[]> query) {
    return stateMachine.apply(new Query<>(++index, query.timestamp(), query.value()));
  }

  /**
   * Service state machine context.
   */
  public class Context implements StateMachine.Context {
    private final StateMachine.Context parent;

    private Context(StateMachine.Context parent) {
      this.parent = parent;
    }

    /**
     * Returns the service name.
     *
     * @return the service name
     */
    public String getName() {
      return name;
    }

    /**
     * Returns the service type.
     *
     * @return the service type
     */
    public String getType() {
      return type;
    }

    @Override
    public long getIndex() {
      return index;
    }

    @Override
    public long getTimestamp() {
      return parent.getTimestamp();
    }

    @Override
    public OperationType getOperationType() {
      return parent.getOperationType();
    }

    @Override
    public Role getRole() {
      return parent.getRole();
    }

    @Override
    public Logger getLogger() {
      return log;
    }
  }
}
