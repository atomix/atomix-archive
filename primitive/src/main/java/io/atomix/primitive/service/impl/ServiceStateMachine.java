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
import io.atomix.primitive.service.StateMachine;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import io.atomix.utils.stream.StreamHandler;
import org.slf4j.Logger;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Service state machine.
 */
public class ServiceStateMachine implements StateMachine {
  private final ServiceId serviceId;
  private final StateMachine stateMachine;
  private final Logger log;
  private final TreeMap<Long, Long> snapshotIndexes = new TreeMap<>();
  private Context context;
  private long index;

  public ServiceStateMachine(ServiceId serviceId, StateMachine stateMachine) {
    this.serviceId = serviceId;
    this.stateMachine = stateMachine;
    this.log = ContextualLoggerFactory.getLogger(getClass(), LoggerContext.builder(PrimitiveService.class)
        .add("name", serviceId.getName())
        .add("type", serviceId.getType())
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
    try {
      return stateMachine.apply(new Command<>(++index, command.timestamp(), command.value()));
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public CompletableFuture<Void> apply(Command<byte[]> command, StreamHandler<byte[]> handler) {
    try {
      return stateMachine.apply(new Command<>(++index, command.timestamp(), command.value()), handler);
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public CompletableFuture<byte[]> apply(Query<byte[]> query) {
    try {
      return stateMachine.apply(new Query<>(index, query.timestamp(), query.value()));
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public CompletableFuture<Void> apply(Query<byte[]> query, StreamHandler<byte[]> handler) {
    try {
      return stateMachine.apply(new Query<>(index, query.timestamp(), query.value()), handler);
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  /**
   * Service state machine context.
   */
  public class Context implements StateMachine.Context {
    private final StateMachine.Context parent;

    private Context(StateMachine.Context parent) {
      this.parent = checkNotNull(parent);
    }

    /**
     * Returns the service name.
     *
     * @return the service name
     */
    public String getName() {
      return serviceId.getName();
    }

    /**
     * Returns the service type.
     *
     * @return the service type
     */
    public String getType() {
      return serviceId.getType();
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
    public Logger getLogger() {
      return log;
    }
  }
}
