package io.atomix.client.value.impl;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.google.protobuf.ByteString;
import io.atomix.api.protocol.DistributedLogProtocol;
import io.atomix.api.protocol.MultiPrimaryProtocol;
import io.atomix.api.protocol.MultiRaftProtocol;
import io.atomix.api.value.CheckAndSetRequest;
import io.atomix.api.value.CheckAndSetResponse;
import io.atomix.api.value.CloseRequest;
import io.atomix.api.value.CloseResponse;
import io.atomix.api.value.CreateRequest;
import io.atomix.api.value.CreateResponse;
import io.atomix.api.value.EventRequest;
import io.atomix.api.value.EventResponse;
import io.atomix.api.value.GetRequest;
import io.atomix.api.value.GetResponse;
import io.atomix.api.value.KeepAliveRequest;
import io.atomix.api.value.KeepAliveResponse;
import io.atomix.api.value.SetRequest;
import io.atomix.api.value.SetResponse;
import io.atomix.api.value.ValueId;
import io.atomix.api.value.ValueServiceGrpc;
import io.atomix.client.PrimitiveManagementService;
import io.atomix.client.channel.ChannelFactory;
import io.atomix.client.impl.AbstractAsyncPrimitive;
import io.atomix.client.impl.PrimitiveIdDescriptor;
import io.atomix.client.impl.PrimitivePartition;
import io.atomix.client.value.AsyncAtomicValue;
import io.atomix.client.value.AtomicValue;
import io.atomix.client.value.AtomicValueEvent;
import io.atomix.client.value.AtomicValueEventListener;
import io.atomix.primitive.partition.Partitioner;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.time.Versioned;
import io.grpc.stub.StreamObserver;

/**
 * Default asynchronous atomic value primitive.
 */
public class DefaultAsyncAtomicValue
    extends AbstractAsyncPrimitive<ValueId, AsyncAtomicValue<String>>
    implements AsyncAtomicValue<String> {
  private final ValueServiceGrpc.ValueServiceStub value;

  public DefaultAsyncAtomicValue(
      ValueId id,
      ChannelFactory channelFactory,
      PrimitiveManagementService managementService,
      Partitioner<String> partitioner,
      Duration timeout) {
    super(id, VALUE_ID_DESCRIPTOR, managementService, partitioner, timeout);
    this.value = ValueServiceGrpc.newStub(channelFactory.getChannel());
  }

  @Override
  public CompletableFuture<Versioned<String>> get() {
    PrimitivePartition partition = getPartition();
    return this.<GetResponse>execute(observer -> value.get(GetRequest.newBuilder()
        .setId(id())
        .setHeader(partition.getQueryHeader())
        .build(), observer))
        .thenCompose(response -> partition.order(
            response.getVersion() > 0
                ? new Versioned<>(response.getValue().toStringUtf8(), response.getVersion())
                : null,
            response.getHeader()));
  }

  @Override
  public CompletableFuture<Versioned<String>> getAndSet(String value) {
    PrimitivePartition partition = getPartition();
    return this.<SetResponse>execute(observer -> this.value.set(SetRequest.newBuilder()
        .setId(id())
        .setHeader(partition.getCommandHeader())
        .setValue(ByteString.copyFromUtf8(value))
        .build(), observer))
        .thenCompose(response -> partition.order(
            response.getPreviousVersion() > 0
                ? new Versioned<>(response.getPreviousValue().toStringUtf8(), response.getPreviousVersion())
                : null,
            response.getHeader()));
  }

  @Override
  public CompletableFuture<Versioned<String>> set(String value) {
    PrimitivePartition partition = getPartition();
    return this.<SetResponse>execute(observer -> this.value.set(SetRequest.newBuilder()
        .setId(id())
        .setHeader(partition.getCommandHeader())
        .setValue(ByteString.copyFromUtf8(value))
        .build(), observer))
        .thenCompose(response -> partition.order(
            response.getVersion() > 0
                ? new Versioned<>(value, response.getVersion())
                : null,
            response.getHeader()));
  }

  @Override
  public CompletableFuture<Optional<Versioned<String>>> compareAndSet(String expect, String update) {
    PrimitivePartition partition = getPartition();
    return this.<CheckAndSetResponse>execute(observer -> this.value.checkAndSet(CheckAndSetRequest.newBuilder()
        .setId(id())
        .setHeader(partition.getCommandHeader())
        .setCheck(ByteString.copyFromUtf8(expect))
        .setUpdate(ByteString.copyFromUtf8(update))
        .build(), observer))
        .thenCompose(response -> partition.order(
            Optional.ofNullable(response.getSucceeded() ? new Versioned<>(update, response.getVersion()) : null),
            response.getHeader()));
  }

  @Override
  public CompletableFuture<Optional<Versioned<String>>> compareAndSet(long version, String value) {
    PrimitivePartition partition = getPartition();
    return this.<CheckAndSetResponse>execute(observer -> this.value.checkAndSet(CheckAndSetRequest.newBuilder()
        .setId(id())
        .setHeader(partition.getCommandHeader())
        .setVersion(version)
        .setUpdate(ByteString.copyFromUtf8(value))
        .build(), observer))
        .thenCompose(response -> partition.order(
            Optional.ofNullable(response.getSucceeded() ? new Versioned<>(value, response.getVersion()) : null),
            response.getHeader()));
  }

  @Override
  public CompletableFuture<Void> addListener(AtomicValueEventListener<String> listener) {
    PrimitivePartition partition = getPartition();
    value.event(EventRequest.newBuilder()
        .setId(id())
        .setHeader(partition.getCommandHeader())
        .build(), new StreamObserver<EventResponse>() {
      @Override
      public void onNext(EventResponse response) {
        PrimitivePartition partition = getPartition(response.getHeader().getPartitionId());
        AtomicValueEvent<String> event = null;
        switch (response.getType()) {
          case UPDATED:
            event = new AtomicValueEvent<>(
                AtomicValueEvent.Type.UPDATE,
                response.getNewVersion() > 0 ? new Versioned<>(response.getNewValue().toStringUtf8(), response.getNewVersion()) : null,
                response.getPreviousVersion() > 0 ? new Versioned<>(response.getPreviousValue().toStringUtf8(), response.getPreviousVersion()) : null);
            break;
        }
        partition.order(event, response.getHeader()).thenAccept(listener::event);
      }

      @Override
      public void onError(Throwable t) {

      }

      @Override
      public void onCompleted() {

      }
    });
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> removeListener(AtomicValueEventListener<String> listener) {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<AsyncAtomicValue<String>> connect() {
    return this.<CreateResponse>execute(stream -> value.create(CreateRequest.newBuilder()
        .setId(id())
        .setTimeout(com.google.protobuf.Duration.newBuilder()
            .setSeconds(timeout.getSeconds())
            .setNanos(timeout.getNano())
            .build())
        .build(), stream))
        .thenAccept(response -> {
          startKeepAlive(response.getHeader());
        })
        .thenApply(v -> this);
  }

  @Override
  protected CompletableFuture<Void> keepAlive() {
    PrimitivePartition partition = getPartition();
    return this.<KeepAliveResponse>execute(stream -> value.keepAlive(KeepAliveRequest.newBuilder()
        .setId(id())
        .setHeader(partition.getSessionHeader())
        .build(), stream))
        .thenAccept(response -> completeKeepAlive(response.getHeader()));
  }

  @Override
  public CompletableFuture<Void> close() {
    PrimitivePartition partition = getPartition();
    return this.<CloseResponse>execute(stream -> value.close(CloseRequest.newBuilder()
        .setId(id())
        .setHeader(partition.getSessionHeader())
        .build(), stream))
        .thenApply(response -> null);
  }

  @Override
  public CompletableFuture<Void> delete() {
    return Futures.exceptionalFuture(new UnsupportedOperationException());
  }

  @Override
  public AtomicValue<String> sync(Duration operationTimeout) {
    return new BlockingAtomicValue<>(this, operationTimeout.toMillis());
  }

  private static final PrimitiveIdDescriptor<ValueId> VALUE_ID_DESCRIPTOR = new PrimitiveIdDescriptor<ValueId>() {
    @Override
    public String getName(ValueId id) {
      return id.getName();
    }

    @Override
    public boolean hasMultiRaftProtocol(ValueId id) {
      return id.hasRaft();
    }

    @Override
    public MultiRaftProtocol getMultiRaftProtocol(ValueId id) {
      return id.getRaft();
    }

    @Override
    public boolean hasMultiPrimaryProtocol(ValueId id) {
      return id.hasMultiPrimary();
    }

    @Override
    public MultiPrimaryProtocol getMultiPrimaryProtocol(ValueId id) {
      return id.getMultiPrimary();
    }

    @Override
    public boolean hasDistributedLogProtocol(ValueId id) {
      return id.hasLog();
    }

    @Override
    public DistributedLogProtocol getDistributedLogProtocol(ValueId id) {
      return id.getLog();
    }
  };
}
