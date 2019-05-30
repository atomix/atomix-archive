package io.atomix.client.value.impl;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;

import com.google.protobuf.ByteString;
import io.atomix.api.headers.Name;
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
import io.atomix.api.value.ValueServiceGrpc;
import io.atomix.client.Versioned;
import io.atomix.client.impl.AbstractManagedPrimitive;
import io.atomix.client.partition.Partition;
import io.atomix.client.utils.concurrent.ThreadContext;
import io.atomix.client.value.AsyncAtomicValue;
import io.atomix.client.value.AtomicValue;
import io.atomix.client.value.AtomicValueEvent;
import io.atomix.client.value.AtomicValueEventListener;
import io.grpc.stub.StreamObserver;

/**
 * Default asynchronous atomic value primitive.
 */
public class DefaultAsyncAtomicValue
    extends AbstractManagedPrimitive<ValueServiceGrpc.ValueServiceStub, AsyncAtomicValue<String>>
    implements AsyncAtomicValue<String> {
  private volatile CompletableFuture<Long> listenFuture;
  private final Set<AtomicValueEventListener<String>> eventListeners = new CopyOnWriteArraySet<>();

  public DefaultAsyncAtomicValue(Name name, Partition partition, ThreadContext context, Duration timeout) {
    super(name, ValueServiceGrpc.newStub(partition.getChannelFactory().getChannel()), context, timeout);
  }

  @Override
  public CompletableFuture<Versioned<String>> get() {
    return query(
        (header, observer) -> getService().get(GetRequest.newBuilder()
            .build(), observer),
        GetResponse::getHeader)
        .thenApply(response -> response.getVersion() > 0
            ? new Versioned<>(response.getValue().toStringUtf8(), response.getVersion())
            : null);
  }

  @Override
  public CompletableFuture<Versioned<String>> getAndSet(String value) {
    return command(
        (header, observer) -> getService().set(SetRequest.newBuilder()
            .setHeader(header)
            .setValue(ByteString.copyFromUtf8(value))
            .build(), observer), SetResponse::getHeader)
        .thenApply(response -> response.getPreviousVersion() > 0
            ? new Versioned<>(response.getPreviousValue().toStringUtf8(), response.getPreviousVersion())
            : null);
  }

  @Override
  public CompletableFuture<Versioned<String>> set(String value) {
    return command(
        (header, observer) -> getService().set(SetRequest.newBuilder()
            .setHeader(header)
            .setValue(ByteString.copyFromUtf8(value))
            .build(), observer), SetResponse::getHeader)
        .thenApply(response -> response.getVersion() > 0
            ? new Versioned<>(value, response.getVersion())
            : null);
  }

  @Override
  public CompletableFuture<Optional<Versioned<String>>> compareAndSet(String expect, String update) {
    return command(
        (header, observer) -> getService().checkAndSet(CheckAndSetRequest.newBuilder()
            .setHeader(header)
            .setCheck(ByteString.copyFromUtf8(expect))
            .setUpdate(ByteString.copyFromUtf8(update))
            .build(), observer), CheckAndSetResponse::getHeader)
        .thenApply(response -> Optional.ofNullable(
            response.getSucceeded() ? new Versioned<>(update, response.getVersion()) : null));
  }

  @Override
  public CompletableFuture<Optional<Versioned<String>>> compareAndSet(long version, String value) {
    return command(
        (header, observer) -> getService().checkAndSet(CheckAndSetRequest.newBuilder()
            .setHeader(header)
            .setVersion(version)
            .setUpdate(ByteString.copyFromUtf8(value))
            .build(), observer), CheckAndSetResponse::getHeader)
        .thenApply(response -> Optional.ofNullable(
            response.getSucceeded() ? new Versioned<>(value, response.getVersion()) : null));
  }

  private synchronized CompletableFuture<Void> listen() {
    if (listenFuture == null && !eventListeners.isEmpty()) {
      listenFuture = command(
          (header, observer) -> getService().event(EventRequest.newBuilder()
              .setHeader(header)
              .build(), observer),
          EventResponse::getHeader,
          new StreamObserver<EventResponse>() {
            @Override
            public void onNext(EventResponse response) {
              AtomicValueEvent<String> event = null;
              switch (response.getType()) {
                case UPDATED:
                  event = new AtomicValueEvent<>(
                      AtomicValueEvent.Type.UPDATE,
                      response.getNewVersion() > 0 ? new Versioned<>(response.getNewValue().toStringUtf8(), response.getNewVersion()) : null,
                      response.getPreviousVersion() > 0 ? new Versioned<>(response.getPreviousValue().toStringUtf8(), response.getPreviousVersion()) : null);
                  break;
              }
              onEvent(event);
            }

            private void onEvent(AtomicValueEvent<String> event) {
              eventListeners.forEach(l -> l.event(event));
            }

            @Override
            public void onError(Throwable t) {
              onCompleted();
            }

            @Override
            public void onCompleted() {
              synchronized (DefaultAsyncAtomicValue.this) {
                listenFuture = null;
              }
              listen();
            }
          });
    }
    return listenFuture.thenApply(v -> null);
  }

  @Override
  public synchronized CompletableFuture<Void> addListener(AtomicValueEventListener<String> listener) {
    eventListeners.add(listener);
    return listen();
  }

  @Override
  public synchronized CompletableFuture<Void> removeListener(AtomicValueEventListener<String> listener) {
    eventListeners.remove(listener);
    return CompletableFuture.completedFuture(null);
  }

  @Override
  protected CompletableFuture<Long> openSession(Duration timeout) {
    return this.<CreateResponse>session((header, observer) -> getService().create(CreateRequest.newBuilder()
        .setTimeout(com.google.protobuf.Duration.newBuilder()
            .setSeconds(timeout.getSeconds())
            .setNanos(timeout.getNano())
            .build())
        .build(), observer))
        .thenApply(response -> response.getHeader().getSessionId());
  }

  @Override
  protected CompletableFuture<Boolean> keepAlive() {
    return this.<KeepAliveResponse>session((header, observer) -> getService().keepAlive(KeepAliveRequest.newBuilder()
        .build(), observer))
        .thenApply(response -> true);
  }

  @Override
  protected CompletableFuture<Void> close(boolean delete) {
    return this.<CloseResponse>session((header, observer) -> getService().close(CloseRequest.newBuilder()
        .setDelete(delete)
        .build(), observer))
        .thenApply(v -> null);
  }

  @Override
  public AtomicValue<String> sync(Duration operationTimeout) {
    return new BlockingAtomicValue<>(this, operationTimeout.toMillis());
  }
}
