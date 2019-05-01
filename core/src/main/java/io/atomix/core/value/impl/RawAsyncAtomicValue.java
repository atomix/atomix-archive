package io.atomix.core.value.impl;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;

import com.google.protobuf.ByteString;
import io.atomix.core.value.AsyncAtomicValue;
import io.atomix.core.value.AtomicValue;
import io.atomix.core.value.AtomicValueEvent;
import io.atomix.core.value.AtomicValueEventListener;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.impl.SessionEnabledAsyncPrimitive;
import io.atomix.utils.stream.StreamHandler;
import io.atomix.utils.time.Versioned;

/**
 * Raw asynchronous atomic value.
 */
public class RawAsyncAtomicValue extends SessionEnabledAsyncPrimitive<ValueProxy, AsyncAtomicValue<byte[]>> implements AsyncAtomicValue<byte[]> {
  private static final Versioned<byte[]> EMPTY = new Versioned<>(null, 0);
  private final Set<AtomicValueEventListener<byte[]>> eventListeners = new CopyOnWriteArraySet<>();
  private volatile long streamId;

  public RawAsyncAtomicValue(ValueProxy proxy, Duration timeout, PrimitiveManagementService managementService) {
    super(proxy, timeout, managementService);
  }

  @Override
  public CompletableFuture<Optional<Versioned<byte[]>>> compareAndSet(byte[] expect, byte[] update) {
    return execute(ValueProxy::checkAndSet, CheckAndSetRequest.newBuilder()
        .setCheck(ByteString.copyFrom(expect))
        .setUpdate(ByteString.copyFrom(update))
        .build())
        .thenApply(response -> {
          if (response.getSucceeded()) {
            return Optional.of(new Versioned<>(update, response.getVersion()));
          }
          return Optional.empty();
        });
  }

  @Override
  public CompletableFuture<Optional<Versioned<byte[]>>> compareAndSet(long version, byte[] value) {
    return execute(ValueProxy::checkAndSet, CheckAndSetRequest.newBuilder()
        .setVersion(version)
        .setUpdate(ByteString.copyFrom(value))
        .build())
        .thenApply(response -> {
          if (response.getSucceeded()) {
            return Optional.of(new Versioned<>(value, response.getVersion()));
          }
          return Optional.empty();
        });
  }

  @Override
  public CompletableFuture<Versioned<byte[]>> get() {
    return execute(ValueProxy::get, GetRequest.newBuilder().build())
        .thenApply(response -> {
          if (!response.getValue().isEmpty()) {
            return new Versioned<>(response.getValue().toByteArray(), response.getVersion());
          }
          return EMPTY;
        });
  }

  @Override
  public CompletableFuture<Versioned<byte[]>> getAndSet(byte[] value) {
    return execute(ValueProxy::set, SetRequest.newBuilder()
        .setValue(ByteString.copyFrom(value))
        .build())
        .thenApply(response -> {
          if (!response.getPreviousValue().isEmpty()) {
            return new Versioned<>(response.getPreviousValue().toByteArray(), response.getPreviousVersion());
          }
          return EMPTY;
        });
  }

  @Override
  public CompletableFuture<Versioned<byte[]>> set(byte[] value) {
    return execute(ValueProxy::set, SetRequest.newBuilder()
        .setValue(ByteString.copyFrom(value))
        .build())
        .thenApply(response -> new Versioned<>(value, response.getVersion()));
  }

  @Override
  public synchronized CompletableFuture<Void> addListener(AtomicValueEventListener<byte[]> listener) {
    boolean add = eventListeners.isEmpty();
    eventListeners.add(listener);
    if (add) {
      return execute(ValueProxy::listen, ListenRequest.newBuilder().build(), new StreamHandler<ListenResponse>() {
        @Override
        public void next(ListenResponse response) {
          eventListeners.forEach(l -> l.event(new AtomicValueEvent<>(
              AtomicValueEvent.Type.UPDATE,
              response.getNewVersion() != 0 ? new Versioned<>(response.getNewValue().toByteArray(), response.getNewVersion()) : null,
              response.getPreviousVersion() != 0 ? new Versioned<>(response.getPreviousValue().toByteArray(), response.getPreviousVersion()) : null)));
        }

        @Override
        public void complete() {

        }

        @Override
        public void error(Throwable error) {

        }
      }).thenApply(streamId -> {
        this.streamId = streamId;
        return null;
      });
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public synchronized CompletableFuture<Void> removeListener(AtomicValueEventListener<byte[]> listener) {
    eventListeners.remove(listener);
    if (eventListeners.isEmpty()) {
      return execute(ValueProxy::unlisten, UnlistenRequest.newBuilder().setStreamId(streamId).build())
          .thenApply(response -> null);
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public AtomicValue<byte[]> sync(Duration operationTimeout) {
    return new BlockingAtomicValue<>(this, operationTimeout.toMillis());
  }
}
