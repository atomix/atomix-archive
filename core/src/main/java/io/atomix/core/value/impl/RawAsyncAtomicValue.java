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
import io.atomix.primitive.impl.ManagedAsyncPrimitive;
import io.atomix.utils.time.Versioned;

/**
 * Raw asynchronous atomic value.
 */
public class RawAsyncAtomicValue extends ManagedAsyncPrimitive<ValueProxy> implements AsyncAtomicValue<byte[]> {
  private final Set<AtomicValueEventListener<byte[]>> eventListeners = new CopyOnWriteArraySet<>();

  public RawAsyncAtomicValue(ValueProxy proxy, Duration timeout, PrimitiveManagementService managementService) {
    super(proxy, timeout, managementService);
    event((p, s) -> p.onEvent(s, listener(this::onEvent)));
  }

  @Override
  public CompletableFuture<Optional<Versioned<byte[]>>> compareAndSet(byte[] expect, byte[] update) {
    return command((proxy, session) -> proxy.checkAndSet(session, CheckAndSetRequest.newBuilder()
        .setCheck(ByteString.copyFrom(expect))
        .setUpdate(ByteString.copyFrom(update))
        .build()))
        .thenApply(response -> {
          if (response.getSucceeded()) {
            return Optional.of(new Versioned<>(update, response.getVersion()));
          }
          return Optional.empty();
        });
  }

  @Override
  public CompletableFuture<Optional<Versioned<byte[]>>> compareAndSet(long version, byte[] value) {
    return command((proxy, session) -> proxy.checkAndSet(session, CheckAndSetRequest.newBuilder()
        .setVersion(version)
        .setUpdate(ByteString.copyFrom(value))
        .build()))
        .thenApply(response -> {
          if (response.getSucceeded()) {
            return Optional.of(new Versioned<>(value, response.getVersion()));
          }
          return Optional.empty();
        });
  }

  @Override
  public CompletableFuture<Versioned<byte[]>> get() {
    return query((proxy, session) -> proxy.get(session, GetRequest.newBuilder().build()))
        .thenApply(response -> {
          if (!response.getValue().isEmpty()) {
            return new Versioned<>(response.getValue().toByteArray(), response.getVersion());
          }
          return null;
        });
  }

  @Override
  public CompletableFuture<Versioned<byte[]>> getAndSet(byte[] value) {
    return command((proxy, session) -> proxy.set(session, SetRequest.newBuilder()
        .setValue(ByteString.copyFrom(value))
        .build()))
        .thenApply(response -> {
          if (!response.getPreviousValue().isEmpty()) {
            return new Versioned<>(response.getPreviousValue().toByteArray(), response.getPreviousVersion());
          }
          return null;
        });
  }

  @Override
  public CompletableFuture<Versioned<byte[]>> set(byte[] value) {
    return command((proxy, session) -> proxy.set(session, SetRequest.newBuilder()
        .setValue(ByteString.copyFrom(value))
        .build()))
        .thenApply(response -> new Versioned<>(value, response.getVersion()));
  }

  @Override
  public synchronized CompletableFuture<Void> addListener(AtomicValueEventListener<byte[]> listener) {
    boolean add = eventListeners.isEmpty();
    eventListeners.add(listener);
    if (add) {
      return command((proxy, session) -> proxy.listen(session, ListenRequest.newBuilder().build()))
          .thenApply(response -> null);
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public synchronized CompletableFuture<Void> removeListener(AtomicValueEventListener<byte[]> listener) {
    eventListeners.remove(listener);
    if (eventListeners.isEmpty()) {
      return command((proxy, session) -> proxy.unlisten(session, UnlistenRequest.newBuilder().build()))
          .thenApply(response -> null);
    }
    return CompletableFuture.completedFuture(null);
  }

  private void onEvent(ValueEvent event) {
    onEvent(new AtomicValueEvent<>(
        AtomicValueEvent.Type.UPDATE,
        event.getPreviousVersion() != 0 ? new Versioned<>(event.getPreviousValue().toByteArray(), event.getPreviousVersion()) : null,
        event.getNewVersion() != 0 ? new Versioned<>(event.getNewValue().toByteArray(), event.getNewVersion()) : null));
  }

  private void onEvent(AtomicValueEvent<byte[]> event) {
    eventListeners.forEach(listener -> listener.event(event));
  }

  @Override
  public AtomicValue<byte[]> sync(Duration operationTimeout) {
    return new BlockingAtomicValue<>(this, operationTimeout.toMillis());
  }
}
