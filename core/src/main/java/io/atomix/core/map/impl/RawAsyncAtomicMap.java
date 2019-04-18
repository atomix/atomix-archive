package io.atomix.core.map.impl;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;
import io.atomix.core.collection.AsyncDistributedCollection;
import io.atomix.core.map.AsyncAtomicMap;
import io.atomix.core.map.AtomicMap;
import io.atomix.core.map.AtomicMapEvent;
import io.atomix.core.map.AtomicMapEventListener;
import io.atomix.core.set.AsyncDistributedSet;
import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionLog;
import io.atomix.primitive.AbstractAsyncPrimitive;
import io.atomix.primitive.PrimitiveException;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.time.Versioned;

/**
 * Raw asynchronous atomic map.
 */
public class RawAsyncAtomicMap extends AbstractAsyncPrimitive<MapProxy> implements AsyncAtomicMap<String, byte[]> {
  private final Map<AtomicMapEventListener<String, byte[]>, Executor> eventListeners = new ConcurrentHashMap<>();

  public RawAsyncAtomicMap(MapProxy proxy) {
    super(proxy);
    proxy.onEvent(this::onEvent);
  }

  @Override
  public CompletableFuture<Integer> size() {
    return getProxy().size(SizeRequest.newBuilder().build())
        .thenApply(response -> response.getSize());
  }

  @Override
  public CompletableFuture<Boolean> containsKey(String key) {
    return getProxy().containsKey(ContainsKeyRequest.newBuilder().addKeys(key).build())
        .thenApply(response -> response.getContainsKey());
  }

  @Override
  public CompletableFuture<Boolean> containsValue(byte[] value) {
    return Futures.exceptionalFuture(new UnsupportedOperationException());
  }

  @Override
  public CompletableFuture<Versioned<byte[]>> get(String key) {
    return getProxy().get(GetRequest.newBuilder().setKey(key).build())
        .thenApply(response -> {
          if (!response.getValue().isEmpty()) {
            return new Versioned<>(response.getValue().toByteArray(), response.getVersion());
          }
          return null;
        });
  }

  @Override
  public CompletableFuture<Versioned<byte[]>> getOrDefault(String key, byte[] defaultValue) {
    return get(key).thenApply(result -> result != null ? result : new Versioned<>(defaultValue, 0));
  }

  @Override
  public CompletableFuture<Versioned<byte[]>> computeIf(
      String key,
      Predicate<? super byte[]> condition,
      BiFunction<? super String, ? super byte[], ? extends byte[]> remappingFunction) {
    // TODO: Implement compute methods
    return Futures.exceptionalFuture(new UnsupportedOperationException());
  }

  @Override
  public CompletableFuture<Versioned<byte[]>> put(String key, byte[] value, Duration ttl) {
    return getProxy().put(PutRequest.newBuilder()
        .setKey(key)
        .setValue(ByteString.copyFrom(value))
        .setTtl(ttl.toMillis())
        .build())
        .thenApply(response -> {
          if (response.getStatus() == UpdateStatus.WRITE_LOCK) {
            throw new PrimitiveException.ConcurrentModification();
          }
          if (!response.getPreviousValue().isEmpty()) {
            return new Versioned<>(response.getPreviousValue().toByteArray(), response.getPreviousVersion());
          }
          return null;
        });
  }

  @Override
  public CompletableFuture<Versioned<byte[]>> remove(String key) {
    return getProxy().remove(RemoveRequest.newBuilder().setKey(key).build())
        .thenApply(response -> {
          if (response.getStatus() == UpdateStatus.WRITE_LOCK) {
            throw new PrimitiveException.ConcurrentModification();
          }
          if (!response.getPreviousValue().isEmpty()) {
            return new Versioned<>(response.getPreviousValue().toByteArray(), response.getPreviousVersion());
          }
          return null;
        });
  }

  @Override
  public CompletableFuture<Void> clear() {
    return getProxy().clear(ClearRequest.newBuilder().build())
        .thenApply(response -> null);
  }

  @Override
  public AsyncDistributedSet<String> keySet() {
    // TODO: Implement key set
    throw new UnsupportedOperationException();
  }

  @Override
  public AsyncDistributedCollection<Versioned<byte[]>> values() {
    // TODO: Implement values
    throw new UnsupportedOperationException();
  }

  @Override
  public AsyncDistributedSet<Map.Entry<String, Versioned<byte[]>>> entrySet() {
    // TODO: Implement entry set
    throw new UnsupportedOperationException();
  }

  @Override
  public CompletableFuture<Versioned<byte[]>> putIfAbsent(String key, byte[] value, Duration ttl) {
    return Futures.exceptionalFuture(new UnsupportedOperationException());
  }

  @Override
  public CompletableFuture<Boolean> remove(String key, byte[] value) {
    return getProxy().remove(RemoveRequest.newBuilder().setKey(key).setValue(ByteString.copyFrom(value)).build())
        .thenApply(response -> {
          if (response.getStatus() == UpdateStatus.WRITE_LOCK) {
            throw new PrimitiveException.ConcurrentModification();
          }
          return response.getStatus() == UpdateStatus.OK;
        });
  }

  @Override
  public CompletableFuture<Boolean> remove(String key, long version) {
    return getProxy().remove(RemoveRequest.newBuilder().setKey(key).setVersion(version).build())
        .thenApply(response -> {
          if (response.getStatus() == UpdateStatus.WRITE_LOCK) {
            throw new PrimitiveException.ConcurrentModification();
          }
          return response.getStatus() == UpdateStatus.OK;
        });
  }

  @Override
  public CompletableFuture<Versioned<byte[]>> replace(String key, byte[] value) {
    return getProxy().replace(ReplaceRequest.newBuilder()
        .setKey(key)
        .setNewValue(ByteString.copyFrom(value))
        .build())
        .thenApply(response -> {
          if (response.getStatus() == UpdateStatus.WRITE_LOCK) {
            throw new PrimitiveException.ConcurrentModification();
          } else if (response.getStatus() == UpdateStatus.OK) {
            return new Versioned<>(response.getPreviousValue().toByteArray(), response.getPreviousVersion());
          }
          return null;
        });
  }

  @Override
  public CompletableFuture<Boolean> replace(String key, byte[] oldValue, byte[] newValue) {
    return getProxy().replace(ReplaceRequest.newBuilder()
        .setKey(key)
        .setPreviousValue(ByteString.copyFrom(oldValue))
        .setNewValue(ByteString.copyFrom(newValue))
        .build())
        .thenApply(response -> {
          if (response.getStatus() == UpdateStatus.WRITE_LOCK) {
            throw new PrimitiveException.ConcurrentModification();
          }
          return response.getStatus() == UpdateStatus.OK;
        });
  }

  @Override
  public CompletableFuture<Boolean> replace(String key, long oldVersion, byte[] newValue) {
    return getProxy().replace(ReplaceRequest.newBuilder()
        .setKey(key)
        .setPreviousVersion(oldVersion)
        .setNewValue(ByteString.copyFrom(newValue))
        .build())
        .thenApply(response -> {
          if (response.getStatus() == UpdateStatus.WRITE_LOCK) {
            throw new PrimitiveException.ConcurrentModification();
          }
          return response.getStatus() == UpdateStatus.OK;
        });
  }

  @Override
  public synchronized CompletableFuture<Void> addListener(AtomicMapEventListener<String, byte[]> listener, Executor executor) {
    if (eventListeners.isEmpty()) {
      eventListeners.put(listener, executor);
      return getProxy().listen(ListenRequest.newBuilder().build())
          .thenApply(response -> null);
    } else {
      eventListeners.put(listener, executor);
      return CompletableFuture.completedFuture(null);
    }
  }

  @Override
  public synchronized CompletableFuture<Void> removeListener(AtomicMapEventListener<String, byte[]> listener) {
    eventListeners.remove(listener);
    if (eventListeners.isEmpty()) {
      return getProxy().unlisten(UnlistenRequest.newBuilder().build())
          .thenApply(response -> null);
    } else {
      return CompletableFuture.completedFuture(null);
    }
  }

  @Override
  public CompletableFuture<Boolean> prepare(TransactionLog<MapUpdate<String, byte[]>> transactionLog) {
    return getProxy().prepare(PrepareRequest.newBuilder()
        .setTransactionId(transactionLog.transactionId().id())
        .setTransaction(AtomicMapTransaction.newBuilder()
            .setVersion(transactionLog.version())
            .addAllUpdates(transactionLog.records().stream()
                .map(update -> AtomicMapUpdate.newBuilder()
                    .setType(AtomicMapUpdate.Type.valueOf(update.type().name()))
                    .setKey(update.key())
                    .setValue(ByteString.copyFrom(update.value()))
                    .setVersion(update.version())
                    .build())
                .collect(Collectors.toList()))
            .build())
        .build())
        .thenApply(response -> response.getStatus() == PrepareResponse.Status.OK);
  }

  @Override
  public CompletableFuture<Void> commit(TransactionId transactionId) {
    return getProxy().commit(CommitRequest.newBuilder()
        .setTransactionId(transactionId.id())
        .build())
        .thenApply(response -> null);
  }

  @Override
  public CompletableFuture<Void> rollback(TransactionId transactionId) {
    return getProxy().rollback(RollbackRequest.newBuilder()
        .setTransactionId(transactionId.id())
        .build())
        .thenApply(response -> null);
  }

  private void onEvent(MapEvent event) {
    onEvent(new AtomicMapEvent<>(
        AtomicMapEvent.Type.valueOf(event.getType().name()),
        event.getKey(),
        !event.getNewValue().isEmpty() ? new Versioned<>(event.getNewValue().toByteArray(), event.getNewVersion()) : null,
        !event.getOldValue().isEmpty() ? new Versioned<>(event.getOldValue().toByteArray(), event.getOldVersion()) : null));
  }

  private void onEvent(AtomicMapEvent<String, byte[]> event) {
    eventListeners.forEach((listener, executor) -> executor.execute(() -> listener.event(event)));
  }

  @Override
  public AtomicMap<String, byte[]> sync(Duration operationTimeout) {
    return new BlockingAtomicMap<>(this, operationTimeout.toMillis());
  }
}
