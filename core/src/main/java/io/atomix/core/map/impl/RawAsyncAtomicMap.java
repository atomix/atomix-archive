package io.atomix.core.map.impl;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import io.atomix.core.collection.AsyncDistributedCollection;
import io.atomix.core.collection.CollectionEvent;
import io.atomix.core.collection.CollectionEventListener;
import io.atomix.core.collection.impl.UnsupportedAsyncDistributedCollection;
import io.atomix.core.iterator.AsyncIterator;
import io.atomix.core.iterator.impl.StreamHandlerIterator;
import io.atomix.core.map.AsyncAtomicMap;
import io.atomix.core.map.AtomicMap;
import io.atomix.core.map.AtomicMapEvent;
import io.atomix.core.map.AtomicMapEventListener;
import io.atomix.core.set.AsyncDistributedSet;
import io.atomix.core.set.impl.UnsupportedAsyncDistributedSet;
import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionLog;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.impl.SessionEnabledAsyncPrimitive;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.stream.EncodingStreamHandler;
import io.atomix.utils.stream.StreamHandler;
import io.atomix.utils.time.Versioned;

/**
 * Raw asynchronous atomic map.
 */
public class RawAsyncAtomicMap extends SessionEnabledAsyncPrimitive<MapProxy, AsyncAtomicMap<String, byte[]>> implements AsyncAtomicMap<String, byte[]> {
  private final Map<AtomicMapEventListener<String, byte[]>, Executor> eventListeners = new ConcurrentHashMap<>();
  private volatile long streamId;

  public RawAsyncAtomicMap(MapProxy proxy, Duration timeout, PrimitiveManagementService managementService) {
    super(proxy, timeout, managementService);
  }

  @Override
  public CompletableFuture<Integer> size() {
    return execute(MapProxy::size, SizeRequest.newBuilder().build())
        .thenApply(response -> response.getSize());
  }

  @Override
  public CompletableFuture<Boolean> containsKey(String key) {
    return execute(MapProxy::containsKey, ContainsKeyRequest.newBuilder().addKeys(key).build())
        .thenApply(response -> response.getContainsKey());
  }

  @Override
  public CompletableFuture<Boolean> containsValue(byte[] value) {
    return Futures.exceptionalFuture(new UnsupportedOperationException());
  }

  @Override
  public CompletableFuture<Versioned<byte[]>> get(String key) {
    return execute(MapProxy::get, GetRequest.newBuilder().setKey(key).build())
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
    return execute(MapProxy::get, GetRequest.newBuilder().setKey(key).build())
        .thenCompose(response -> {
          byte[] currentValue = response.getVersion() > 0 ? response.getValue().toByteArray() : null;
          if (!condition.test(currentValue)) {
            return CompletableFuture.completedFuture(
                response.getVersion() > 0
                    ? new Versioned<>(response.getValue().toByteArray(), response.getVersion())
                    : null);
          }

          // Compute the new value.
          byte[] computedValue;
          try {
            computedValue = remappingFunction.apply(key, currentValue);
          } catch (Exception e) {
            return Futures.exceptionalFuture(e);
          }

          // If both the old and new value are null, return a null result.
          if (currentValue == null && computedValue == null) {
            return CompletableFuture.completedFuture(null);
          }

          // If the response was empty, add the value only if the key is empty.
          if (response.getVersion() == 0) {
            return execute(MapProxy::put, PutRequest.newBuilder()
                .setKey(key)
                .setValue(ByteString.copyFrom(computedValue))
                .setVersion(MapService.VERSION_EMPTY)
                .build())
                .thenCompose(result -> {
                  if (result.getStatus() == UpdateStatus.WRITE_LOCK
                      || result.getStatus() == UpdateStatus.PRECONDITION_FAILED) {
                    return Futures.exceptionalFuture(new PrimitiveException.ConcurrentModification());
                  }
                  return CompletableFuture.completedFuture(new Versioned<>(computedValue, result.getNewVersion()));
                });
          }
          // If the computed value is null, remove the value if the version has not changed.
          else if (computedValue == null) {
            return remove(key, response.getVersion())
                .thenCompose(succeeded -> {
                  if (!succeeded) {
                    return Futures.exceptionalFuture(new PrimitiveException.ConcurrentModification());
                  }
                  return CompletableFuture.completedFuture(null);
                });
          }
          // If both the current value and the computed value are non-empty, update the value
          // if the key has not changed.
          else {
            return execute(MapProxy::replace, ReplaceRequest.newBuilder()
                .setKey(key)
                .setPreviousVersion(response.getVersion())
                .setNewValue(ByteString.copyFrom(computedValue))
                .build())
                .thenCompose(result -> {
                  if (result.getStatus() == UpdateStatus.WRITE_LOCK
                      || result.getStatus() == UpdateStatus.PRECONDITION_FAILED) {
                    return Futures.exceptionalFuture(new PrimitiveException.ConcurrentModification());
                  }
                  return CompletableFuture.completedFuture(new Versioned<>(computedValue, result.getNewVersion()));
                });
          }
        });
  }

  @Override
  public CompletableFuture<Versioned<byte[]>> put(String key, byte[] value, Duration ttl) {
    return execute(MapProxy::put, PutRequest.newBuilder()
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
  public CompletableFuture<Versioned<byte[]>> putIfAbsent(String key, byte[] value, Duration ttl) {
    return execute(MapProxy::put, PutRequest.newBuilder()
        .setKey(key)
        .setValue(ByteString.copyFrom(value))
        .setTtl(ttl.toMillis())
        .setVersion(MapService.VERSION_EMPTY)
        .build())
        .thenApply(response -> {
          if (response.getStatus() == UpdateStatus.WRITE_LOCK) {
            throw new PrimitiveException.ConcurrentModification();
          }
          if (response.getStatus() == UpdateStatus.PRECONDITION_FAILED) {
            return new Versioned<>(response.getPreviousValue().toByteArray(), response.getPreviousVersion());
          }
          return null;
        });
  }

  @Override
  public CompletableFuture<Versioned<byte[]>> remove(String key) {
    return execute(MapProxy::remove, RemoveRequest.newBuilder().setKey(key).build())
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
    return execute(MapProxy::clear, ClearRequest.newBuilder().build())
        .thenApply(response -> null);
  }

  @Override
  public AsyncDistributedSet<String> keySet() {
    return new KeySet();
  }

  @Override
  public AsyncDistributedCollection<Versioned<byte[]>> values() {
    return new Values();
  }

  @Override
  public AsyncDistributedSet<Map.Entry<String, Versioned<byte[]>>> entrySet() {
    return new EntrySet();
  }

  @Override
  public CompletableFuture<Boolean> remove(String key, byte[] value) {
    return execute(MapProxy::remove, RemoveRequest.newBuilder().setKey(key).setValue(ByteString.copyFrom(value)).build())
        .thenApply(response -> {
          if (response.getStatus() == UpdateStatus.WRITE_LOCK) {
            throw new PrimitiveException.ConcurrentModification();
          }
          return response.getStatus() == UpdateStatus.OK;
        });
  }

  @Override
  public CompletableFuture<Boolean> remove(String key, long version) {
    return execute(MapProxy::remove, RemoveRequest.newBuilder().setKey(key).setVersion(version).build())
        .thenApply(response -> {
          if (response.getStatus() == UpdateStatus.WRITE_LOCK) {
            throw new PrimitiveException.ConcurrentModification();
          }
          return response.getStatus() == UpdateStatus.OK;
        });
  }

  @Override
  public CompletableFuture<Versioned<byte[]>> replace(String key, byte[] value) {
    return execute(MapProxy::replace, ReplaceRequest.newBuilder()
        .setKey(key)
        .setNewValue(ByteString.copyFrom(value))
        .build())
        .thenApply(response -> {
          if (response.getStatus() == UpdateStatus.WRITE_LOCK) {
            throw new PrimitiveException.ConcurrentModification();
          } else if (response.getStatus() == UpdateStatus.PRECONDITION_FAILED) {
            return null;
          } else if (response.getStatus() == UpdateStatus.OK) {
            return new Versioned<>(response.getPreviousValue().toByteArray(), response.getPreviousVersion());
          }
          return null;
        });
  }

  @Override
  public CompletableFuture<Boolean> replace(String key, byte[] oldValue, byte[] newValue) {
    return execute(MapProxy::replace, ReplaceRequest.newBuilder()
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
    return execute(MapProxy::replace, ReplaceRequest.newBuilder()
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
      return execute(MapProxy::listen, ListenRequest.newBuilder().build(), new StreamHandler<ListenResponse>() {
        @Override
        public void next(ListenResponse response) {
          eventListeners.forEach((l, e) -> e.execute(() -> l.event(new AtomicMapEvent<>(
              AtomicMapEvent.Type.valueOf(response.getType().name()),
              response.getKey(),
              !response.getNewValue().isEmpty() ? new Versioned<>(response.getNewValue().toByteArray(), response.getNewVersion()) : null,
              !response.getOldValue().isEmpty() ? new Versioned<>(response.getOldValue().toByteArray(), response.getOldVersion()) : null))));
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
    } else {
      eventListeners.put(listener, executor);
      return CompletableFuture.completedFuture(null);
    }
  }

  @Override
  public synchronized CompletableFuture<Void> removeListener(AtomicMapEventListener<String, byte[]> listener) {
    eventListeners.remove(listener);
    if (eventListeners.isEmpty()) {
      return execute(MapProxy::unlisten, UnlistenRequest.newBuilder().setStreamId(streamId).build())
          .thenApply(response -> null);
    } else {
      return CompletableFuture.completedFuture(null);
    }
  }

  @Override
  public CompletableFuture<Boolean> prepare(TransactionLog<MapUpdate<String, byte[]>> transactionLog) {
    return execute(MapProxy::prepare, PrepareRequest.newBuilder()
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
    return execute(MapProxy::commit, CommitRequest.newBuilder()
        .setTransactionId(transactionId.id())
        .build())
        .thenApply(response -> null);
  }

  @Override
  public CompletableFuture<Void> rollback(TransactionId transactionId) {
    return execute(MapProxy::rollback, RollbackRequest.newBuilder()
        .setTransactionId(transactionId.id())
        .build())
        .thenApply(response -> null);
  }

  @Override
  public AtomicMap<String, byte[]> sync(Duration operationTimeout) {
    return new BlockingAtomicMap<>(this, operationTimeout.toMillis());
  }

  private class EntrySet extends UnsupportedAsyncDistributedSet<Map.Entry<String, Versioned<byte[]>>> {
    private final Map<CollectionEventListener<Map.Entry<String, Versioned<byte[]>>>, AtomicMapEventListener<String, byte[]>> eventListeners = Maps.newIdentityHashMap();

    @Override
    public String name() {
      return RawAsyncAtomicMap.this.name();
    }

    @Override
    public CompletableFuture<Boolean> add(Map.Entry<String, Versioned<byte[]>> element) {
      return execute(MapProxy::put, PutRequest.newBuilder()
          .setKey(element.getKey())
          .setValue(ByteString.copyFrom(element.getValue().value()))
          .build())
          .thenApply(response -> {
            if (response.getStatus() == UpdateStatus.WRITE_LOCK) {
              throw new PrimitiveException.ConcurrentModification();
            }
            return response.getStatus() != UpdateStatus.NOOP;
          });
    }

    @Override
    public CompletableFuture<Boolean> remove(Map.Entry<String, Versioned<byte[]>> element) {
      return RawAsyncAtomicMap.this.remove(element.getKey(), element.getValue().version());
    }

    @Override
    public CompletableFuture<Integer> size() {
      return RawAsyncAtomicMap.this.size();
    }

    @Override
    public CompletableFuture<Boolean> isEmpty() {
      return RawAsyncAtomicMap.this.isEmpty();
    }

    @Override
    public CompletableFuture<Void> clear() {
      return RawAsyncAtomicMap.this.clear();
    }

    @Override
    public CompletableFuture<Boolean> contains(Map.Entry<String, Versioned<byte[]>> element) {
      return get(element.getKey()).thenApply(result -> result != null && result.equals(element.getValue()));
    }

    @Override
    public synchronized CompletableFuture<Void> addListener(CollectionEventListener<Map.Entry<String, Versioned<byte[]>>> listener, Executor executor) {
      AtomicMapEventListener<String, byte[]> mapListener = event -> {
        switch (event.type()) {
          case INSERTED:
            listener.event(new CollectionEvent<>(CollectionEvent.Type.ADDED, Maps.immutableEntry(event.key(), event.newValue())));
            break;
          case UPDATED:
            listener.event(new CollectionEvent<>(CollectionEvent.Type.REMOVED, Maps.immutableEntry(event.key(), event.oldValue())));
            listener.event(new CollectionEvent<>(CollectionEvent.Type.ADDED, Maps.immutableEntry(event.key(), event.newValue())));
            break;
          case REMOVED:
            listener.event(new CollectionEvent<>(CollectionEvent.Type.REMOVED, Maps.immutableEntry(event.key(), event.oldValue())));
            break;
          default:
            break;
        }
      };
      if (eventListeners.putIfAbsent(listener, mapListener) == null) {
        return RawAsyncAtomicMap.this.addListener(mapListener, executor);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public synchronized CompletableFuture<Void> removeListener(CollectionEventListener<Map.Entry<String, Versioned<byte[]>>> listener) {
      AtomicMapEventListener<String, byte[]> mapListener = eventListeners.remove(listener);
      if (mapListener != null) {
        return RawAsyncAtomicMap.this.removeListener(mapListener);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public AsyncIterator<Map.Entry<String, Versioned<byte[]>>> iterator() {
      StreamHandlerIterator<Map.Entry<String, Versioned<byte[]>>> iterator = new StreamHandlerIterator<>();
      execute(
          MapProxy::entries,
          EntriesRequest.newBuilder().build(),
          new EncodingStreamHandler<>(iterator, response -> Maps.immutableEntry(
              response.getKey(),
              new Versioned<>(response.getValue().toByteArray(), response.getVersion()))));
      return iterator;
    }
  }

  private class KeySet extends UnsupportedAsyncDistributedSet<String> {
    private final Map<CollectionEventListener<String>, AtomicMapEventListener<String, byte[]>> eventListeners = Maps.newIdentityHashMap();

    @Override
    public String name() {
      return RawAsyncAtomicMap.this.name();
    }

    @Override
    public CompletableFuture<Boolean> remove(String element) {
      return RawAsyncAtomicMap.this.remove(element)
          .thenApply(value -> value != null);
    }

    @Override
    public CompletableFuture<Integer> size() {
      return RawAsyncAtomicMap.this.size();
    }

    @Override
    public CompletableFuture<Boolean> isEmpty() {
      return RawAsyncAtomicMap.this.isEmpty();
    }

    @Override
    public CompletableFuture<Void> clear() {
      return RawAsyncAtomicMap.this.clear();
    }

    @Override
    public CompletableFuture<Boolean> contains(String element) {
      return containsKey(element);
    }

    @Override
    @SuppressWarnings("unchecked")
    public CompletableFuture<Boolean> containsAll(Collection<? extends String> c) {
      return execute(MapProxy::containsKey, ContainsKeyRequest.newBuilder().addAllKeys((Iterable<String>) c).build())
          .thenApply(response -> response.getContainsKey());
    }

    @Override
    public synchronized CompletableFuture<Void> addListener(CollectionEventListener<String> listener, Executor executor) {
      AtomicMapEventListener<String, byte[]> mapListener = event -> {
        switch (event.type()) {
          case INSERTED:
            listener.event(new CollectionEvent<>(CollectionEvent.Type.ADDED, event.key()));
            break;
          case REMOVED:
            listener.event(new CollectionEvent<>(CollectionEvent.Type.REMOVED, event.key()));
            break;
          default:
            break;
        }
      };
      if (eventListeners.putIfAbsent(listener, mapListener) == null) {
        return RawAsyncAtomicMap.this.addListener(mapListener, executor);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public synchronized CompletableFuture<Void> removeListener(CollectionEventListener<String> listener) {
      AtomicMapEventListener<String, byte[]> mapListener = eventListeners.remove(listener);
      if (mapListener != null) {
        return RawAsyncAtomicMap.this.removeListener(mapListener);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public AsyncIterator<String> iterator() {
      StreamHandlerIterator<String> iterator = new StreamHandlerIterator<>();
      execute(
          MapProxy::keys,
          KeysRequest.newBuilder().build(),
          new EncodingStreamHandler<>(iterator, KeysResponse::getKey));
      return iterator;
    }
  }

  private class Values extends UnsupportedAsyncDistributedCollection<Versioned<byte[]>> {
    private final Map<CollectionEventListener<Versioned<byte[]>>, AtomicMapEventListener<String, byte[]>> eventListeners = Maps.newIdentityHashMap();

    @Override
    public String name() {
      return RawAsyncAtomicMap.this.name();
    }

    @Override
    public CompletableFuture<Integer> size() {
      return RawAsyncAtomicMap.this.size();
    }

    @Override
    public CompletableFuture<Boolean> isEmpty() {
      return RawAsyncAtomicMap.this.isEmpty();
    }

    @Override
    public synchronized CompletableFuture<Void> addListener(CollectionEventListener<Versioned<byte[]>> listener, Executor executor) {
      AtomicMapEventListener<String, byte[]> mapListener = event -> {
        switch (event.type()) {
          case INSERTED:
            listener.event(new CollectionEvent<>(CollectionEvent.Type.ADDED, event.newValue()));
            break;
          case UPDATED:
            listener.event(new CollectionEvent<>(CollectionEvent.Type.REMOVED, event.oldValue()));
            listener.event(new CollectionEvent<>(CollectionEvent.Type.ADDED, event.newValue()));
            break;
          case REMOVED:
            listener.event(new CollectionEvent<>(CollectionEvent.Type.REMOVED, event.oldValue()));
            break;
          default:
            break;
        }
      };
      if (eventListeners.putIfAbsent(listener, mapListener) == null) {
        return RawAsyncAtomicMap.this.addListener(mapListener, executor);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public synchronized CompletableFuture<Void> removeListener(CollectionEventListener<Versioned<byte[]>> listener) {
      AtomicMapEventListener<String, byte[]> mapListener = eventListeners.remove(listener);
      if (mapListener != null) {
        return RawAsyncAtomicMap.this.removeListener(mapListener);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public AsyncIterator<Versioned<byte[]>> iterator() {
      StreamHandlerIterator<Versioned<byte[]>> iterator = new StreamHandlerIterator<>();
      execute(
          MapProxy::entries,
          EntriesRequest.newBuilder().build(),
          new EncodingStreamHandler<>(
              iterator,
              response -> new Versioned<>(response.getValue().toByteArray(), response.getVersion())));
      return iterator;
    }
  }
}
