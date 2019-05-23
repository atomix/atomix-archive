package io.atomix.core.map.impl;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;

import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import io.atomix.core.collection.AsyncDistributedCollection;
import io.atomix.core.collection.CollectionEvent;
import io.atomix.core.collection.CollectionEventListener;
import io.atomix.core.collection.impl.UnsupportedAsyncDistributedCollection;
import io.atomix.core.impl.AbstractAsyncPrimitive;
import io.atomix.core.impl.PrimitiveIdDescriptor;
import io.atomix.core.impl.PrimitivePartition;
import io.atomix.core.impl.TranscodingStreamObserver;
import io.atomix.core.iterator.AsyncIterator;
import io.atomix.core.iterator.impl.StreamObserverIterator;
import io.atomix.core.map.AsyncAtomicMap;
import io.atomix.core.map.AtomicMap;
import io.atomix.core.map.AtomicMapEvent;
import io.atomix.core.map.AtomicMapEventListener;
import io.atomix.core.map.ClearRequest;
import io.atomix.core.map.ClearResponse;
import io.atomix.core.map.CloseRequest;
import io.atomix.core.map.CloseResponse;
import io.atomix.core.map.CreateRequest;
import io.atomix.core.map.CreateResponse;
import io.atomix.core.map.EntriesRequest;
import io.atomix.core.map.EntriesResponse;
import io.atomix.core.map.EventRequest;
import io.atomix.core.map.EventResponse;
import io.atomix.core.map.ExistsRequest;
import io.atomix.core.map.ExistsResponse;
import io.atomix.core.map.GetRequest;
import io.atomix.core.map.GetResponse;
import io.atomix.core.map.KeepAliveRequest;
import io.atomix.core.map.KeepAliveResponse;
import io.atomix.core.map.MapId;
import io.atomix.core.map.MapServiceGrpc;
import io.atomix.core.map.PutRequest;
import io.atomix.core.map.PutResponse;
import io.atomix.core.map.RemoveRequest;
import io.atomix.core.map.RemoveResponse;
import io.atomix.core.map.ReplaceRequest;
import io.atomix.core.map.ReplaceResponse;
import io.atomix.core.map.ResponseStatus;
import io.atomix.core.map.SizeRequest;
import io.atomix.core.map.SizeResponse;
import io.atomix.core.set.AsyncDistributedSet;
import io.atomix.core.set.impl.UnsupportedAsyncDistributedSet;
import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionLog;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.partition.Partitioner;
import io.atomix.primitive.protocol.DistributedLogProtocol;
import io.atomix.primitive.protocol.MultiPrimaryProtocol;
import io.atomix.primitive.protocol.MultiRaftProtocol;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.time.Versioned;
import io.grpc.Channel;
import io.grpc.stub.StreamObserver;

/**
 * Default asynchronous atomic map primitive.
 */
public class DefaultAsyncAtomicMap extends AbstractAsyncPrimitive<MapId, AsyncAtomicMap<String, byte[]>> implements AsyncAtomicMap<String, byte[]> {
  private final MapServiceGrpc.MapServiceStub map;
  private final Map<AtomicMapEventListener<String, byte[]>, Executor> eventListeners = new ConcurrentHashMap<>();

  public DefaultAsyncAtomicMap(
      MapId mapId,
      Supplier<Channel> channelFactory,
      PrimitiveManagementService managementService,
      Partitioner<String> partitioner,
      Duration timeout) {
    super(mapId, MAP_ID_DESCRIPTOR, managementService, partitioner, timeout);
    this.map = MapServiceGrpc.newStub(channelFactory.get());
  }

  @Override
  public CompletableFuture<Integer> size() {
    return this.<SizeResponse>execute(stream -> map.size(SizeRequest.newBuilder()
        .setId(id())
        .addAllHeaders(getQueryHeaders())
        .build(), stream))
        .thenCompose(response -> order(response.getSize(), response.getHeadersList()));
  }

  @Override
  public CompletableFuture<Boolean> containsKey(String key) {
    PrimitivePartition partition = getPartition(key);
    return this.<ExistsResponse>execute(stream -> map.exists(ExistsRequest.newBuilder()
        .setId(id())
        .setHeader(partition.getQueryHeader())
        .setKey(key)
        .build(), stream))
        .thenCompose(response -> partition.order(response.getContainsKey(), response.getHeader()));
  }

  @Override
  public CompletableFuture<Boolean> containsValue(byte[] value) {
    return Futures.exceptionalFuture(new UnsupportedOperationException());
  }

  @Override
  public CompletableFuture<Versioned<byte[]>> get(String key) {
    PrimitivePartition partition = getPartition(key);
    return this.<GetResponse>execute(stream -> map.get(GetRequest.newBuilder()
        .setId(id())
        .setHeader(partition.getQueryHeader())
        .setKey(key)
        .build(), stream))
        .thenCompose(response -> partition.order(
            response.getVersion() != 0
                ? new Versioned<>(response.getValue().toByteArray(), response.getVersion())
                : null, response.getHeader()));
  }

  @Override
  public CompletableFuture<Versioned<byte[]>> getOrDefault(String key, byte[] defaultValue) {
    PrimitivePartition partition = getPartition(key);
    return this.<GetResponse>execute(stream -> map.get(GetRequest.newBuilder()
        .setId(id())
        .setHeader(partition.getQueryHeader())
        .setKey(key)
        .build(), stream))
        .thenCompose(response -> partition.order(
            response.getVersion() != 0
                ? new Versioned<>(response.getValue().toByteArray(), response.getVersion())
                : new Versioned<>(defaultValue, 0), response.getHeader()));
  }

  @Override
  public CompletableFuture<Versioned<byte[]>> computeIf(
      String key,
      Predicate<? super byte[]> condition,
      BiFunction<? super String, ? super byte[], ? extends byte[]> remappingFunction) {
    PrimitivePartition partition = getPartition(key);
    return this.<GetResponse>execute(stream -> map.get(GetRequest.newBuilder()
        .setId(id())
        .setHeader(partition.getQueryHeader())
        .setKey(key)
        .build(), stream))
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
            return this.<PutResponse>execute(stream -> map.put(PutRequest.newBuilder()
                .setId(id())
                .setHeader(partition.getCommandHeader())
                .setKey(key)
                .setValue(ByteString.copyFrom(computedValue))
                .setVersion(MapService.VERSION_EMPTY)
                .build(), stream))
                .thenCompose(result -> {
                  if (result.getStatus() == ResponseStatus.WRITE_LOCK
                      || result.getStatus() == ResponseStatus.PRECONDITION_FAILED) {
                    return Futures.exceptionalFuture(new PrimitiveException.ConcurrentModification());
                  }
                  return CompletableFuture.completedFuture(new Versioned<>(computedValue, result.getHeader().getIndex()));
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
            return this.<ReplaceResponse>execute(stream -> map.replace(ReplaceRequest.newBuilder()
                .setId(id())
                .setHeader(partition.getCommandHeader())
                .setKey(key)
                .setPreviousVersion(response.getVersion())
                .setNewValue(ByteString.copyFrom(computedValue))
                .build(), stream))
                .thenCompose(result -> {
                  if (result.getStatus() == ResponseStatus.WRITE_LOCK
                      || result.getStatus() == ResponseStatus.PRECONDITION_FAILED) {
                    return Futures.exceptionalFuture(new PrimitiveException.ConcurrentModification());
                  }
                  return CompletableFuture.completedFuture(new Versioned<>(computedValue, result.getHeader().getIndex()));
                });
          }
        });
  }

  @Override
  public CompletableFuture<Versioned<byte[]>> put(String key, byte[] value, Duration ttl) {
    PrimitivePartition partition = getPartition(key);
    return this.<PutResponse>execute(stream -> map.put(PutRequest.newBuilder()
        .setId(id())
        .setHeader(partition.getCommandHeader())
        .setKey(key)
        .setValue(ByteString.copyFrom(value))
        .setTtl(ttl.toMillis())
        .build(), stream))
        .thenCompose(response -> {
          if (response.getStatus() == ResponseStatus.WRITE_LOCK) {
            return Futures.exceptionalFuture(new PrimitiveException.ConcurrentModification());
          }
          if (!response.getPreviousValue().isEmpty()) {
            return partition.order(new Versioned<>(response.getPreviousValue().toByteArray(), response.getPreviousVersion()), response.getHeader());
          }
          return partition.order(null, response.getHeader());
        });
  }

  @Override
  public CompletableFuture<Versioned<byte[]>> putIfAbsent(String key, byte[] value, Duration ttl) {
    PrimitivePartition partition = getPartition(key);
    return this.<PutResponse>execute(stream -> map.put(PutRequest.newBuilder()
        .setId(id())
        .setHeader(partition.getCommandHeader())
        .setKey(key)
        .setValue(ByteString.copyFrom(value))
        .setVersion(MapService.VERSION_EMPTY)
        .setTtl(ttl.toMillis())
        .build(), stream))
        .thenCompose(response -> {
          if (response.getStatus() == ResponseStatus.WRITE_LOCK) {
            return Futures.exceptionalFuture(new PrimitiveException.ConcurrentModification());
          }
          if (response.getStatus() == ResponseStatus.PRECONDITION_FAILED) {
            return partition.order(new Versioned<>(response.getPreviousValue().toByteArray(), response.getPreviousVersion()), response.getHeader());
          }
          return partition.order(null, response.getHeader());
        });
  }

  @Override
  public CompletableFuture<Versioned<byte[]>> remove(String key) {
    PrimitivePartition partition = getPartition(key);
    return this.<RemoveResponse>execute(stream -> map.remove(RemoveRequest.newBuilder()
        .setId(id())
        .setHeader(partition.getCommandHeader())
        .setKey(key)
        .build(), stream))
        .thenCompose(response -> {
          if (response.getStatus() == ResponseStatus.WRITE_LOCK) {
            return Futures.exceptionalFuture(new PrimitiveException.ConcurrentModification());
          }
          if (!response.getPreviousValue().isEmpty()) {
            return partition.order(new Versioned<>(response.getPreviousValue().toByteArray(), response.getPreviousVersion()), response.getHeader());
          }
          return partition.order(null, response.getHeader());
        });
  }

  @Override
  public CompletableFuture<Void> clear() {
    return this.<ClearResponse>execute(stream -> map.clear(ClearRequest.newBuilder()
        .setId(id())
        .addAllHeaders(getCommandHeaders())
        .build(), stream))
        .thenCompose(response -> order(null, response.getHeadersList()));
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
    PrimitivePartition partition = getPartition(key);
    return this.<RemoveResponse>execute(stream -> map.remove(RemoveRequest.newBuilder()
        .setId(id())
        .setHeader(partition.getCommandHeader())
        .setKey(key)
        .setValue(ByteString.copyFrom(value))
        .build(), stream))
        .thenCompose(response -> {
          if (response.getStatus() == ResponseStatus.WRITE_LOCK) {
            return Futures.exceptionalFuture(new PrimitiveException.ConcurrentModification());
          }
          return partition.order(response.getStatus() == ResponseStatus.OK, response.getHeader());
        });
  }

  @Override
  public CompletableFuture<Boolean> remove(String key, long version) {
    PrimitivePartition partition = getPartition(key);
    return this.<RemoveResponse>execute(stream -> map.remove(RemoveRequest.newBuilder()
        .setId(id())
        .setHeader(partition.getCommandHeader())
        .setKey(key)
        .setVersion(version)
        .build(), stream))
        .thenCompose(response -> {
          if (response.getStatus() == ResponseStatus.WRITE_LOCK) {
            return Futures.exceptionalFuture(new PrimitiveException.ConcurrentModification());
          }
          return partition.order(response.getStatus() == ResponseStatus.OK, response.getHeader());
        });
  }

  @Override
  public CompletableFuture<Versioned<byte[]>> replace(String key, byte[] value) {
    PrimitivePartition partition = getPartition(key);
    return this.<ReplaceResponse>execute(stream -> map.replace(ReplaceRequest.newBuilder()
        .setId(id())
        .setHeader(partition.getCommandHeader())
        .setKey(key)
        .setNewValue(ByteString.copyFrom(value))
        .build(), stream))
        .thenCompose(response -> {
          if (response.getStatus() == ResponseStatus.WRITE_LOCK) {
            return Futures.exceptionalFuture(new PrimitiveException.ConcurrentModification());
          } else if (response.getStatus() == ResponseStatus.PRECONDITION_FAILED) {
            return partition.order(null, response.getHeader());
          } else if (response.getStatus() == ResponseStatus.OK) {
            return partition.order(new Versioned<>(response.getPreviousValue().toByteArray(), response.getPreviousVersion()), response.getHeader());
          }
          return partition.order(null, response.getHeader());
        });
  }

  @Override
  public CompletableFuture<Boolean> replace(String key, byte[] oldValue, byte[] newValue) {
    PrimitivePartition partition = getPartition(key);
    return this.<ReplaceResponse>execute(stream -> map.replace(ReplaceRequest.newBuilder()
        .setId(id())
        .setHeader(partition.getCommandHeader())
        .setKey(key)
        .setPreviousValue(ByteString.copyFrom(oldValue))
        .setNewValue(ByteString.copyFrom(newValue))
        .build(), stream))
        .thenCompose(response -> {
          if (response.getStatus() == ResponseStatus.WRITE_LOCK) {
            return Futures.exceptionalFuture(new PrimitiveException.ConcurrentModification());
          }
          return partition.order(response.getStatus() == ResponseStatus.OK, response.getHeader());
        });
  }

  @Override
  public CompletableFuture<Boolean> replace(String key, long oldVersion, byte[] newValue) {
    PrimitivePartition partition = getPartition(key);
    return this.<ReplaceResponse>execute(stream -> map.replace(ReplaceRequest.newBuilder()
        .setId(id())
        .setHeader(partition.getCommandHeader())
        .setKey(key)
        .setPreviousVersion(oldVersion)
        .setNewValue(ByteString.copyFrom(newValue))
        .build(), stream))
        .thenCompose(response -> {
          if (response.getStatus() == ResponseStatus.WRITE_LOCK) {
            return Futures.exceptionalFuture(new PrimitiveException.ConcurrentModification());
          }
          return partition.order(response.getStatus() == ResponseStatus.OK, response.getHeader());
        });
  }

  @Override
  public CompletableFuture<Void> addListener(AtomicMapEventListener<String, byte[]> listener, Executor executor) {
    map.events(EventRequest.newBuilder()
        .setId(id())
        .addAllHeaders(getCommandHeaders())
        .build(), new StreamObserver<EventResponse>() {
      @Override
      public void onNext(EventResponse response) {
        PrimitivePartition partition = getPartition(response.getHeader().getPartitionId());
        AtomicMapEvent<String, byte[]> event = null;
        switch (response.getType()) {
          case INSERTED:
            event = new AtomicMapEvent<>(
                AtomicMapEvent.Type.INSERTED,
                response.getKey(),
                new Versioned<>(response.getNewValue().toByteArray(), response.getNewVersion()),
                null);
            break;
          case UPDATED:
            event = new AtomicMapEvent<>(
                AtomicMapEvent.Type.UPDATED,
                response.getKey(),
                new Versioned<>(response.getNewValue().toByteArray(), response.getNewVersion()),
                new Versioned<>(response.getOldValue().toByteArray(), response.getOldVersion()));
            break;
          case REMOVED:
            event = new AtomicMapEvent<>(
                AtomicMapEvent.Type.REMOVED,
                response.getKey(),
                null,
                new Versioned<>(response.getOldValue().toByteArray(), response.getOldVersion()));
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
  public CompletableFuture<Void> removeListener(AtomicMapEventListener<String, byte[]> listener) {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Boolean> prepare(TransactionLog<MapUpdate<String, byte[]>> transactionLog) {
    return Futures.exceptionalFuture(new UnsupportedOperationException());
  }

  @Override
  public CompletableFuture<Void> commit(TransactionId transactionId) {
    return Futures.exceptionalFuture(new UnsupportedOperationException());
  }

  @Override
  public CompletableFuture<Void> rollback(TransactionId transactionId) {
    return Futures.exceptionalFuture(new UnsupportedOperationException());
  }

  @Override
  public CompletableFuture<AsyncAtomicMap<String, byte[]>> connect() {
    return this.<CreateResponse>execute(stream -> map.create(CreateRequest.newBuilder()
        .setId(id())
        .setTimeout(com.google.protobuf.Duration.newBuilder()
            .setSeconds(timeout.getSeconds())
            .setNanos(timeout.getNano())
            .build())
        .build(), stream))
        .thenAccept(response -> {
          startKeepAlive(response.getHeadersList());
        })
        .thenApply(v -> this);
  }

  @Override
  protected CompletableFuture<Void> keepAlive() {
    return this.<KeepAliveResponse>execute(stream -> map.keepAlive(KeepAliveRequest.newBuilder()
        .setId(id())
        .addAllHeaders(getSessionHeaders())
        .build(), stream))
        .thenAccept(response -> completeKeepAlive(response.getHeadersList()));
  }

  @Override
  public CompletableFuture<Void> close() {
    return this.<CloseResponse>execute(stream -> map.close(CloseRequest.newBuilder()
        .setId(id())
        .addAllHeaders(getSessionHeaders())
        .build(), stream))
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
      return DefaultAsyncAtomicMap.this.name();
    }

    @Override
    public CompletableFuture<Boolean> add(Map.Entry<String, Versioned<byte[]>> element) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Boolean> remove(Map.Entry<String, Versioned<byte[]>> element) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
    }

    @Override
    public CompletableFuture<Integer> size() {
      return DefaultAsyncAtomicMap.this.size();
    }

    @Override
    public CompletableFuture<Boolean> isEmpty() {
      return DefaultAsyncAtomicMap.this.isEmpty();
    }

    @Override
    public CompletableFuture<Void> clear() {
      return DefaultAsyncAtomicMap.this.clear();
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
        return DefaultAsyncAtomicMap.this.addListener(mapListener, executor);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public synchronized CompletableFuture<Void> removeListener(CollectionEventListener<Map.Entry<String, Versioned<byte[]>>> listener) {
      AtomicMapEventListener<String, byte[]> mapListener = eventListeners.remove(listener);
      if (mapListener != null) {
        return DefaultAsyncAtomicMap.this.removeListener(mapListener);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public AsyncIterator<Map.Entry<String, Versioned<byte[]>>> iterator() {
      StreamObserverIterator<Map.Entry<String, Versioned<byte[]>>> iterator = new StreamObserverIterator<>();
      map.entries(EntriesRequest.newBuilder()
              .setId(id())
              .addAllHeaders(getQueryHeaders())
              .build(),
          new TranscodingStreamObserver<>(
              iterator,
              response -> Maps.immutableEntry(
                  response.getKey(),
                  new Versioned<>(response.getValue().toByteArray(), response.getVersion()))));
      return iterator;
    }
  }

  private class KeySet extends UnsupportedAsyncDistributedSet<String> {
    private final Map<CollectionEventListener<String>, AtomicMapEventListener<String, byte[]>> eventListeners = Maps.newIdentityHashMap();

    @Override
    public String name() {
      return DefaultAsyncAtomicMap.this.name();
    }

    @Override
    public CompletableFuture<Boolean> remove(String element) {
      return DefaultAsyncAtomicMap.this.remove(element)
          .thenApply(value -> value != null);
    }

    @Override
    public CompletableFuture<Integer> size() {
      return DefaultAsyncAtomicMap.this.size();
    }

    @Override
    public CompletableFuture<Boolean> isEmpty() {
      return DefaultAsyncAtomicMap.this.isEmpty();
    }

    @Override
    public CompletableFuture<Void> clear() {
      return DefaultAsyncAtomicMap.this.clear();
    }

    @Override
    public CompletableFuture<Boolean> contains(String element) {
      return containsKey(element);
    }

    @Override
    @SuppressWarnings("unchecked")
    public CompletableFuture<Boolean> containsAll(Collection<? extends String> c) {
      return Futures.exceptionalFuture(new UnsupportedOperationException());
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
        return DefaultAsyncAtomicMap.this.addListener(mapListener, executor);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public synchronized CompletableFuture<Void> removeListener(CollectionEventListener<String> listener) {
      AtomicMapEventListener<String, byte[]> mapListener = eventListeners.remove(listener);
      if (mapListener != null) {
        return DefaultAsyncAtomicMap.this.removeListener(mapListener);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public AsyncIterator<String> iterator() {
      StreamObserverIterator<String> iterator = new StreamObserverIterator<>();
      map.entries(EntriesRequest.newBuilder()
              .setId(id())
              .addAllHeaders(getQueryHeaders())
              .build(),
          new TranscodingStreamObserver<>(
              iterator,
              EntriesResponse::getKey));
      return iterator;
    }
  }

  private class Values extends UnsupportedAsyncDistributedCollection<Versioned<byte[]>> {
    private final Map<CollectionEventListener<Versioned<byte[]>>, AtomicMapEventListener<String, byte[]>> eventListeners = Maps.newIdentityHashMap();

    @Override
    public String name() {
      return DefaultAsyncAtomicMap.this.name();
    }

    @Override
    public CompletableFuture<Integer> size() {
      return DefaultAsyncAtomicMap.this.size();
    }

    @Override
    public CompletableFuture<Boolean> isEmpty() {
      return DefaultAsyncAtomicMap.this.isEmpty();
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
        return DefaultAsyncAtomicMap.this.addListener(mapListener, executor);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public synchronized CompletableFuture<Void> removeListener(CollectionEventListener<Versioned<byte[]>> listener) {
      AtomicMapEventListener<String, byte[]> mapListener = eventListeners.remove(listener);
      if (mapListener != null) {
        return DefaultAsyncAtomicMap.this.removeListener(mapListener);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public AsyncIterator<Versioned<byte[]>> iterator() {
      StreamObserverIterator<Versioned<byte[]>> iterator = new StreamObserverIterator<>();
      map.entries(EntriesRequest.newBuilder()
              .setId(id())
              .addAllHeaders(getQueryHeaders())
              .build(),
          new TranscodingStreamObserver<>(
              iterator,
              response -> new Versioned<>(response.getValue().toByteArray(), response.getVersion())));
      return iterator;
    }
  }

  private static final PrimitiveIdDescriptor<MapId> MAP_ID_DESCRIPTOR = new PrimitiveIdDescriptor<MapId>() {
    @Override
    public String getName(MapId id) {
      return id.getName();
    }

    @Override
    public boolean hasMultiRaftProtocol(MapId id) {
      return id.hasRaft();
    }

    @Override
    public MultiRaftProtocol getMultiRaftProtocol(MapId id) {
      return id.getRaft();
    }

    @Override
    public boolean hasMultiPrimaryProtocol(MapId id) {
      return id.hasMultiPrimary();
    }

    @Override
    public MultiPrimaryProtocol getMultiPrimaryProtocol(MapId id) {
      return id.getMultiPrimary();
    }

    @Override
    public boolean hasDistributedLogProtocol(MapId id) {
      return id.hasLog();
    }

    @Override
    public DistributedLogProtocol getDistributedLogProtocol(MapId id) {
      return id.getLog();
    }
  };
}
