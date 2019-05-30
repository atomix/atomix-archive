package io.atomix.client.map.impl;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Predicate;

import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import io.atomix.api.map.ClearRequest;
import io.atomix.api.map.ClearResponse;
import io.atomix.api.map.CloseRequest;
import io.atomix.api.map.CloseResponse;
import io.atomix.api.map.CreateRequest;
import io.atomix.api.map.CreateResponse;
import io.atomix.api.map.EntriesRequest;
import io.atomix.api.map.EntriesResponse;
import io.atomix.api.map.EventRequest;
import io.atomix.api.map.EventResponse;
import io.atomix.api.map.ExistsRequest;
import io.atomix.api.map.ExistsResponse;
import io.atomix.api.map.GetRequest;
import io.atomix.api.map.GetResponse;
import io.atomix.api.map.KeepAliveRequest;
import io.atomix.api.map.KeepAliveResponse;
import io.atomix.api.map.MapServiceGrpc;
import io.atomix.api.map.PutRequest;
import io.atomix.api.map.PutResponse;
import io.atomix.api.map.RemoveRequest;
import io.atomix.api.map.RemoveResponse;
import io.atomix.api.map.ReplaceRequest;
import io.atomix.api.map.ReplaceResponse;
import io.atomix.api.map.ResponseStatus;
import io.atomix.api.map.SizeRequest;
import io.atomix.api.map.SizeResponse;
import io.atomix.api.primitive.PrimitiveId;
import io.atomix.client.PrimitiveException;
import io.atomix.client.PrimitiveManagementService;
import io.atomix.client.Versioned;
import io.atomix.client.collection.AsyncDistributedCollection;
import io.atomix.client.collection.CollectionEvent;
import io.atomix.client.collection.CollectionEventListener;
import io.atomix.client.collection.impl.UnsupportedAsyncDistributedCollection;
import io.atomix.client.impl.AbstractManagedPrimitive;
import io.atomix.client.impl.TranscodingStreamObserver;
import io.atomix.client.iterator.AsyncIterator;
import io.atomix.client.iterator.impl.StreamObserverIterator;
import io.atomix.client.map.AsyncAtomicMap;
import io.atomix.client.map.AtomicMap;
import io.atomix.client.map.AtomicMapEvent;
import io.atomix.client.map.AtomicMapEventListener;
import io.atomix.client.set.AsyncDistributedSet;
import io.atomix.client.set.impl.UnsupportedAsyncDistributedSet;
import io.atomix.client.utils.concurrent.Futures;
import io.grpc.stub.StreamObserver;

/**
 * Default asynchronous atomic map primitive.
 */
public class DefaultAsyncAtomicMap extends AbstractManagedPrimitive<MapServiceGrpc.MapServiceStub, AsyncAtomicMap<String, byte[]>> implements AsyncAtomicMap<String, byte[]> {
  private volatile CompletableFuture<Long> listenFuture;
  private final Map<AtomicMapEventListener<String, byte[]>, Executor> eventListeners = new ConcurrentHashMap<>();

  public DefaultAsyncAtomicMap(PrimitiveId id, PrimitiveManagementService managementService, Duration timeout) {
    super(id, MapServiceGrpc.newStub(managementService.getChannelFactory().getChannel()), managementService, timeout);
  }

  @Override
  public CompletableFuture<Integer> size() {
    return query(
        (map, header, observer) -> map.size(SizeRequest.newBuilder()
            .setMapId(getPrimitiveId())
            .setHeader(header)
            .build(), observer),
        SizeResponse::getHeader)
        .thenApply(response -> response.getSize());
  }

  @Override
  public CompletableFuture<Boolean> containsKey(String key) {
    return query(
        (map, header, observer) -> map.exists(ExistsRequest.newBuilder()
            .setMapId(getPrimitiveId())
            .setHeader(header)
            .build(), observer),
        ExistsResponse::getHeader)
        .thenApply(response -> response.getContainsKey());
  }

  @Override
  public CompletableFuture<Boolean> containsValue(byte[] value) {
    return Futures.exceptionalFuture(new UnsupportedOperationException());
  }

  @Override
  public CompletableFuture<Versioned<byte[]>> get(String key) {
    return query(
        (map, header, observer) -> map.get(GetRequest.newBuilder()
            .setMapId(getPrimitiveId())
            .setHeader(header)
            .setKey(key)
            .build(), observer),
        GetResponse::getHeader)
        .thenApply(response -> response.getVersion() != 0
            ? new Versioned<>(response.getValue().toByteArray(), response.getVersion())
            : null);
  }

  @Override
  public CompletableFuture<Versioned<byte[]>> getOrDefault(String key, byte[] defaultValue) {
    return query(
        (map, header, observer) -> map.get(GetRequest.newBuilder()
            .setMapId(getPrimitiveId())
            .setHeader(header)
            .setKey(key)
            .build(), observer),
        GetResponse::getHeader)
        .thenApply(response -> response.getVersion() != 0
            ? new Versioned<>(response.getValue().toByteArray(), response.getVersion())
            : new Versioned<>(defaultValue, 0));
  }

  @Override
  public CompletableFuture<Versioned<byte[]>> computeIf(
      String key,
      Predicate<? super byte[]> condition,
      BiFunction<? super String, ? super byte[], ? extends byte[]> remappingFunction) {
    return query(
        (map, header, observer) -> map.get(GetRequest.newBuilder()
            .setMapId(getPrimitiveId())
            .setHeader(header)
            .setKey(key)
            .build(), observer),
        GetResponse::getHeader)
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
            return command(
                (map, header, observer) -> map.put(PutRequest.newBuilder()
                    .setMapId(getPrimitiveId())
                    .setHeader(header)
                    .setKey(key)
                    .setValue(ByteString.copyFrom(computedValue))
                    .setVersion(-1)
                    .build(), observer),
                PutResponse::getHeader)
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
            return command(
                (map, header, observer) -> map.replace(ReplaceRequest.newBuilder()
                    .setMapId(getPrimitiveId())
                    .setHeader(header)
                    .setKey(key)
                    .setPreviousVersion(response.getVersion())
                    .setNewValue(ByteString.copyFrom(computedValue))
                    .build(), observer),
                ReplaceResponse::getHeader)
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
    return command(
        (map, header, observer) -> map.put(PutRequest.newBuilder()
            .setMapId(getPrimitiveId())
            .setHeader(header)
            .setKey(key)
            .setValue(ByteString.copyFrom(value))
            .setTtl(ttl.toMillis())
            .build(), observer),
        PutResponse::getHeader)
        .thenCompose(response -> {
          if (response.getStatus() == ResponseStatus.WRITE_LOCK) {
            return Futures.exceptionalFuture(new PrimitiveException.ConcurrentModification());
          }
          if (!response.getPreviousValue().isEmpty()) {
            return CompletableFuture.completedFuture(new Versioned<>(response.getPreviousValue().toByteArray(), response.getPreviousVersion()));
          }
          return CompletableFuture.completedFuture(null);
        });
  }

  @Override
  public CompletableFuture<Versioned<byte[]>> putIfAbsent(String key, byte[] value, Duration ttl) {
    return command(
        (map, header, observer) -> map.put(PutRequest.newBuilder()
            .setMapId(getPrimitiveId())
            .setHeader(header)
            .setKey(key)
            .setValue(ByteString.copyFrom(value))
            .setVersion(-1)
            .setTtl(ttl.toMillis())
            .build(), observer),
        PutResponse::getHeader)
        .thenCompose(response -> {
          if (response.getStatus() == ResponseStatus.WRITE_LOCK) {
            return Futures.exceptionalFuture(new PrimitiveException.ConcurrentModification());
          }
          if (response.getStatus() == ResponseStatus.PRECONDITION_FAILED) {
            return CompletableFuture.completedFuture(new Versioned<>(response.getPreviousValue().toByteArray(), response.getPreviousVersion()));
          }
          return CompletableFuture.completedFuture(null);
        });
  }

  @Override
  public CompletableFuture<Versioned<byte[]>> remove(String key) {
    return command(
        (map, header, observer) -> map.remove(RemoveRequest.newBuilder()
            .setMapId(getPrimitiveId())
            .setHeader(header)
            .setKey(key)
            .build(), observer),
        RemoveResponse::getHeader)
        .thenCompose(response -> {
          if (response.getStatus() == ResponseStatus.WRITE_LOCK) {
            return Futures.exceptionalFuture(new PrimitiveException.ConcurrentModification());
          }
          if (!response.getPreviousValue().isEmpty()) {
            return CompletableFuture.completedFuture(new Versioned<>(response.getPreviousValue().toByteArray(), response.getPreviousVersion()));
          }
          return CompletableFuture.completedFuture(null);
        });
  }

  @Override
  public CompletableFuture<Void> clear() {
    return command(
        (map, header, observer) -> map.clear(ClearRequest.newBuilder()
            .setMapId(getPrimitiveId())
            .setHeader(header)
            .build(), observer),
        ClearResponse::getHeader)
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
    return command(
        (map, header, observer) -> map.remove(RemoveRequest.newBuilder()
            .setMapId(getPrimitiveId())
            .setHeader(header)
            .setKey(key)
            .setValue(ByteString.copyFrom(value))
            .build(), observer),
        RemoveResponse::getHeader)
        .thenCompose(response -> {
          if (response.getStatus() == ResponseStatus.WRITE_LOCK) {
            return Futures.exceptionalFuture(new PrimitiveException.ConcurrentModification());
          }
          return CompletableFuture.completedFuture(response.getStatus() == ResponseStatus.OK);
        });
  }

  @Override
  public CompletableFuture<Boolean> remove(String key, long version) {
    return command(
        (map, header, observer) -> map.remove(RemoveRequest.newBuilder()
            .setMapId(getPrimitiveId())
            .setHeader(header)
            .setKey(key)
            .setVersion(version)
            .build(), observer),
        RemoveResponse::getHeader)
        .thenCompose(response -> {
          if (response.getStatus() == ResponseStatus.WRITE_LOCK) {
            return Futures.exceptionalFuture(new PrimitiveException.ConcurrentModification());
          }
          return CompletableFuture.completedFuture(response.getStatus() == ResponseStatus.OK);
        });
  }

  @Override
  public CompletableFuture<Versioned<byte[]>> replace(String key, byte[] value) {
    return command(
        (map, header, observer) -> map.replace(ReplaceRequest.newBuilder()
            .setMapId(getPrimitiveId())
            .setHeader(header)
            .setKey(key)
            .setNewValue(ByteString.copyFrom(value))
            .build(), observer),
        ReplaceResponse::getHeader)
        .thenCompose(response -> {
          if (response.getStatus() == ResponseStatus.WRITE_LOCK) {
            return Futures.exceptionalFuture(new PrimitiveException.ConcurrentModification());
          } else if (response.getStatus() == ResponseStatus.PRECONDITION_FAILED) {
            return CompletableFuture.completedFuture(null);
          } else if (response.getStatus() == ResponseStatus.OK) {
            return CompletableFuture.completedFuture(new Versioned<>(response.getPreviousValue().toByteArray(), response.getPreviousVersion()));
          }
          return CompletableFuture.completedFuture(null);
        });
  }

  @Override
  public CompletableFuture<Boolean> replace(String key, byte[] oldValue, byte[] newValue) {
    return command(
        (map, header, observer) -> map.replace(ReplaceRequest.newBuilder()
            .setMapId(getPrimitiveId())
            .setHeader(header)
            .setKey(key)
            .setPreviousValue(ByteString.copyFrom(oldValue))
            .setNewValue(ByteString.copyFrom(newValue))
            .build(), observer),
        ReplaceResponse::getHeader)
        .thenCompose(response -> {
          if (response.getStatus() == ResponseStatus.WRITE_LOCK) {
            return Futures.exceptionalFuture(new PrimitiveException.ConcurrentModification());
          }
          return CompletableFuture.completedFuture(response.getStatus() == ResponseStatus.OK);
        });
  }

  @Override
  public CompletableFuture<Boolean> replace(String key, long oldVersion, byte[] newValue) {
    return command(
        (map, header, observer) -> map.replace(ReplaceRequest.newBuilder()
            .setMapId(getPrimitiveId())
            .setHeader(header)
            .setKey(key)
            .setPreviousVersion(oldVersion)
            .setNewValue(ByteString.copyFrom(newValue))
            .build(), observer),
        ReplaceResponse::getHeader)
        .thenCompose(response -> {
          if (response.getStatus() == ResponseStatus.WRITE_LOCK) {
            return Futures.exceptionalFuture(new PrimitiveException.ConcurrentModification());
          }
          return CompletableFuture.completedFuture(response.getStatus() == ResponseStatus.OK);
        });
  }

  private synchronized CompletableFuture<Void> listen() {
    if (listenFuture == null && !eventListeners.isEmpty()) {
      listenFuture = command(
          (service, header, observer) -> service.events(EventRequest.newBuilder()
              .setMapId(getPrimitiveId())
              .setHeader(header)
              .build(), observer),
          EventResponse::getHeader,
          new StreamObserver<EventResponse>() {
            @Override
            public void onNext(EventResponse response) {
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
              onEvent(event);
            }

            private void onEvent(AtomicMapEvent<String, byte[]> event) {
              eventListeners.forEach((l, e) -> e.execute(() -> l.event(event)));
            }

            @Override
            public void onError(Throwable t) {
              onCompleted();
            }

            @Override
            public void onCompleted() {
              synchronized (DefaultAsyncAtomicMap.this) {
                listenFuture = null;
              }
              listen();
            }
          });
    }
    return listenFuture.thenApply(v -> null);
  }

  @Override
  public synchronized CompletableFuture<Void> addListener(AtomicMapEventListener<String, byte[]> listener, Executor executor) {
    eventListeners.put(listener, executor);
    return listen();
  }

  @Override
  public synchronized CompletableFuture<Void> removeListener(AtomicMapEventListener<String, byte[]> listener) {
    eventListeners.remove(listener);
    return CompletableFuture.completedFuture(null);
  }

  @Override
  protected CompletableFuture<Long> openSession(Duration timeout) {
    return this.<CreateResponse>session((service, header, observer) -> service.create(CreateRequest.newBuilder()
        .setMapId(getPrimitiveId())
        .setTimeout(com.google.protobuf.Duration.newBuilder()
            .setSeconds(timeout.getSeconds())
            .setNanos(timeout.getNano())
            .build())
        .build(), observer))
        .thenApply(response -> response.getHeader().getSessionId());
  }

  @Override
  protected CompletableFuture<Boolean> keepAlive() {
    return this.<KeepAliveResponse>session((service, header, observer) -> service.keepAlive(KeepAliveRequest.newBuilder()
        .setMapId(getPrimitiveId())
        .build(), observer))
        .thenApply(response -> true);
  }

  @Override
  protected CompletableFuture<Void> close(boolean delete) {
    return this.<CloseResponse>session((service, header, observer) -> service.close(CloseRequest.newBuilder()
        .setMapId(getPrimitiveId())
        .setDelete(delete)
        .build(), observer))
        .thenApply(v -> null);
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
      query(
          (map, header, observer) -> map.entries(EntriesRequest.newBuilder()
              .setMapId(getPrimitiveId())
              .setHeader(header)
              .build(), observer),
          EntriesResponse::getHeader,
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
      query(
          (map, header, observer) -> map.entries(EntriesRequest.newBuilder()
              .setMapId(getPrimitiveId())
              .setHeader(header)
              .build(), observer),
          EntriesResponse::getHeader,
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
      query(
          (map, header, observer) -> map.entries(EntriesRequest.newBuilder()
              .setMapId(getPrimitiveId())
              .setHeader(header)
              .build(), observer),
          EntriesResponse::getHeader,
          new TranscodingStreamObserver<>(
              iterator,
              response -> new Versioned<>(response.getValue().toByteArray(), response.getVersion())));
      return iterator;
    }
  }
}
