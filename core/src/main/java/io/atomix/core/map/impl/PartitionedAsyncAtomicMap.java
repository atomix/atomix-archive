package io.atomix.core.map.impl;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Predicate;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.atomix.core.collection.AsyncDistributedCollection;
import io.atomix.core.collection.CollectionEvent;
import io.atomix.core.collection.CollectionEventListener;
import io.atomix.core.collection.impl.UnsupportedAsyncDistributedCollection;
import io.atomix.core.iterator.AsyncIterator;
import io.atomix.core.iterator.impl.PartitionedAsyncIterator;
import io.atomix.core.map.AsyncAtomicMap;
import io.atomix.core.map.AtomicMap;
import io.atomix.core.map.AtomicMapEventListener;
import io.atomix.core.set.AsyncDistributedSet;
import io.atomix.core.set.impl.UnsupportedAsyncDistributedSet;
import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionLog;
import io.atomix.primitive.PartitionedAsyncPrimitive;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.Partitioner;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.time.Versioned;

/**
 * Partitioned asynchronous atomic map.
 */
public class PartitionedAsyncAtomicMap
    extends PartitionedAsyncPrimitive<AsyncAtomicMap<String, byte[]>>
    implements AsyncAtomicMap<String, byte[]> {

  public PartitionedAsyncAtomicMap(
      String name,
      PrimitiveType type,
      Map<PartitionId, AsyncAtomicMap<String, byte[]>> partitions,
      Partitioner<String> partitioner) {
    super(name, type, partitions, partitioner);
  }

  @Override
  public CompletableFuture<Integer> size() {
    return Futures.allOf(getPartitions().stream()
        .map(AsyncAtomicMap::size))
        .thenApply(results -> results.reduce(Math::addExact).orElse(0));
  }

  @Override
  public CompletableFuture<Boolean> containsKey(String key) {
    return getPartition(key).containsKey(key);
  }

  @Override
  public CompletableFuture<Boolean> containsValue(byte[] value) {
    return Futures.allOf(getPartitions().stream().map(partition -> partition.containsValue(value)))
        .thenApply(results -> results.filter(Predicate.isEqual(true)).findFirst().orElse(false));
  }

  @Override
  public CompletableFuture<Versioned<byte[]>> get(String key) {
    return getPartition(key).get(key);
  }

  @Override
  public CompletableFuture<Versioned<byte[]>> getOrDefault(String key, byte[] defaultValue) {
    return getPartition(key).getOrDefault(key, defaultValue);
  }

  @Override
  public CompletableFuture<Versioned<byte[]>> computeIf(
      String key,
      Predicate<? super byte[]> condition,
      BiFunction<? super String, ? super byte[], ? extends byte[]> remappingFunction) {
    return getPartition(key).computeIf(key, condition, remappingFunction);
  }

  @Override
  public CompletableFuture<Versioned<byte[]>> put(String key, byte[] value, Duration ttl) {
    return getPartition(key).put(key, value, ttl);
  }

  @Override
  public CompletableFuture<Versioned<byte[]>> remove(String key) {
    return getPartition(key).remove(key);
  }

  @Override
  public CompletableFuture<Void> clear() {
    return Futures.allOf(getPartitions().stream().map(partition -> partition.clear()))
        .thenApply(results -> null);
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
  public CompletableFuture<Versioned<byte[]>> putIfAbsent(String key, byte[] value, Duration ttl) {
    return getPartition(key).putIfAbsent(key, value, ttl);
  }

  @Override
  public CompletableFuture<Boolean> remove(String key, byte[] value) {
    return getPartition(key).remove(key, value);
  }

  @Override
  public CompletableFuture<Boolean> remove(String key, long version) {
    return getPartition(key).remove(key, version);
  }

  @Override
  public CompletableFuture<Versioned<byte[]>> replace(String key, byte[] value) {
    return getPartition(key).replace(key, value);
  }

  @Override
  public CompletableFuture<Boolean> replace(String key, byte[] oldValue, byte[] newValue) {
    return getPartition(key).replace(key, oldValue, newValue);
  }

  @Override
  public CompletableFuture<Boolean> replace(String key, long oldVersion, byte[] newValue) {
    return getPartition(key).replace(key, oldVersion, newValue);
  }

  @Override
  public CompletableFuture<Void> addListener(AtomicMapEventListener<String, byte[]> listener, Executor executor) {
    return Futures.allOf(getPartitions().stream().map(partition -> partition.addListener(listener, executor)))
        .thenApply(results -> null);
  }

  @Override
  public CompletableFuture<Void> removeListener(AtomicMapEventListener<String, byte[]> listener) {
    return Futures.allOf(getPartitions().stream().map(partition -> partition.removeListener(listener)))
        .thenApply(results -> null);
  }

  @Override
  public CompletableFuture<Boolean> prepare(TransactionLog<MapUpdate<String, byte[]>> transactionLog) {
    Map<PartitionId, List<MapUpdate<String, byte[]>>> updatesGroupedByMap = Maps.newIdentityHashMap();
    transactionLog.records().forEach(update -> {
      updatesGroupedByMap.computeIfAbsent(getPartitionId(update.key()), k -> Lists.newLinkedList()).add(update);
    });
    Map<PartitionId, TransactionLog<MapUpdate<String, byte[]>>> transactionsByMap =
        Maps.transformValues(updatesGroupedByMap, list -> new TransactionLog<>(transactionLog.transactionId(), transactionLog.version(), list));

    return Futures.allOf(transactionsByMap.entrySet()
        .stream()
        .map(e -> getPartition(e.getKey()).prepare(e.getValue())))
        .thenApply(results -> results.reduce(Boolean::logicalAnd).orElse(true));
  }

  @Override
  public CompletableFuture<Void> commit(TransactionId transactionId) {
    return Futures.allOf(getPartitions().stream().map(partition -> partition.commit(transactionId)))
        .thenApply(results -> null);
  }

  @Override
  public CompletableFuture<Void> rollback(TransactionId transactionId) {
    return Futures.allOf(getPartitions().stream().map(partition -> partition.rollback(transactionId)))
        .thenApply(results -> null);
  }

  @Override
  public AtomicMap<String, byte[]> sync(Duration operationTimeout) {
    return new BlockingAtomicMap<>(this, operationTimeout.toMillis());
  }

  private class EntrySet extends UnsupportedAsyncDistributedSet<Map.Entry<String, Versioned<byte[]>>> {
    private final Map<CollectionEventListener<Map.Entry<String, Versioned<byte[]>>>, AtomicMapEventListener<String, byte[]>> eventListeners = Maps.newIdentityHashMap();

    @Override
    public String name() {
      return PartitionedAsyncAtomicMap.this.name();
    }

    @Override
    public CompletableFuture<Boolean> add(Map.Entry<String, Versioned<byte[]>> element) {
      return getPartition(element.getKey()).entrySet().add(element);
    }

    @Override
    public CompletableFuture<Boolean> remove(Map.Entry<String, Versioned<byte[]>> element) {
      return PartitionedAsyncAtomicMap.this.remove(element.getKey(), element.getValue().version());
    }

    @Override
    public CompletableFuture<Integer> size() {
      return PartitionedAsyncAtomicMap.this.size();
    }

    @Override
    public CompletableFuture<Boolean> isEmpty() {
      return PartitionedAsyncAtomicMap.this.isEmpty();
    }

    @Override
    public CompletableFuture<Void> clear() {
      return PartitionedAsyncAtomicMap.this.clear();
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
        return PartitionedAsyncAtomicMap.this.addListener(mapListener, executor);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public synchronized CompletableFuture<Void> removeListener(CollectionEventListener<Map.Entry<String, Versioned<byte[]>>> listener) {
      AtomicMapEventListener<String, byte[]> mapListener = eventListeners.remove(listener);
      if (mapListener != null) {
        return PartitionedAsyncAtomicMap.this.removeListener(mapListener);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public AsyncIterator<Map.Entry<String, Versioned<byte[]>>> iterator() {
      return new PartitionedAsyncIterator<>(getPartitions().stream().map(map -> map.entrySet().iterator()).iterator());
    }
  }

  private class KeySet extends UnsupportedAsyncDistributedSet<String> {
    private final Map<CollectionEventListener<String>, AtomicMapEventListener<String, byte[]>> eventListeners = Maps.newIdentityHashMap();

    @Override
    public String name() {
      return PartitionedAsyncAtomicMap.this.name();
    }

    @Override
    public CompletableFuture<Boolean> remove(String element) {
      return PartitionedAsyncAtomicMap.this.remove(element)
          .thenApply(value -> value != null);
    }

    @Override
    public CompletableFuture<Integer> size() {
      return PartitionedAsyncAtomicMap.this.size();
    }

    @Override
    public CompletableFuture<Boolean> isEmpty() {
      return PartitionedAsyncAtomicMap.this.isEmpty();
    }

    @Override
    public CompletableFuture<Void> clear() {
      return PartitionedAsyncAtomicMap.this.clear();
    }

    @Override
    public CompletableFuture<Boolean> contains(String element) {
      return containsKey(element);
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
        return PartitionedAsyncAtomicMap.this.addListener(mapListener, executor);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public synchronized CompletableFuture<Void> removeListener(CollectionEventListener<String> listener) {
      AtomicMapEventListener<String, byte[]> mapListener = eventListeners.remove(listener);
      if (mapListener != null) {
        return PartitionedAsyncAtomicMap.this.removeListener(mapListener);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public AsyncIterator<String> iterator() {
      return new PartitionedAsyncIterator<>(getPartitions().stream().map(map -> map.keySet().iterator()).iterator());
    }
  }

  private class Values extends UnsupportedAsyncDistributedCollection<Versioned<byte[]>> {
    private final Map<CollectionEventListener<Versioned<byte[]>>, AtomicMapEventListener<String, byte[]>> eventListeners = Maps.newIdentityHashMap();

    @Override
    public String name() {
      return PartitionedAsyncAtomicMap.this.name();
    }

    @Override
    public CompletableFuture<Integer> size() {
      return PartitionedAsyncAtomicMap.this.size();
    }

    @Override
    public CompletableFuture<Boolean> isEmpty() {
      return PartitionedAsyncAtomicMap.this.isEmpty();
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
        return PartitionedAsyncAtomicMap.this.addListener(mapListener, executor);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public synchronized CompletableFuture<Void> removeListener(CollectionEventListener<Versioned<byte[]>> listener) {
      AtomicMapEventListener<String, byte[]> mapListener = eventListeners.remove(listener);
      if (mapListener != null) {
        return PartitionedAsyncAtomicMap.this.removeListener(mapListener);
      }
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public AsyncIterator<Versioned<byte[]>> iterator() {
      return new PartitionedAsyncIterator<>(getPartitions().stream().map(map -> map.values().iterator()).iterator());
    }
  }
}
