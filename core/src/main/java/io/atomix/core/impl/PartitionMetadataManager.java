package io.atomix.core.impl;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.cluster.messaging.ClusterEventService;
import io.atomix.core.map.AsyncAtomicMap;
import io.atomix.core.map.AtomicMapEvent;
import io.atomix.core.map.AtomicMapEventListener;
import io.atomix.core.map.AtomicMapType;
import io.atomix.core.map.impl.MapProxy;
import io.atomix.core.map.impl.MapService;
import io.atomix.core.map.impl.PartitionedAsyncAtomicMap;
import io.atomix.core.map.impl.RawAsyncAtomicMap;
import io.atomix.core.map.impl.TranscodingAsyncAtomicMap;
import io.atomix.primitive.PrimitiveCache;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.PrimitiveTypeRegistry;
import io.atomix.primitive.partition.PartitionGroupTypeRegistry;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PartitionMetadataEvent;
import io.atomix.primitive.partition.PartitionMetadataService;
import io.atomix.primitive.partition.PartitionService;
import io.atomix.primitive.partition.Partitioner;
import io.atomix.primitive.partition.SystemPartitionService;
import io.atomix.primitive.protocol.PrimitiveProtocolTypeRegistry;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.primitive.session.SessionIdService;
import io.atomix.primitive.session.impl.PrimitiveSessionIdManager;
import io.atomix.primitive.util.ByteArrayDecoder;
import io.atomix.primitive.util.ByteArrayEncoder;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Dependency;
import io.atomix.utils.component.Managed;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.concurrent.ThreadContextFactory;
import io.atomix.utils.concurrent.ThreadService;
import org.apache.commons.lang3.tuple.Pair;

import static io.atomix.primitive.util.ByteArrayDecoder.decode;
import static io.atomix.primitive.util.ByteArrayEncoder.encode;

/**
 * Partition metadata manager.
 */
@Component
public class PartitionMetadataManager implements PartitionMetadataService, Managed {
  @Dependency
  private SystemPartitionService systemPartitionService;
  @Dependency
  private ClusterMembershipService membershipService;
  @Dependency
  private PrimitiveSessionIdManager sessionIdManager;
  @Dependency
  private ThreadService threadService;

  private final Map<Consumer<PartitionMetadataEvent>, AtomicMapEventListener<String, byte[]>> listeners = new ConcurrentHashMap<>();
  private volatile AsyncAtomicMap<String, byte[]> metadatas;
  private volatile ThreadContext threadContext;

  @Override
  public <T> CompletableFuture<T> update(String partitionGroup, ByteArrayDecoder<T> decoder, Function<T, T> function, ByteArrayEncoder<T> encoder) {
    CompletableFuture<T> future = new CompletableFuture<>();
    update(partitionGroup, decoder, function, encoder, future);
    return future;
  }

  private <T> void update(String partitionGroup, ByteArrayDecoder<T> decoder, Function<T, T> function, ByteArrayEncoder<T> encoder, CompletableFuture<T> future) {
    metadatas.get(partitionGroup)
        .whenComplete((getResult, getError) -> {
          if (getError == null) {
            T metadata = function.apply(getResult != null ? decode(getResult.value(), decoder) : null);
            if (getResult != null && metadata == null) {
              metadatas.remove(partitionGroup, getResult.version())
                  .whenComplete((removeResult, removeError) -> {
                    if (removeError == null) {
                      if (removeResult) {
                        future.complete(null);
                      } else {
                        threadContext.schedule(Duration.ofMillis(10), () -> update(partitionGroup, decoder, function, encoder, future));
                      }
                    } else {
                      future.completeExceptionally(removeError);
                    }
                  });
            } else if (getResult == null && metadata != null) {
              metadatas.putIfAbsent(partitionGroup, encode(metadata, encoder))
                  .whenComplete((putResult, removeError) -> {
                    if (removeError == null) {
                      if (putResult == null) {
                        future.complete(metadata);
                      } else {
                        threadContext.schedule(Duration.ofMillis(10), () -> update(partitionGroup, decoder, function, encoder, future));
                      }
                    } else {
                      future.completeExceptionally(removeError);
                    }
                  });
            } else if (getResult != null && metadata != null) {
              T existingMetadata = decode(getResult.value(), decoder);
              if (!existingMetadata.equals(metadata)) {
                metadatas.replace(partitionGroup, getResult.version(), encode(metadata, encoder))
                    .whenComplete((putResult, removeError) -> {
                      if (removeError == null) {
                        if (putResult) {
                          future.complete(metadata);
                        } else {
                          threadContext.schedule(Duration.ofMillis(10), () -> update(partitionGroup, decoder, function, encoder, future));
                        }
                      } else {
                        future.completeExceptionally(removeError);
                      }
                    });
              } else {
                future.complete(existingMetadata);
              }
            } else {
              future.complete(null);
            }
          } else {
            future.completeExceptionally(getError);
          }
        });
  }

  @Override
  public <T> CompletableFuture<T> get(String partitionGroup, ByteArrayDecoder<T> decoder) {
    return metadatas.get(partitionGroup)
        .thenApply(v -> v != null ? decode(v.value(), decoder) : null);
  }

  @Override
  public CompletableFuture<Boolean> delete(String partitionGroup) {
    return metadatas.remove(partitionGroup)
        .thenApply(v -> v != null);
  }

  @Override
  public <T> CompletableFuture<Void> addListener(String partitionGroup, ByteArrayDecoder<T> decoder, Consumer<PartitionMetadataEvent<T>> listener) {
    return addPredicatedListener(
        event -> event.key().equals(partitionGroup),
        bytes -> decode(bytes, decoder),
        listener);
  }

  @Override
  public <T> CompletableFuture<Void> removeListener(String partitionGroup, Consumer<PartitionMetadataEvent<T>> listener) {
    return removePredicatedListener(listener);
  }

  @Override
  public CompletableFuture<Void> addListener(Consumer<PartitionMetadataEvent> listener) {
    return addPredicatedListener(
        event -> true,
        bytes -> bytes,
        listener);
  }

  @Override
  public CompletableFuture<Void> removeListener(Consumer<PartitionMetadataEvent> listener) {
    return removePredicatedListener(listener);
  }

  @SuppressWarnings("unchecked")
  private synchronized <T> CompletableFuture<Void> addPredicatedListener(
      Predicate<AtomicMapEvent<String, byte[]>> predicate,
      Function<byte[], T> decoder,
      Consumer listener) {
    AtomicMapEventListener<String, byte[]> mapListener = event -> {
      if (predicate.test(event)) {
        switch (event.type()) {
          case INSERTED:
            listener.accept(new PartitionMetadataEvent<>(
                PartitionMetadataEvent.Type.CREATE,
                event.key(),
                decoder.apply(event.newValue().value())));
            break;
          case UPDATED:
            listener.accept(new PartitionMetadataEvent<>(
                PartitionMetadataEvent.Type.UPDATE,
                event.key(),
                decoder.apply(event.newValue().value())));
            break;
          case REMOVED:
            listener.accept(new PartitionMetadataEvent<>(
                PartitionMetadataEvent.Type.DELETE,
                event.key(),
                decoder.apply(event.oldValue().value())));
            break;
        }
      }
    };
    listeners.put(listener, mapListener);
    return metadatas.addListener(mapListener);
  }

  private synchronized CompletableFuture<Void> removePredicatedListener(Consumer listener) {
    AtomicMapEventListener<String, byte[]> mapListener = listeners.remove(listener);
    if (mapListener != null) {
      return metadatas.removeListener(mapListener);
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Void> start() {
    threadContext = threadService.createContext();
    Map<PartitionId, RawAsyncAtomicMap> partitions = systemPartitionService.getSystemPartitionGroup()
        .getPartitions()
        .stream()
        .map(partition -> {
          MapProxy proxy = new MapProxy(new PrimitiveProxy.Context(
              "atomix-partition-metadata", MapService.TYPE, partition, threadService.getFactory()));
          return Pair.of(partition.id(), new RawAsyncAtomicMap(proxy, Duration.ofSeconds(30), new PartialPrimitiveManagementService()));
        }).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    return Futures.allOf(partitions.values().stream().map(RawAsyncAtomicMap::connect))
        .thenApply(v -> new TranscodingAsyncAtomicMap<String, byte[], String, byte[]>(
            new PartitionedAsyncAtomicMap(
                "atomix-partition-metadata",
                AtomicMapType.instance(),
                (Map) partitions,
                Partitioner.MURMUR3),
            key -> key,
            string -> string,
            bytes -> bytes,
            bytes -> bytes))
        .thenAccept(metadata -> {
          this.metadatas = metadata;
        });
  }

  @Override
  public CompletableFuture<Void> stop() {
    return metadatas.close();
  }

  private class PartialPrimitiveManagementService implements PrimitiveManagementService {
    @Override
    public ClusterMembershipService getMembershipService() {
      return membershipService;
    }

    @Override
    public ClusterCommunicationService getCommunicationService() {
      throw new UnsupportedOperationException();
    }

    @Override
    public ClusterEventService getEventService() {
      throw new UnsupportedOperationException();
    }

    @Override
    public PartitionService getPartitionService() {
      throw new UnsupportedOperationException();
    }

    @Override
    public PrimitiveCache getPrimitiveCache() {
      throw new UnsupportedOperationException();
    }

    @Override
    public PrimitiveRegistry getPrimitiveRegistry() {
      throw new UnsupportedOperationException();
    }

    @Override
    public PrimitiveTypeRegistry getPrimitiveTypeRegistry() {
      throw new UnsupportedOperationException();
    }

    @Override
    public PrimitiveProtocolTypeRegistry getProtocolTypeRegistry() {
      throw new UnsupportedOperationException();
    }

    @Override
    public PartitionGroupTypeRegistry getPartitionGroupTypeRegistry() {
      throw new UnsupportedOperationException();
    }

    @Override
    public SessionIdService getSessionIdService() {
      return sessionIdManager;
    }

    @Override
    public ThreadContextFactory getThreadFactory() {
      return threadService.getFactory();
    }
  }
}
