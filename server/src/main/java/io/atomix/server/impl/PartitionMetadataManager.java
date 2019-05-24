package io.atomix.server.impl;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import com.google.protobuf.ByteString;
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.grpc.ChannelService;
import io.atomix.primitive.partition.PartitionClient;
import io.atomix.primitive.partition.PartitionMetadataEvent;
import io.atomix.primitive.partition.PartitionMetadataService;
import io.atomix.primitive.partition.SystemPartitionService;
import io.atomix.primitive.service.impl.ServiceId;
import io.atomix.primitive.session.impl.CloseSessionRequest;
import io.atomix.primitive.session.impl.DefaultSessionClient;
import io.atomix.primitive.session.impl.OpenSessionRequest;
import io.atomix.primitive.session.impl.SessionCommandContext;
import io.atomix.primitive.session.impl.SessionQueryContext;
import io.atomix.primitive.session.impl.SessionStreamContext;
import io.atomix.primitive.util.ByteArrayDecoder;
import io.atomix.primitive.util.ByteArrayEncoder;
import io.atomix.server.map.GetRequest;
import io.atomix.server.map.ListenRequest;
import io.atomix.server.map.ListenResponse;
import io.atomix.server.map.MapProxy;
import io.atomix.server.map.MapService;
import io.atomix.server.map.PutRequest;
import io.atomix.server.map.RemoveRequest;
import io.atomix.server.map.ReplaceRequest;
import io.atomix.server.map.UpdateStatus;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Dependency;
import io.atomix.utils.component.Managed;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.concurrent.ThreadService;
import io.atomix.utils.stream.StreamHandler;
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
  private ChannelService channelService;
  @Dependency
  private ThreadService threadService;

  private volatile long sessionId;
  private volatile MapProxy map;
  private volatile ThreadContext threadContext;

  @Override
  public <T> CompletableFuture<T> update(String partitionGroup, ByteArrayDecoder<T> decoder, Function<T, T> function, ByteArrayEncoder<T> encoder) {
    CompletableFuture<T> future = new CompletableFuture<>();
    update(partitionGroup, decoder, function, encoder, future);
    return future;
  }

  private <T> void update(String partitionGroup, ByteArrayDecoder<T> decoder, Function<T, T> function, ByteArrayEncoder<T> encoder, CompletableFuture<T> future) {
    map.get(
        SessionQueryContext.newBuilder()
            .setSessionId(sessionId)
            .build(),
        GetRequest.newBuilder()
            .setKey(partitionGroup)
            .build())
        .whenComplete((getResult, getError) -> {
          if (getError == null) {
            T metadata = function.apply(getResult != null ? decode(getResult.getRight().getValue().toByteArray(), decoder) : null);
            if (getResult.getRight().getVersion() != 0 && metadata == null) {
              map.remove(
                  SessionCommandContext.newBuilder()
                      .setSessionId(sessionId)
                      .build(),
                  RemoveRequest.newBuilder()
                      .setKey(partitionGroup)
                      .setVersion(getResult.getRight().getVersion())
                      .build())
                  .whenComplete((removeResult, removeError) -> {
                    if (removeError == null) {
                      if (removeResult.getRight().getStatus() == UpdateStatus.OK) {
                        future.complete(null);
                      } else {
                        threadContext.schedule(Duration.ofMillis(10), () -> update(partitionGroup, decoder, function, encoder, future));
                      }
                    } else {
                      future.completeExceptionally(removeError);
                    }
                  });
            } else if (getResult.getRight().getVersion() == 0 && metadata != null) {
              map.put(
                  SessionCommandContext.newBuilder()
                      .setSessionId(sessionId)
                      .build(),
                  PutRequest.newBuilder()
                      .setKey(partitionGroup)
                      .setValue(ByteString.copyFrom(encode(metadata, encoder)))
                      .setVersion(-1)
                      .build())
                  .whenComplete((putResult, removeError) -> {
                    if (removeError == null) {
                      if (putResult.getRight().getStatus() == UpdateStatus.OK) {
                        future.complete(metadata);
                      } else {
                        threadContext.schedule(Duration.ofMillis(10), () -> update(partitionGroup, decoder, function, encoder, future));
                      }
                    } else {
                      future.completeExceptionally(removeError);
                    }
                  });
            } else if (getResult.getRight().getVersion() != 0 && metadata != null) {
              T existingMetadata = decode(getResult.getRight().getValue().toByteArray(), decoder);
              if (!existingMetadata.equals(metadata)) {
                map.replace(
                    SessionCommandContext.newBuilder()
                        .setSessionId(sessionId)
                        .build(),
                    ReplaceRequest.newBuilder()
                        .setKey(partitionGroup)
                        .setNewValue(ByteString.copyFrom(encode(metadata, encoder)))
                        .setPreviousVersion(getResult.getRight().getVersion())
                        .build())
                    .whenComplete((putResult, removeError) -> {
                      if (removeError == null) {
                        if (putResult.getRight().getStatus() == UpdateStatus.OK) {
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
    return map.get(
        SessionQueryContext.newBuilder()
            .setSessionId(sessionId)
            .build(),
        GetRequest.newBuilder()
            .setKey(partitionGroup)
            .build())
        .thenApply(v -> v.getRight().getVersion() != 0 ? decode(v.getRight().getValue().toByteArray(), decoder) : null);
  }

  @Override
  public CompletableFuture<Boolean> delete(String partitionGroup) {
    return map.remove(
        SessionCommandContext.newBuilder()
            .setSessionId(sessionId)
            .build(),
        RemoveRequest.newBuilder()
            .setKey(partitionGroup)
            .build())
        .thenApply(v -> v.getRight().getStatus() == UpdateStatus.OK);
  }

  @Override
  public <T> CompletableFuture<Void> addListener(String partitionGroup, ByteArrayDecoder<T> decoder, Consumer<PartitionMetadataEvent<T>> listener) {
    return addPredicatedListener(
        event -> event.getKey().equals(partitionGroup),
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
      Predicate<ListenResponse> predicate,
      Function<byte[], T> decoder,
      Consumer listener) {
    return map.listen(
        SessionCommandContext.newBuilder()
            .setSessionId(sessionId)
            .build(),
        ListenRequest.newBuilder().build(),
        new StreamHandler<Pair<SessionStreamContext, ListenResponse>>() {
          @Override
          public void next(Pair<SessionStreamContext, ListenResponse> event) {
            if (predicate.test(event.getRight())) {
              switch (event.getRight().getType()) {
                case INSERTED:
                  listener.accept(new PartitionMetadataEvent<>(
                      PartitionMetadataEvent.Type.CREATE,
                      event.getRight().getKey(),
                      decoder.apply(event.getRight().getNewValue().toByteArray())));
                  break;
                case UPDATED:
                  listener.accept(new PartitionMetadataEvent<>(
                      PartitionMetadataEvent.Type.UPDATE,
                      event.getRight().getKey(),
                      decoder.apply(event.getRight().getNewValue().toByteArray())));
                  break;
                case REMOVED:
                  listener.accept(new PartitionMetadataEvent<>(
                      PartitionMetadataEvent.Type.DELETE,
                      event.getRight().getKey(),
                      decoder.apply(event.getRight().getOldValue().toByteArray())));
                  break;
              }
            }
          }

          @Override
          public void complete() {

          }

          @Override
          public void error(Throwable error) {

          }
        }).thenApply(response -> null);
  }

  private synchronized CompletableFuture<Void> removePredicatedListener(Consumer listener) {
    // TODO: Implement support for adding and removing listeners
    return CompletableFuture.completedFuture(null);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Void> start() {
    threadContext = threadService.createContext();
    ServiceId serviceId = ServiceId.newBuilder()
        .setType(MapService.TYPE.name())
        .setName("atomix-partition-metadata")
        .build();
    PartitionClient client = systemPartitionService.getSystemPartitionGroup().getPartitions().iterator().next().getClient();
    map = new MapProxy(new DefaultSessionClient(serviceId, client));
    return map.openSession(OpenSessionRequest.newBuilder()
        .setTimeout(Duration.ofSeconds(30).toMillis())
        .build())
        .thenAccept(response -> {
          sessionId = response.getSessionId();
        });
  }

  @Override
  public CompletableFuture<Void> stop() {
    return map.closeSession(CloseSessionRequest.newBuilder()
        .setSessionId(sessionId)
        .build())
        .thenApply(response -> null);
  }
}
