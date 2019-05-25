package io.atomix.client.impl;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.ListenableFuture;
import io.atomix.api.headers.RequestHeader;
import io.atomix.api.headers.ResponseHeader;
import io.atomix.api.headers.SessionCommandHeader;
import io.atomix.api.headers.SessionHeader;
import io.atomix.api.headers.SessionQueryHeader;
import io.atomix.api.headers.SessionResponseHeader;
import io.atomix.api.primitive.PrimitiveId;
import io.atomix.client.AsyncPrimitive;
import io.atomix.client.ManagedAsyncPrimitive;
import io.atomix.client.PrimitiveManagementService;
import io.atomix.utils.concurrent.ThreadContext;
import io.grpc.stub.StreamObserver;

/**
 * Primitive session.
 */
public abstract class AbstractAsyncPrimitive<P extends AsyncPrimitive> implements ManagedAsyncPrimitive<P> {
  private static final BiFunction<String, List<Integer>, Integer> DEFAULT_PARTITIONER = (key, partitions) -> {
    int hash = Math.abs(Hashing.murmur3_32().hashUnencodedChars(key).asInt());
    return partitions.get(Hashing.consistentHash(hash, partitions.size()));
  };

  private final PrimitiveId id;
  private final ThreadContext context;
  private final BiFunction<String, List<Integer>, Integer> partitioner;
  protected final Duration timeout;
  private final Map<Integer, PrimitivePartition> partitions = new ConcurrentHashMap<>();
  private final List<Integer> partitionIds = new CopyOnWriteArrayList<>();

  protected AbstractAsyncPrimitive(
      PrimitiveId id,
      PrimitiveManagementService managementService,
      Duration timeout) {
    this(id, managementService, DEFAULT_PARTITIONER, timeout);
  }

  protected AbstractAsyncPrimitive(
      PrimitiveId id,
      PrimitiveManagementService managementService,
      BiFunction<String, List<Integer>, Integer> partitioner,
      Duration timeout) {
    this.id = id;
    this.context = managementService.getThreadFactory().createContext();
    this.partitioner = partitioner;
    this.timeout = timeout;
  }

  /**
   * Returns the primitive ID.
   *
   * @return the primitive ID
   */
  protected PrimitiveId id() {
    return id;
  }

  /**
   * Returns the primitive thread context.
   *
   * @return the primitive thread context
   */
  protected ThreadContext context() {
    return context;
  }

  @Override
  public String name() {
    return id.getName();
  }

  /**
   * Returns a single partition.
   *
   * @return a single primitive partition
   */
  protected PrimitivePartition getPartition() {
    return partitions.values().iterator().next();
  }

  /**
   * Gets a partition by ID.
   *
   * @param partitionId the partition ID
   * @return the partition
   */
  protected PrimitivePartition getPartition(int partitionId) {
    return partitions.get(partitionId);
  }

  /**
   * Gets a partition by ID.
   *
   * @param key the partition key
   * @return the partition
   */
  protected PrimitivePartition getPartition(String key) {
    return getPartition(partitioner.apply(key, partitionIds));
  }

  /**
   * Gets or creates a partition by ID.
   *
   * @param partitionId the partition ID
   * @return the partition
   */
  protected PrimitivePartition getOrCreatePartition(int partitionId) {
    PrimitivePartition partition = partitions.get(partitionId);
    if (partition == null) {
      partition = partitions.computeIfAbsent(partitionId, PrimitivePartition::new);
    }
    return partition;
  }

  /**
   * Returns the collection of all active partitions.
   *
   * @return the collection of all active partitions
   */
  protected Collection<PrimitivePartition> getPartitions() {
    return partitions.values();
  }

  /**
   * Returns a collection of request headers for all partitions.
   *
   * @return a collection of request headers for all partitions
   */
  protected Collection<RequestHeader> getRequestHeaders() {
    return getPartitions().stream()
        .map(partition -> partition.getRequestHeader())
        .collect(Collectors.toList());
  }

  /**
   * Returns a collection of session headers for all partitions.
   *
   * @return a collection of session headers for all partitions
   */
  protected Collection<SessionHeader> getSessionHeaders() {
    return getPartitions().stream()
        .map(partition -> partition.getSessionHeader())
        .collect(Collectors.toList());
  }

  /**
   * Returns a collection of command headers for all partitions.
   *
   * @return a collection of command headers for all partitions
   */
  protected Collection<SessionCommandHeader> getCommandHeaders() {
    return getPartitions().stream()
        .map(partition -> partition.getCommandHeader())
        .collect(Collectors.toList());
  }

  /**
   * Returns a collection of query headers for all partitions.
   *
   * @return a collection of query headers for all partitions
   */
  protected Collection<SessionQueryHeader> getQueryHeaders() {
    return getPartitions().stream()
        .map(partition -> partition.getQueryHeader())
        .collect(Collectors.toList());
  }

  /**
   * Updates all response headers.
   *
   * @param headers the response headers
   */
  private void updateResponseHeaders(Collection<ResponseHeader> headers) {
    for (ResponseHeader header : headers) {
      getOrCreatePartition(header.getPartitionId()).update(header);
    }
  }

  /**
   * Updates all session headers.
   *
   * @param headers the response headers
   */
  private void updateSessionHeaders(Collection<SessionResponseHeader> headers) {
    for (SessionResponseHeader header : headers) {
      PrimitivePartition partition = getPartition(header.getPartitionId());
      if (partition != null) {
        partition.update(header);
      }
    }
  }

  /**
   * Completes the given response in progam order.
   *
   * @param response the response to complete
   * @param headers  the response headers
   * @param <T>      the response type
   * @return a future to be completed in program order
   */
  protected <T> CompletableFuture<T> order(T response, Collection<SessionResponseHeader> headers) {
    updateSessionHeaders(headers);
    return CompletableFuture.completedFuture(response);
  }

  /**
   * Starts the session.
   *
   * @param header the session header
   */
  protected void startKeepAlive(SessionHeader header) {
    startKeepAlive(Collections.singleton(header));
  }

  /**
   * Starts the session.
   *
   * @param headers the session headers
   */
  protected void startKeepAlive(Collection<SessionHeader> headers) {
    headers.forEach(header -> getOrCreatePartition(header.getPartitionId()).update(header));
    context.schedule(timeout.dividedBy(2), () -> keepAlive());
  }

  /**
   * Sends a keep-alive request.
   *
   * @return a future to be completed once the keep alive is complete
   */
  protected abstract CompletableFuture<Void> keepAlive();

  /**
   * Completes a keep alive request.
   *
   * @param header the session header
   */
  protected void completeKeepAlive(SessionHeader header) {
    completeKeepAlive(Collections.singleton(header));
  }

  /**
   * Completes a keep alive request.
   *
   * @param headers the session headers
   */
  protected void completeKeepAlive(Collection<SessionHeader> headers) {
  }

  /**
   * Executes the given callback, wrapping the response in a future.
   *
   * @param callback the callback to execute
   * @param <T>      the response type
   * @return the response future
   */
  protected <T> CompletableFuture<T> execute(Supplier<ListenableFuture<T>> callback) {
    CompletableFuture<T> future = new CompletableFuture<>();
    ListenableFuture<T> listenableFuture = callback.get();
    listenableFuture.addListener(() -> {
      try {
        future.complete(listenableFuture.get());
      } catch (InterruptedException e) {
        future.completeExceptionally(e);
      } catch (ExecutionException e) {
        future.completeExceptionally(e.getCause());
      }
    }, context);
    return future;
  }

  /**
   * Executes the given callback, providing a {@link StreamObserver} to be converted in the returned future.
   *
   * @param callback the callback to execute
   * @param <T>      the response type
   * @return the response future
   */
  protected <T> CompletableFuture<T> execute(Consumer<StreamObserver<T>> callback) {
    CompletableFuture<T> future = new CompletableFuture<>();
    callback.accept(new StreamObserver<T>() {
      @Override
      public void onNext(T response) {
        context.execute(() -> future.complete(response));
      }

      @Override
      public void onError(Throwable t) {
        context.execute(() -> future.completeExceptionally(t));
      }

      @Override
      public void onCompleted() {
        if (!future.isDone()) {
          future.complete(null);
        }
      }
    });
    return future;
  }
}
