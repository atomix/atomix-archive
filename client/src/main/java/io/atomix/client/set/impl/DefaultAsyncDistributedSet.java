package io.atomix.client.set.impl;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import io.atomix.api.headers.Name;
import io.atomix.api.set.AddRequest;
import io.atomix.api.set.AddResponse;
import io.atomix.api.set.ClearRequest;
import io.atomix.api.set.ClearResponse;
import io.atomix.api.set.CloseRequest;
import io.atomix.api.set.CloseResponse;
import io.atomix.api.set.ContainsRequest;
import io.atomix.api.set.ContainsResponse;
import io.atomix.api.set.CreateRequest;
import io.atomix.api.set.CreateResponse;
import io.atomix.api.set.EventRequest;
import io.atomix.api.set.EventResponse;
import io.atomix.api.set.IterateRequest;
import io.atomix.api.set.IterateResponse;
import io.atomix.api.set.KeepAliveRequest;
import io.atomix.api.set.KeepAliveResponse;
import io.atomix.api.set.RemoveRequest;
import io.atomix.api.set.RemoveResponse;
import io.atomix.api.set.SetServiceGrpc;
import io.atomix.api.set.SizeRequest;
import io.atomix.api.set.SizeResponse;
import io.atomix.client.collection.CollectionEvent;
import io.atomix.client.collection.CollectionEventListener;
import io.atomix.client.impl.AbstractManagedPrimitive;
import io.atomix.client.impl.TranscodingStreamObserver;
import io.atomix.client.iterator.AsyncIterator;
import io.atomix.client.iterator.impl.StreamObserverIterator;
import io.atomix.client.partition.Partition;
import io.atomix.client.set.AsyncDistributedSet;
import io.atomix.client.set.DistributedSet;
import io.atomix.client.utils.concurrent.Futures;
import io.atomix.client.utils.concurrent.ThreadContext;
import io.grpc.stub.StreamObserver;

/**
 * Default distributed set primitive.
 */
public class DefaultAsyncDistributedSet
    extends AbstractManagedPrimitive<SetServiceGrpc.SetServiceStub, AsyncDistributedSet<String>>
    implements AsyncDistributedSet<String> {
  private volatile CompletableFuture<Long> listenFuture;
  private final Map<CollectionEventListener<String>, Executor> eventListeners = new ConcurrentHashMap<>();

  public DefaultAsyncDistributedSet(Name name, Partition partition, ThreadContext context, Duration timeout) {
    super(name, SetServiceGrpc.newStub(partition.getChannelFactory().getChannel()), context, timeout);
  }

  @Override
  public CompletableFuture<Boolean> add(String element) {
    return addAll(Collections.singleton(element));
  }

  @Override
  public CompletableFuture<Boolean> remove(String element) {
    return removeAll(Collections.singleton(element));
  }

  @Override
  public CompletableFuture<Integer> size() {
    return query(
        (header, observer) -> getService().size(SizeRequest.newBuilder()
            .setHeader(header)
            .build(), observer),
        SizeResponse::getHeader)
        .thenApply(response -> response.getSize());
  }

  @Override
  public CompletableFuture<Boolean> isEmpty() {
    return size().thenApply(size -> size == 0);
  }

  @Override
  public CompletableFuture<Boolean> contains(String element) {
    return containsAll(Collections.singleton(element));
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Boolean> addAll(Collection<? extends String> c) {
    return command(
        (header, observer) -> getService().add(AddRequest.newBuilder()
            .setHeader(header)
            .addAllValues((Collection) c)
            .build(), observer),
        AddResponse::getHeader)
        .thenApply(response -> response.getAdded());
  }

  @Override
  public CompletableFuture<Boolean> containsAll(Collection<? extends String> c) {
    return query(
        (header, observer) -> getService().contains(ContainsRequest.newBuilder()
            .setHeader(header)
            .addAllValues((Collection) c)
            .build(), observer),
        ContainsResponse::getHeader)
        .thenApply(response -> response.getContains());
  }

  @Override
  public CompletableFuture<Boolean> retainAll(Collection<? extends String> c) {
    return Futures.exceptionalFuture(new UnsupportedOperationException());
  }

  @Override
  public CompletableFuture<Boolean> removeAll(Collection<? extends String> c) {
    return command(
        (header, observer) -> getService().remove(RemoveRequest.newBuilder()
            .setHeader(header)
            .addAllValues((Collection) c)
            .build(), observer),
        RemoveResponse::getHeader)
        .thenApply(response -> response.getRemoved());
  }

  @Override
  public CompletableFuture<Void> clear() {
    return command(
        (header, observer) -> getService().clear(ClearRequest.newBuilder()
            .setHeader(header)
            .build(), observer),
        ClearResponse::getHeader)
        .thenApply(response -> null);
  }

  private synchronized CompletableFuture<Void> listen() {
    if (listenFuture == null && !eventListeners.isEmpty()) {
      listenFuture = command(
          (header, observer) -> getService().listen(EventRequest.newBuilder()
              .setHeader(header)
              .build(), observer),
          EventResponse::getHeader,
          new StreamObserver<EventResponse>() {
            @Override
            public void onNext(EventResponse response) {
              CollectionEvent<String> event = null;
              switch (response.getType()) {
                case ADDED:
                  event = new CollectionEvent<>(
                      CollectionEvent.Type.ADDED,
                      response.getValue());
                  break;
                case REMOVED:
                  event = new CollectionEvent<>(
                      CollectionEvent.Type.REMOVED,
                      response.getValue());
                  break;
              }
              onEvent(event);
            }

            private void onEvent(CollectionEvent<String> event) {
              eventListeners.forEach((l, e) -> e.execute(() -> l.event(event)));
            }

            @Override
            public void onError(Throwable t) {
              onCompleted();
            }

            @Override
            public void onCompleted() {
              synchronized (DefaultAsyncDistributedSet.this) {
                listenFuture = null;
              }
              listen();
            }
          });
    }
    return listenFuture.thenApply(v -> null);
  }

  @Override
  public synchronized CompletableFuture<Void> addListener(CollectionEventListener<String> listener, Executor executor) {
    eventListeners.put(listener, executor);
    return listen();
  }

  @Override
  public synchronized CompletableFuture<Void> removeListener(CollectionEventListener<String> listener) {
    eventListeners.remove(listener);
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public AsyncIterator<String> iterator() {
    StreamObserverIterator<String> iterator = new StreamObserverIterator<>();
    query(
        (header, observer) -> getService().iterate(IterateRequest.newBuilder()
            .setHeader(header)
            .build(), observer),
        IterateResponse::getHeader,
        new TranscodingStreamObserver<>(
            iterator,
            IterateResponse::getValue));
    return iterator;
  }

  @Override
  protected CompletableFuture<Long> openSession(Duration timeout) {
    return this.<CreateResponse>session((header, observer) -> getService().create(CreateRequest.newBuilder()
        .setTimeout(com.google.protobuf.Duration.newBuilder()
            .setSeconds(timeout.getSeconds())
            .setNanos(timeout.getNano())
            .build())
        .build(), observer))
        .thenApply(response -> response.getHeader().getSessionId());
  }

  @Override
  protected CompletableFuture<Boolean> keepAlive() {
    return this.<KeepAliveResponse>session((header, observer) -> getService().keepAlive(KeepAliveRequest.newBuilder()
        .build(), observer))
        .thenApply(response -> true);
  }

  @Override
  protected CompletableFuture<Void> close(boolean delete) {
    return this.<CloseResponse>session((header, observer) -> getService().close(CloseRequest.newBuilder()
        .setDelete(delete)
        .build(), observer))
        .thenApply(v -> null);
  }

  @Override
  public DistributedSet<String> sync(Duration operationTimeout) {
    return new BlockingDistributedSet<>(this, operationTimeout.toMillis());
  }
}
