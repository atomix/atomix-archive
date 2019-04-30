package io.atomix.grpc.impl;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.protobuf.Message;
import io.atomix.grpc.headers.RequestHeader;
import io.atomix.grpc.headers.RequestHeaders;
import io.atomix.grpc.headers.SessionCommandHeader;
import io.atomix.grpc.headers.SessionCommandHeaders;
import io.atomix.grpc.headers.SessionHeader;
import io.atomix.grpc.headers.SessionHeaders;
import io.atomix.grpc.headers.SessionQueryHeader;
import io.atomix.grpc.headers.SessionQueryHeaders;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.primitive.session.impl.SessionResponseContext;
import io.atomix.utils.QuadFunction;
import io.atomix.utils.TriFunction;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.stream.StreamHandler;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Primitive factory.
 */
public class RequestExecutor<P extends PrimitiveProxy, I extends Message, H extends Message, T extends Message, R extends Message> {
  private final PrimitiveFactory<P, I> primitiveFactory;
  private final RequestDescriptor<T, I, H> requestDescriptor;
  private final Supplier<R> responseSupplier;

  public RequestExecutor(
      PrimitiveFactory<P, I> primitiveFactory,
      RequestDescriptor<T, I, H> requestDescriptor,
      Supplier<R> responseSupplier) {
    this.primitiveFactory = primitiveFactory;
    this.requestDescriptor = requestDescriptor;
    this.responseSupplier = responseSupplier;
  }

  /**
   * Returns the primitive ID for the given request.
   *
   * @param request the request for which to return the primitive ID
   * @return the primitive ID for the given request
   */
  private I getId(T request) {
    return requestDescriptor.getId(request);
  }

  /**
   * Returns the default header for the given partition.
   *
   * @param partitionId the partition ID
   * @return the default header for the given partition
   */
  private H getDefaultHeader(int partitionId) {
    return requestDescriptor.getDefaultHeader(partitionId);
  }

  /**
   * Returns the headers for the given request.
   *
   * @param request the request for which to return headers
   * @return the headers for the given request
   */
  private Collection<H> getHeaders(T request) {
    return requestDescriptor.getHeaders(request);
  }

  /**
   * Returns the partition ID for the given header.
   *
   * @param header the header for which to return the partition ID
   * @return the partition ID for the given header
   */
  private int getPartitionId(H header) {
    return requestDescriptor.getPartitionId(header);
  }

  /**
   * Executes the given function on the primitive.
   *
   * @param request          the request
   * @param responseObserver the response observer
   * @param function         the function to execute
   */
  public void execute(
      T request,
      StreamObserver<R> responseObserver,
      Function<P, CompletableFuture<R>> function) {
    I id = getId(request);
    if (isValidId(id, responseObserver)) {
      function.apply(primitiveFactory.getPrimitive(id)).whenComplete((result, funcError) -> {
        if (funcError == null) {
          responseObserver.onNext(result);
          responseObserver.onCompleted();
        } else {
          responseObserver.onError(funcError);
        }
      });
    }
  }

  /**
   * Creates a new session ID and applies it to the given function.
   *
   * @param request          the request
   * @param key              the request key
   * @param responseObserver the response observer
   * @param function         the function to which to apply the session ID
   */
  public void createBy(
      T request,
      String key,
      StreamObserver<R> responseObserver,
      TriFunction<PartitionId, Long, P, CompletableFuture<R>> function) {
    I id = getId(request);
    if (isValidId(id, responseObserver)) {
      primitiveFactory.createSession().whenComplete((sessionId, sessionError) -> {
        if (sessionError == null) {
          PartitionId partitionId = primitiveFactory.getPartitionId(id, key);
          function.apply(partitionId, sessionId, primitiveFactory.getPrimitive(primitiveFactory.getPrimitiveName(id), partitionId)).whenComplete((result, funcError) -> {
            if (funcError == null) {
              responseObserver.onNext(result);
              responseObserver.onCompleted();
            } else {
              responseObserver.onError(funcError);
            }
          });
        } else {
          responseObserver.onError(sessionError);
        }
      });
    }
  }

  /**
   * Creates a new session ID and applies it to the given function.
   *
   * @param request          the request
   * @param responseObserver the response observer
   * @param function         the function to which to apply the session ID
   * @param responseFunction the response function
   */
  public <V> void createAll(
      T request,
      StreamObserver<R> responseObserver,
      TriFunction<PartitionId, Long, P, CompletableFuture<V>> function,
      BiFunction<Long, Stream<V>, R> responseFunction) {
    I id = getId(request);
    if (isValidId(id, responseObserver)) {
      primitiveFactory.createSession().whenComplete((sessionId, sessionError) -> {
        if (sessionError == null) {
          final String name = primitiveFactory.getPrimitiveName(id);
          Futures.allOf(primitiveFactory.getPartitionIds(id).stream()
              .map(partitionId -> function.apply(partitionId, sessionId, primitiveFactory.getPrimitive(name, partitionId))))
              .thenApply(results -> responseFunction.apply(sessionId, results))
              .whenComplete((result, funcError) -> {
                if (funcError == null) {
                  responseObserver.onNext(result);
                  responseObserver.onCompleted();
                } else {
                  responseObserver.onError(funcError);
                }
              });
        } else {
          responseObserver.onError(sessionError);
        }
      });
    }
  }

  /**
   * Executes the given function on all partitions and aggregates the responses.
   *
   * @param request          the request
   * @param responseObserver the response observer
   * @param function         the function to execute
   * @param aggregator       a function with which to aggregate responses
   */
  public <V> void executeAll(
      T request,
      StreamObserver<R> responseObserver,
      TriFunction<PartitionId, H, P, CompletableFuture<V>> function,
      Function<Stream<V>, R> aggregator) {
    I id = getId(request);
    if (isValidId(id, responseObserver)) {
      final String group = primitiveFactory.getPartitionGroup(id);
      final String name = primitiveFactory.getPrimitiveName(id);
      Map<PartitionId, H> headerMap = getHeaders(request).stream()
          .map(header -> Pair.of(PartitionId.newBuilder()
              .setGroup(group)
              .setPartition(getPartitionId(header))
              .build(), header))
          .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
      Futures.allOf(primitiveFactory.getPartitionIds(id).stream()
          .map(partitionId -> {
            H header = headerMap.computeIfAbsent(partitionId, i -> getDefaultHeader(i.getPartition()));
            return function.apply(partitionId, header, primitiveFactory.getPrimitive(name, partitionId));
          }))
          .thenApply(aggregator)
          .whenComplete((result, funcError) -> {
            if (funcError == null) {
              responseObserver.onNext(result);
              responseObserver.onCompleted();
            } else {
              responseObserver.onError(funcError);
            }
          });
    }
  }

  /**
   * Executes the given streaming function on all partitions.
   *
   * @param request          the request
   * @param responseObserver the response observer
   * @param function         the function to execute
   * @param converter        the converter to apply to stream events
   */
  public <V> void executeAll(
      T request,
      StreamObserver<R> responseObserver,
      TriFunction<H, StreamHandler<V>, P, CompletableFuture<SessionResponseContext>> function,
      BiFunction<H, V, R> converter) {
    I id = getId(request);
    if (isValidId(id, responseObserver)) {
      final String group = primitiveFactory.getPartitionGroup(id);
      final String name = primitiveFactory.getPrimitiveName(id);
      Map<PartitionId, H> headerMap = getHeaders(request).stream()
          .map(header -> Pair.of(PartitionId.newBuilder()
              .setGroup(group)
              .setPartition(getPartitionId(header))
              .build(), header))
          .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
      Futures.allOf(primitiveFactory.getPartitionIds(id).stream()
          .map(partitionId -> {
            H header = headerMap.computeIfAbsent(partitionId, i -> getDefaultHeader(i.getPartition()));
            return function.apply(header, new StreamHandler<V>() {
              @Override
              public void next(V value) {
                responseObserver.onNext(converter.apply(header, value));
              }

              @Override
              public void complete() {
                responseObserver.onCompleted();
              }

              @Override
              public void error(Throwable error) {
                responseObserver.onError(error);
              }
            }, primitiveFactory.getPrimitive(name, partitionId));
          }))
          .whenComplete((result, funcError) -> {
            if (funcError != null) {
              responseObserver.onError(funcError);
            }
          });
    }
  }

  /**
   * Executes the given streaming function on all partitions.
   *
   * @param request          the request
   * @param responseObserver the response observer
   * @param function         the function to execute
   */
  public void executeAll(
      T request,
      StreamObserver<R> responseObserver,
      TriFunction<PartitionId, H, P, CompletableFuture<R>> function) {
    I id = getId(request);
    if (isValidId(id, responseObserver)) {
      final String group = primitiveFactory.getPartitionGroup(id);
      final String name = primitiveFactory.getPrimitiveName(id);
      Map<PartitionId, H> headerMap = getHeaders(request).stream()
          .map(header -> Pair.of(PartitionId.newBuilder()
              .setGroup(group)
              .setPartition(getPartitionId(header))
              .build(), header))
          .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
      Futures.allOf(primitiveFactory.getPartitionIds(id).stream()
          .map(partitionId -> {
            H header = headerMap.computeIfAbsent(partitionId, i -> getDefaultHeader(i.getPartition()));
            return function.apply(partitionId, header, primitiveFactory.getPrimitive(name, partitionId));
          }))
          .whenComplete((result, funcError) -> {
            if (funcError != null) {
              responseObserver.onError(funcError);
            }
          });
    }
  }

  /**
   * Executes the given function on the partition owning the given key.
   *
   * @param request          the request
   * @param key              the key with which to partition the request
   * @param responseObserver the response observer
   * @param function         the function to execute
   */
  public void executeBy(
      T request,
      String key,
      StreamObserver<R> responseObserver,
      TriFunction<PartitionId, H, P, CompletableFuture<R>> function) {
    I id = getId(request);
    if (isValidId(id, responseObserver)) {
      PartitionId partitionId = primitiveFactory.getPartitionId(id, key);
      H header = getHeaders(request).stream()
          .filter(h -> getPartitionId(h) == partitionId.getPartition())
          .findFirst()
          .orElseGet(() -> getDefaultHeader(partitionId.getPartition()));
      function.apply(partitionId, header, primitiveFactory.getPrimitive(primitiveFactory.getPrimitiveName(id), partitionId)).whenComplete((result, funcError) -> {
        if (funcError == null) {
          responseObserver.onNext(result);
          responseObserver.onCompleted();
        } else {
          responseObserver.onError(funcError);
        }
      });
    }
  }

  /**
   * Executes the given function on the partition owning the given key.
   *
   * @param request          the request
   * @param key              the key with which to partition the request
   * @param responseObserver the response observer
   * @param function         the function to execute
   */
  public <V> void executeBy(
      T request,
      String key,
      StreamObserver<R> responseObserver,
      QuadFunction<PartitionId, H, StreamHandler<V>, P, CompletableFuture<SessionResponseContext>> function,
      TriFunction<PartitionId, H, V, R> converter) {
    I id = getId(request);
    if (isValidId(id, responseObserver)) {
      PartitionId partitionId = primitiveFactory.getPartitionId(id, key);
      H header = getHeaders(request).stream()
          .filter(h -> getPartitionId(h) == partitionId.getPartition())
          .findFirst()
          .orElseGet(() -> getDefaultHeader(partitionId.getPartition()));
      P primitive = primitiveFactory.getPrimitive(primitiveFactory.getPrimitiveName(id), partitionId);
      StreamHandler<V> handler = new StreamHandler<V>() {
        @Override
        public void next(V value) {
          responseObserver.onNext(converter.apply(partitionId, header, value));
        }

        @Override
        public void complete() {
          responseObserver.onCompleted();
        }

        @Override
        public void error(Throwable error) {
          responseObserver.onError(error);
        }
      };
      function.apply(partitionId, header, handler, primitive).whenComplete((result, funcError) -> {
        if (funcError != null) {
          responseObserver.onError(funcError);
        }
      });
    }
  }

  /**
   * Executes the given function on the partitions owning the given keys and aggregates the responses.
   *
   * @param request          the request
   * @param keys             the keys with which to partition the request
   * @param responseObserver the response observer
   * @param function         the function to execute
   * @param aggregator       a function with which to aggregate responses
   */
  public <V> void executeBy(
      T request,
      Collection<String> keys,
      StreamObserver<R> responseObserver,
      TriFunction<H, Collection<String>, P, CompletableFuture<V>> function,
      Function<Stream<V>, R> aggregator) {
    I id = getId(request);
    if (isValidId(id, responseObserver)) {
      final String group = primitiveFactory.getPartitionGroup(id);
      final String name = primitiveFactory.getPrimitiveName(id);
      Map<PartitionId, Collection<String>> partitionMap = primitiveFactory.getPartitionIds(id, keys);
      Map<PartitionId, H> headerMap = getHeaders(request).stream()
          .map(header -> Pair.of(PartitionId.newBuilder()
              .setGroup(group)
              .setPartition(getPartitionId(header))
              .build(), header))
          .filter(pair -> partitionMap.containsKey(pair.getKey()))
          .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
      Futures.allOf(partitionMap.entrySet().stream()
          .map(e -> {
            PartitionId partitionId = e.getKey();
            Collection<String> partitionKeys = e.getValue();
            H partitionHeader = headerMap.computeIfAbsent(partitionId, i -> getDefaultHeader(i.getPartition()));
            return function.apply(partitionHeader, partitionKeys, primitiveFactory.getPrimitive(name, partitionId));
          }))
          .thenApply(aggregator)
          .whenComplete((result, funcError) -> {
            if (funcError == null) {
              responseObserver.onNext(result);
              responseObserver.onCompleted();
            } else {
              responseObserver.onError(funcError);
            }
          });
    }
  }

  /**
   * Validates the given ID.
   *
   * @param id               the primitive ID
   * @param responseObserver the response observer
   * @return indicates whether the ID is valid
   */
  private boolean isValidId(I id, StreamObserver<R> responseObserver) {
    if (!primitiveFactory.hasPrimitiveName(id)) {
      fail(Status.INVALID_ARGUMENT, "Primitive name not specified", responseObserver);
      return false;
    }
    if (!primitiveFactory.hasProtocol(id)) {
      fail(Status.INVALID_ARGUMENT, "Primitive protocol not specified", responseObserver);
      return false;
    }
    return true;
  }

  /**
   * Sends a failure response to the given observer.
   *
   * @param status           the response status
   * @param message          the failure message
   * @param responseObserver the response observer on which to send the error
   */
  private void fail(Status status, String message, StreamObserver<R> responseObserver) {
    R response = responseSupplier.get();
    Metadata.Key<R> key = ProtoUtils.keyForProto(response);
    Metadata metadata = new Metadata();
    metadata.put(key, response);
    responseObserver.onError(status.withDescription(message)
        .asRuntimeException(metadata));
  }

  /**
   * Request descriptor.
   */
  public interface RequestDescriptor<T extends Message, I extends Message, H extends Message> {

    /**
     * Returns the request ID for the given request.
     *
     * @param request the request for which to return the ID
     * @return the ID for the given request
     */
    I getId(T request);

    /**
     * Returns the headers for the given request.
     *
     * @param request the request for which to return the headers
     * @return the headers for the given request
     */
    Collection<H> getHeaders(T request);

    /**
     * Returns the partition ID for the given header.
     *
     * @param header the header for which to return the partition ID
     * @return the partition ID for the given header
     */
    int getPartitionId(H header);

    /**
     * Returns the default header for the given partition.
     *
     * @param partitionId the partition ID
     * @return the default header for the given partition
     */
    H getDefaultHeader(int partitionId);
  }

  static class BasicDescriptor<T extends Message, I extends Message> implements RequestExecutor.RequestDescriptor<T, I, RequestHeader> {
    private final Function<T, I> idGetter;
    private final Function<T, RequestHeaders> headerGetter;

    BasicDescriptor(Function<T, I> idGetter, Function<T, RequestHeaders> headerGetter) {
      this.idGetter = idGetter;
      this.headerGetter = headerGetter;
    }

    @Override
    public I getId(T request) {
      return idGetter.apply(request);
    }

    @Override
    public Collection<RequestHeader> getHeaders(T request) {
      return headerGetter.apply(request).getHeadersList();
    }

    @Override
    public int getPartitionId(RequestHeader header) {
      return header.getPartitionId();
    }

    @Override
    public RequestHeader getDefaultHeader(int partitionId) {
      return RequestHeader.newBuilder()
          .setPartitionId(partitionId)
          .build();
    }
  }

  static class SessionDescriptor<T extends Message, I extends Message> implements RequestExecutor.RequestDescriptor<T, I, SessionHeader> {
    private final Function<T, I> idGetter;
    private final Function<T, SessionHeaders> headerGetter;

    SessionDescriptor(Function<T, I> idGetter, Function<T, SessionHeaders> headerGetter) {
      this.idGetter = idGetter;
      this.headerGetter = headerGetter;
    }

    @Override
    public I getId(T request) {
      return idGetter.apply(request);
    }

    @Override
    public Collection<SessionHeader> getHeaders(T request) {
      return headerGetter.apply(request).getHeadersList();
    }

    @Override
    public int getPartitionId(SessionHeader header) {
      return header.getPartitionId();
    }

    @Override
    public SessionHeader getDefaultHeader(int partitionId) {
      return SessionHeader.newBuilder()
          .setPartitionId(partitionId)
          .build();
    }
  }

  static class SessionCommandDescriptor<T extends Message, I extends Message> implements RequestExecutor.RequestDescriptor<T, I, SessionCommandHeader> {
    private final Function<T, I> idGetter;
    private final Function<T, SessionCommandHeaders> headerGetter;

    SessionCommandDescriptor(Function<T, I> idGetter, Function<T, SessionCommandHeaders> headerGetter) {
      this.idGetter = idGetter;
      this.headerGetter = headerGetter;
    }

    @Override
    public I getId(T request) {
      return idGetter.apply(request);
    }

    @Override
    public Collection<SessionCommandHeader> getHeaders(T request) {
      return headerGetter.apply(request).getHeadersList();
    }

    @Override
    public int getPartitionId(SessionCommandHeader header) {
      return header.getPartitionId();
    }

    @Override
    public SessionCommandHeader getDefaultHeader(int partitionId) {
      return SessionCommandHeader.newBuilder()
          .setPartitionId(partitionId)
          .build();
    }
  }

  static class SessionQueryDescriptor<T extends Message, I extends Message> implements RequestExecutor.RequestDescriptor<T, I, SessionQueryHeader> {
    private final Function<T, I> idGetter;
    private final Function<T, SessionQueryHeaders> headerGetter;

    SessionQueryDescriptor(Function<T, I> idGetter, Function<T, SessionQueryHeaders> headerGetter) {
      this.idGetter = idGetter;
      this.headerGetter = headerGetter;
    }

    @Override
    public I getId(T request) {
      return idGetter.apply(request);
    }

    @Override
    public Collection<SessionQueryHeader> getHeaders(T request) {
      return headerGetter.apply(request).getHeadersList();
    }

    @Override
    public int getPartitionId(SessionQueryHeader header) {
      return header.getPartitionId();
    }

    @Override
    public SessionQueryHeader getDefaultHeader(int partitionId) {
      return SessionQueryHeader.newBuilder()
          .setPartitionId(partitionId)
          .build();
    }
  }
}
