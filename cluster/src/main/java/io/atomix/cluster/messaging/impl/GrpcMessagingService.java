package io.atomix.cluster.messaging.impl;

import java.net.ConnectException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.protobuf.ByteString;
import io.atomix.cluster.MemberService;
import io.atomix.cluster.grpc.ChannelService;
import io.atomix.cluster.grpc.ServiceRegistry;
import io.atomix.cluster.messaging.Message;
import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.cluster.messaging.MessagingService;
import io.atomix.cluster.messaging.MessagingServiceGrpc;
import io.atomix.utils.component.Component;
import io.atomix.utils.component.Dependency;
import io.atomix.utils.component.Managed;
import io.atomix.utils.net.Address;
import io.atomix.utils.stream.StreamFunction;
import io.atomix.utils.stream.StreamHandler;
import io.grpc.stub.StreamObserver;

/**
 * gRPC messaging service.
 */
@Component(MessagingConfig.class)
public class GrpcMessagingService extends MessagingServiceGrpc.MessagingServiceImplBase implements MessagingService, Managed<MessagingConfig> {
  @Dependency
  private MemberService memberService;
  @Dependency
  private ServiceRegistry grpc;
  @Dependency
  private ChannelService channelService;

  private final Map<Address, MessagingServiceGrpc.MessagingServiceStub> services = new ConcurrentHashMap<>();
  private final Map<String, BiConsumer<Message, StreamObserver<Message>>> handlers = new ConcurrentHashMap<>();
  private final Map<String, Function<StreamObserver<Message>, StreamObserver<Message>>> streamHandlers = new ConcurrentHashMap<>();
  private Address address;

  @Override
  public Address address() {
    return address;
  }

  @Override
  public void sendAndReceive(Message request, StreamObserver<Message> responseObserver) {
    receiveStream(request, responseObserver);
  }

  @Override
  public StreamObserver<Message> sendStream(StreamObserver<Message> responseObserver) {
    return sendAndReceiveStream(responseObserver);
  }

  @Override
  public void receiveStream(Message request, StreamObserver<Message> responseObserver) {
    BiConsumer<Message, StreamObserver<Message>> handler = handlers.get(request.getType());
    if (handler != null) {
      handler.accept(request, responseObserver);
    }
  }

  @Override
  public StreamObserver<Message> sendAndReceiveStream(StreamObserver<Message> responseObserver) {
    return new StreamObserver<Message>() {
      private volatile StreamObserver<Message> streamObserver;

      @Override
      public void onNext(Message message) {
        StreamObserver<Message> streamObserver = this.streamObserver;
        if (streamObserver == null) {
          synchronized (this) {
            if (this.streamObserver == null) {
              Function<StreamObserver<Message>, StreamObserver<Message>> streamHandler = streamHandlers.get(message.getType());
              if (streamHandler != null) {
                streamObserver = streamHandler.apply(responseObserver);
                this.streamObserver = streamObserver;
              }
            }
          }
        }
        if (streamObserver != null) {
          streamObserver.onNext(message);
        } else {
          responseObserver.onError(new ConnectException());
        }
      }

      @Override
      public void onError(Throwable t) {
        StreamObserver<Message> streamObserver = this.streamObserver;
        if (streamObserver != null) {
          streamObserver.onError(t);
        }
      }

      @Override
      public void onCompleted() {
        StreamObserver<Message> streamObserver = this.streamObserver;
        if (streamObserver != null) {
          streamObserver.onCompleted();
        }
      }
    };
  }

  private MessagingServiceGrpc.MessagingServiceStub getService(Address address) {
    MessagingServiceGrpc.MessagingServiceStub service = services.get(address);
    if (service == null) {
      service = services.computeIfAbsent(address, a -> MessagingServiceGrpc.newStub(channelService.getChannel(address.host(), address.port())));
    }
    return service;
  }

  @Override
  public CompletableFuture<Void> sendAsync(Address address, String type, byte[] payload) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    getService(address).sendAndReceive(Message.newBuilder()
        .setType(type)
        .setPayload(ByteString.copyFrom(payload))
        .build(), new StreamObserver<Message>() {
      @Override
      public void onNext(Message message) {
      }

      @Override
      public void onError(Throwable t) {
        future.completeExceptionally(t);
      }

      @Override
      public void onCompleted() {
        future.complete(null);
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<StreamHandler<byte[]>> sendStreamAsync(Address address, String type) {
    CompletableFuture<StreamHandler<byte[]>> future = new CompletableFuture<>();
    StreamObserver<Message> stream = getService(address).sendStream(new StreamObserver<Message>() {
      @Override
      public void onNext(Message value) {

      }

      @Override
      public void onError(Throwable t) {
        future.completeExceptionally(t);
      }

      @Override
      public void onCompleted() {
      }
    });
    future.complete(new StreamHandler<byte[]>() {
      @Override
      public void next(byte[] value) {
        stream.onNext(Message.newBuilder()
            .setType(type)
            .setPayload(ByteString.copyFrom(value))
            .build());
      }

      @Override
      public void complete() {
        stream.onCompleted();
      }

      @Override
      public void error(Throwable error) {
        stream.onError(error);
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<byte[]> sendAndReceive(Address address, String type, byte[] payload, Duration timeout, Executor executor) {
    CompletableFuture<byte[]> future = new CompletableFuture<>();
    getService(address).sendAndReceive(Message.newBuilder()
        .setType(type)
        .setPayload(ByteString.copyFrom(payload))
        .build(), new StreamObserver<Message>() {
      @Override
      public void onNext(Message message) {
        future.complete(message.getPayload().toByteArray());
      }

      @Override
      public void onError(Throwable t) {
        future.completeExceptionally(t);
      }

      @Override
      public void onCompleted() {
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<StreamFunction<byte[], CompletableFuture<byte[]>>> sendStreamAndReceive(Address address, String type, Duration timeout, Executor executor) {
    CompletableFuture<byte[]> future = new CompletableFuture<>();
    StreamObserver<Message> stream = getService(address).sendStream(new StreamObserver<Message>() {
      @Override
      public void onNext(Message message) {
        future.complete(message.getPayload().toByteArray());
      }

      @Override
      public void onError(Throwable t) {
        future.completeExceptionally(t);
      }

      @Override
      public void onCompleted() {
      }
    });
    return CompletableFuture.completedFuture(new StreamFunction<byte[], CompletableFuture<byte[]>>() {
      @Override
      public void next(byte[] value) {
        stream.onNext(Message.newBuilder()
            .setType(type)
            .setPayload(ByteString.copyFrom(value))
            .build());
      }

      @Override
      public CompletableFuture<byte[]> complete() {
        stream.onCompleted();
        return future;
      }

      @Override
      public CompletableFuture<byte[]> error(Throwable error) {
        stream.onError(error);
        return future;
      }
    });
  }

  @Override
  public CompletableFuture<Void> sendAndReceiveStream(Address address, String type, byte[] payload, StreamHandler<byte[]> handler, Duration timeout, Executor executor) {
    getService(address).receiveStream(Message.newBuilder()
        .setType(type)
        .setPayload(ByteString.copyFrom(payload))
        .build(), new StreamObserver<Message>() {
      @Override
      public void onNext(Message value) {
        handler.next(value.getPayload().toByteArray());
      }

      @Override
      public void onError(Throwable t) {
        handler.error(t);
      }

      @Override
      public void onCompleted() {
        handler.complete();
      }
    });
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<StreamHandler<byte[]>> sendStreamAndReceiveStream(Address address, String type, StreamHandler<byte[]> handler, Duration timeout, Executor executor) {
    StreamObserver<Message> stream = getService(address).sendAndReceiveStream(new StreamObserver<Message>() {
      @Override
      public void onNext(Message message) {
        handler.next(message.getPayload().toByteArray());
      }

      @Override
      public void onError(Throwable t) {
        handler.error(t);
      }

      @Override
      public void onCompleted() {
        handler.complete();
      }
    });
    return CompletableFuture.completedFuture(new StreamHandler<byte[]>() {
      @Override
      public void next(byte[] value) {
        stream.onNext(Message.newBuilder()
            .setType(type)
            .setPayload(ByteString.copyFrom(value))
            .build());
      }

      @Override
      public void complete() {
        stream.onCompleted();
      }

      @Override
      public void error(Throwable error) {
        stream.onError(error);
      }
    });
  }

  @Override
  public void registerHandler(String type, Consumer<byte[]> handler, Executor executor) {
    handlers.put(type, (message, stream) -> {
      try {
        handler.accept(message.getPayload().toByteArray());
      } finally {
        stream.onCompleted();
      }
    });
  }

  @Override
  public void registerHandler(String type, Function<byte[], byte[]> handler, Executor executor) {
    handlers.put(type, (message, stream) -> {
      try {
        stream.onNext(Message.newBuilder()
            .setType(type)
            .setPayload(ByteString.copyFrom(handler.apply(message.getPayload().toByteArray())))
            .build());
        stream.onCompleted();
      } catch (Exception e) {
        stream.onError(e);
      }
    });
  }

  @Override
  public void registerHandler(String type, Function<byte[], CompletableFuture<byte[]>> handler) {
    handlers.put(type, (message, stream) -> {
      handler.apply(message.getPayload().toByteArray())
          .whenComplete((result, error) -> {
            if (error == null) {
              stream.onNext(Message.newBuilder()
                  .setType(type)
                  .setPayload(ByteString.copyFrom(result))
                  .build());
              stream.onCompleted();
            } else {
              stream.onError(error);
            }
          });
    });
  }

  @Override
  public void registerStreamHandler(String type, Supplier<StreamFunction<byte[], CompletableFuture<byte[]>>> handler) {
    streamHandlers.put(type, stream -> {
      StreamFunction<byte[], CompletableFuture<byte[]>> function = handler.get();
      return new StreamObserver<Message>() {
        @Override
        public void onNext(Message message) {
          function.next(message.getPayload().toByteArray());
        }

        @Override
        public void onError(Throwable t) {
          function.error(t).whenComplete((result, error) -> {
            if (error == null) {
              if (result != null) {
                stream.onNext(Message.newBuilder()
                    .setType(type)
                    .setPayload(ByteString.copyFrom(result))
                    .build());
              }
              stream.onCompleted();
            } else {
              stream.onError(error);
            }
          });
        }

        @Override
        public void onCompleted() {
          function.complete().whenComplete((result, error) -> {
            if (error == null) {
              if (result != null) {
                stream.onNext(Message.newBuilder()
                    .setType(type)
                    .setPayload(ByteString.copyFrom(result))
                    .build());
              }
              stream.onCompleted();
            } else {
              stream.onError(error);
            }
          });
        }
      };
    });
  }

  @Override
  public void registerStreamingHandler(String type, BiConsumer<byte[], StreamHandler<byte[]>> handler) {
    handlers.put(type, (message, stream) -> {
      handler.accept(message.getPayload().toByteArray(), new StreamHandler<byte[]>() {
        @Override
        public void next(byte[] value) {
          stream.onNext(Message.newBuilder()
              .setType(type)
              .setPayload(ByteString.copyFrom(value))
              .build());
        }

        @Override
        public void complete() {
          stream.onCompleted();
        }

        @Override
        public void error(Throwable error) {
          stream.onError(error);
        }
      });
    });
  }

  @Override
  public void registerStreamingStreamHandler(String type, Function<StreamHandler<byte[]>, StreamHandler<byte[]>> handler) {
    streamHandlers.put(type, stream -> {
      StreamHandler<byte[]> responseStream = handler.apply(new StreamHandler<byte[]>() {
        @Override
        public void next(byte[] value) {
          stream.onNext(Message.newBuilder()
              .setType(type)
              .setPayload(ByteString.copyFrom(value))
              .build());
        }

        @Override
        public void complete() {
          stream.onCompleted();
        }

        @Override
        public void error(Throwable error) {
          stream.onError(error);
        }
      });
      return new StreamObserver<Message>() {
        @Override
        public void onNext(Message value) {
          responseStream.next(value.getPayload().toByteArray());
        }

        @Override
        public void onError(Throwable t) {
          responseStream.error(t);
        }

        @Override
        public void onCompleted() {
          responseStream.complete();
        }
      };
    });
  }

  @Override
  public void unregisterHandler(String type) {
    handlers.remove(type);
    streamHandlers.remove(type);
  }

  @Override
  public CompletableFuture<Void> start(MessagingConfig config) {
    address = Address.from(memberService.getLocalMember().getHost(), memberService.getLocalMember().getPort());
    grpc.register(this);
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> stop() {
    return CompletableFuture.completedFuture(null);
  }
}
