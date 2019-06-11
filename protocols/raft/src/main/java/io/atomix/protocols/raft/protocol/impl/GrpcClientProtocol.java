/*
 * Copyright 2019-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.protocols.raft.protocol.impl;

import java.net.ConnectException;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import io.atomix.protocols.raft.protocol.CommandRequest;
import io.atomix.protocols.raft.protocol.CommandResponse;
import io.atomix.protocols.raft.protocol.QueryRequest;
import io.atomix.protocols.raft.protocol.QueryResponse;
import io.atomix.protocols.raft.protocol.RaftClientProtocol;
import io.atomix.protocols.raft.protocol.RaftServiceGrpc;
import io.atomix.server.management.ServiceFactory;
import io.atomix.utils.stream.StreamHandler;
import io.grpc.stub.StreamObserver;

/**
 * gRPC client protocol.
 */
public class GrpcClientProtocol implements RaftClientProtocol {
  private static final ConnectException CONNECT_EXCEPTION = new ConnectException();

  static {
    CONNECT_EXCEPTION.setStackTrace(new StackTraceElement[0]);
  }

  private final ServiceFactory<RaftServiceGrpc.RaftServiceStub> factory;

  public GrpcClientProtocol(ServiceFactory<RaftServiceGrpc.RaftServiceStub> factory) {
    this.factory = factory;
  }

  @Override
  public CompletableFuture<QueryResponse> query(String server, QueryRequest request) {
    return execute(server, (raft, observer) -> raft.query(request, observer));
  }

  @Override
  public CompletableFuture<Void> queryStream(String server, QueryRequest request, StreamHandler<QueryResponse> handler) {
    return execute(server, handler, (raft, observer) -> raft.queryStream(request, observer));
  }

  @Override
  public CompletableFuture<CommandResponse> command(String server, CommandRequest request) {
    return execute(server, (raft, observer) -> raft.command(request, observer));
  }

  @Override
  public CompletableFuture<Void> commandStream(String server, CommandRequest request, StreamHandler<CommandResponse> handler) {
    return execute(server, handler, (raft, observer) -> raft.commandStream(request, observer));
  }

  protected <T> CompletableFuture<T> execute(String member, BiConsumer<RaftServiceGrpc.RaftServiceStub, StreamObserver<T>> callback) {
    CompletableFuture<T> future = new CompletableFuture<>();
    callback.accept(factory.getService(member), new StreamObserver<T>() {
      @Override
      public void onNext(T response) {
        future.complete(response);
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

  protected <T> CompletableFuture<Void> execute(String member, StreamHandler<T> handler, BiConsumer<RaftServiceGrpc.RaftServiceStub, StreamObserver<T>> callback) {
    callback.accept(factory.getService(member), new StreamObserver<T>() {
      @Override
      public void onNext(T value) {
        handler.next(value);
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
}
