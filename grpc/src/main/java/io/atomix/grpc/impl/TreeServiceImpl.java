/*
 * Copyright 2019-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.grpc.impl;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import io.atomix.core.Atomix;
import io.atomix.core.tree.AsyncAtomicDocumentTree;
import io.atomix.core.tree.DocumentTreeEventListener;
import io.atomix.grpc.tree.GetChildrenRequest;
import io.atomix.grpc.tree.GetChildrenResponse;
import io.atomix.grpc.tree.GetRequest;
import io.atomix.grpc.tree.GetResponse;
import io.atomix.grpc.tree.RemoveRequest;
import io.atomix.grpc.tree.RemoveResponse;
import io.atomix.grpc.tree.SetRequest;
import io.atomix.grpc.tree.SetResponse;
import io.atomix.grpc.tree.TreeEvent;
import io.atomix.grpc.tree.TreeId;
import io.atomix.grpc.tree.TreeNode;
import io.atomix.grpc.tree.TreeServiceGrpc;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.protocols.backup.MultiPrimaryProtocol;
import io.atomix.protocols.log.DistributedLogProtocol;
import io.atomix.protocols.raft.MultiRaftProtocol;
import io.grpc.stub.StreamObserver;

/**
 * Tree service implementation.
 */
public class TreeServiceImpl extends TreeServiceGrpc.TreeServiceImplBase {
  private final Atomix atomix;

  public TreeServiceImpl(Atomix atomix) {
    this.atomix = atomix;
  }

  private ProxyProtocol toProtocol(TreeId id) {
    if (id.hasRaft()) {
      return MultiRaftProtocol.builder(id.getRaft().getGroup())
          .build();
    } else if (id.hasMultiPrimary()) {
      return MultiPrimaryProtocol.builder(id.getMultiPrimary().getGroup())
          .build();
    } else if (id.hasLog()) {
      return DistributedLogProtocol.builder(id.getLog().getGroup())
          .build();
    }
    return null;
  }

  private CompletableFuture<AsyncAtomicDocumentTree<byte[]>> getTree(TreeId id) {
    return atomix.<byte[]>atomicDocumentTreeBuilder(id.getName())
        .withProtocol(toProtocol(id))
        .getAsync()
        .thenApply(tree -> tree.async());
  }

  private <T> void run(TreeId id, Function<AsyncAtomicDocumentTree<byte[]>, CompletableFuture<T>> function, StreamObserver<T> responseObserver) {
    getTree(id).whenComplete((tree, getError) -> {
      if (getError == null) {
        function.apply(tree).whenComplete((result, funcError) -> {
          if (funcError == null) {
            responseObserver.onNext(result);
            responseObserver.onCompleted();
          } else {
            responseObserver.onError(funcError);
            responseObserver.onCompleted();
          }
        });
      } else {
        responseObserver.onError(getError);
        responseObserver.onCompleted();
      }
    });
  }

  @Override
  public void getChildren(GetChildrenRequest request, StreamObserver<GetChildrenResponse> responseObserver) {
    run(request.getId(), tree -> tree.getChildren(request.getPath())
        .thenApply(children -> GetChildrenResponse.newBuilder()
            .putAllChildren(children.entrySet()
                .stream()
                .map(e -> Maps.immutableEntry(e.getKey(), TreeNode.newBuilder()
                    .setValue(ByteString.copyFrom(e.getValue().value()))
                    .setVersion(e.getValue().version())
                    .build())).collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue())))
            .build()), responseObserver);
  }

  @Override
  public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
    run(request.getId(), tree -> tree.get(request.getPath())
        .thenApply(value -> GetResponse.newBuilder()
            .setNode(TreeNode.newBuilder()
                .setValue(ByteString.copyFrom(value.value()))
                .setVersion(value.version())
                .build())
            .build()), responseObserver);
  }

  @Override
  public void set(SetRequest request, StreamObserver<SetResponse> responseObserver) {
    if (request.getVersion() == 0) {
      run(request.getId(), tree -> tree.set(request.getPath(), request.getValue().toByteArray())
          .thenApply(result -> SetResponse.newBuilder()
              .setSucceeded(true)
              .build()), responseObserver);
    } else {
      run(request.getId(), tree -> tree.replace(request.getPath(), request.getValue().toByteArray(), request.getVersion())
          .thenApply(succeeded -> SetResponse.newBuilder()
              .setSucceeded(succeeded)
              .build()), responseObserver);
    }
  }

  @Override
  public void remove(RemoveRequest request, StreamObserver<RemoveResponse> responseObserver) {
    run(request.getId(), tree -> tree.remove(request.getPath())
        .thenApply(result -> RemoveResponse.newBuilder().build()), responseObserver);
  }

  @Override
  public StreamObserver<TreeId> listen(StreamObserver<TreeEvent> responseObserver) {
    Map<TreeId, DocumentTreeEventListener<byte[]>> listeners = new ConcurrentHashMap<>();
    return new StreamObserver<TreeId>() {
      @Override
      public void onNext(TreeId id) {
        DocumentTreeEventListener<byte[]> listener = event -> {
          switch (event.type()) {
            case CREATED:
              responseObserver.onNext(TreeEvent.newBuilder()
                  .setId(id)
                  .setType(TreeEvent.Type.CREATED)
                  .setValue(ByteString.copyFrom(event.newValue().get().value()))
                  .setVersion(event.newValue().get().version())
                  .build());
              break;
            case UPDATED:
              responseObserver.onNext(TreeEvent.newBuilder()
                  .setId(id)
                  .setType(TreeEvent.Type.UPDATED)
                  .setValue(ByteString.copyFrom(event.newValue().get().value()))
                  .setVersion(event.newValue().get().version())
                  .build());
              break;
            case DELETED:
              responseObserver.onNext(TreeEvent.newBuilder()
                  .setId(id)
                  .setType(TreeEvent.Type.DELETED)
                  .setValue(ByteString.copyFrom(event.oldValue().get().value()))
                  .setVersion(event.oldValue().get().version())
                  .build());
              break;
          }
        };
        listeners.put(id, listener);
        getTree(id).thenAccept(tree -> tree.addListener(listener));
      }

      @Override
      public void onError(Throwable t) {
        listeners.forEach((id, listener) -> getTree(id).thenAccept(tree -> tree.removeListener(listener)));
        responseObserver.onCompleted();
      }

      @Override
      public void onCompleted() {
        listeners.forEach((id, listener) -> getTree(id).thenAccept(tree -> tree.removeListener(listener)));
        responseObserver.onCompleted();
      }
    };
  }
}
