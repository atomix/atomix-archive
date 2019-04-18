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

import java.util.concurrent.CompletableFuture;

import io.atomix.core.Atomix;
import io.atomix.grpc.GrpcService;
import io.atomix.grpc.ManagedGrpcService;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.net.Address;
import io.grpc.Server;
import io.grpc.ServerBuilder;

/**
 * Netty gRPC service.
 */
public class NettyGrpcService implements ManagedGrpcService {
  private final Atomix atomix;
  private final Address address;
  private Server server;

  public NettyGrpcService(Atomix atomix, Address address) {
    this.atomix = atomix;
    this.address = address;
  }

  @Override
  public Address address() {
    return address;
  }

  @Override
  public CompletableFuture<GrpcService> start() {
    server = ServerBuilder.forPort(address.port())
        .addService(new ClusterServiceImpl(atomix))
        .addService(new CounterServiceImpl(atomix))
        .addService(new EventServiceImpl(atomix))
        .addService(new LockServiceImpl(atomix))
        .addService(new LogServiceImpl(atomix))
        .addService(new MapServiceImpl(atomix))
        .addService(new PartitionServiceImpl(atomix))
        .addService(new PrimitivesServiceImpl(atomix))
        .addService(new SetServiceImpl(atomix))
        .addService(new ValueServiceImpl(atomix))
        .build();
    try {
      server.start();
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
    return CompletableFuture.completedFuture(this);
  }

  @Override
  public boolean isRunning() {
    return !server.isShutdown();
  }

  @Override
  public CompletableFuture<Void> stop() {
    server.shutdown();
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Netty gRPC service builder.
   */
  public static class Builder extends ManagedGrpcService.Builder {
    @Override
    public ManagedGrpcService build() {
      return new NettyGrpcService(atomix, address);
    }
  }
}
