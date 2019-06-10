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
package io.atomix.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.google.protobuf.ByteString;
import io.atomix.service.Command;
import io.atomix.service.PrimitiveService;
import io.atomix.service.Query;
import io.atomix.service.ServiceException;
import io.atomix.service.ServiceTypeRegistry;
import io.atomix.service.StateMachine;
import io.atomix.service.protocol.CreateResponse;
import io.atomix.service.protocol.DeleteResponse;
import io.atomix.service.protocol.ServiceId;
import io.atomix.service.protocol.ServiceRequest;
import io.atomix.service.protocol.ServiceResponse;
import io.atomix.service.util.ByteArrayDecoder;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.stream.StreamHandler;

/**
 * Default state machine.
 */
public class ServiceManagerStateMachine implements StateMachine {
  private final ServiceTypeRegistry serviceTypes;
  private final Map<String, ServiceStateMachine> services = new HashMap<>();
  private Context context;

  public ServiceManagerStateMachine(ServiceTypeRegistry serviceTypes) {
    this.serviceTypes = serviceTypes;
  }

  @Override
  public void init(Context context) {
    this.context = context;
  }

  @Override
  public void snapshot(OutputStream output) throws IOException {
    for (ServiceStateMachine service : services.values()) {
      ServiceId.newBuilder()
          .setName(service.context().getName())
          .setType(service.context().getType())
          .build()
          .writeDelimitedTo(output);
      service.snapshot(output);
    }
  }

  @Override
  public void install(InputStream input) throws IOException {
    services.clear();
    while (input.available() > 0) {
      ServiceId serviceId = ServiceId.parseDelimitedFrom(input);
      services.put(serviceId.getName(), newService(serviceId));
    }
  }

  private ServiceStateMachine newService(ServiceId serviceId) {
    PrimitiveService.Type serviceType = serviceTypes.getServiceType(serviceId.getType());
    ServiceStateMachine service = new ServiceStateMachine(
        serviceId, serviceType.newService());
    service.init(context);
    return service;
  }

  @Override
  public boolean canDelete(long index) {
    for (ServiceStateMachine service : services.values()) {
      if (!service.canDelete(index)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public CompletableFuture<byte[]> apply(Command<byte[]> command) {
    Command<ServiceRequest> serviceCommand = command.map(bytes -> ByteArrayDecoder.decode(bytes, ServiceRequest::parseFrom));
    ServiceId id = serviceCommand.value().getId();
    ServiceStateMachine service = services.computeIfAbsent(id.getName(), name -> newService(id));

    // If the service is being created, just return an empty response.
    if (serviceCommand.value().hasCreate()) {
      return CompletableFuture.completedFuture(ServiceResponse.newBuilder()
          .setCreate(CreateResponse.newBuilder().build())
          .build()
          .toByteArray());
    }

    // If the service is being deleted, remove the service and return an empty response.
    if (serviceCommand.value().hasDelete()) {
      services.remove(id.getName());
      return CompletableFuture.completedFuture(ServiceResponse.newBuilder()
          .setDelete(DeleteResponse.newBuilder().build())
          .build()
          .toByteArray());
    }

    return service.apply(serviceCommand.map(request -> request.getCommand().toByteArray()))
        .thenApply(response -> ServiceResponse.newBuilder()
            .setCommand(ByteString.copyFrom(response))
            .build()
            .toByteArray());
  }

  @Override
  public CompletableFuture<Void> apply(Command<byte[]> command, StreamHandler<byte[]> handler) {
    Command<ServiceRequest> serviceCommand = command.map(bytes -> ByteArrayDecoder.decode(bytes, ServiceRequest::parseFrom));
    ServiceId id = serviceCommand.value().getId();
    ServiceStateMachine service = services.computeIfAbsent(id.getName(), name -> newService(id));
    return service.apply(serviceCommand.map(request -> request.getCommand().toByteArray()), new StreamHandler<byte[]>() {
      @Override
      public void next(byte[] response) {
        handler.next(ServiceResponse.newBuilder()
            .setCommand(ByteString.copyFrom(response))
            .build()
            .toByteArray());
      }

      @Override
      public void complete() {
        handler.complete();
      }

      @Override
      public void error(Throwable error) {
        handler.error(error);
      }
    });
  }

  @Override
  public CompletableFuture<byte[]> apply(Query<byte[]> query) {
    Query<ServiceRequest> serviceQuery = query.map(bytes -> ByteArrayDecoder.decode(bytes, ServiceRequest::parseFrom));
    ServiceId id = serviceQuery.value().getId();
    ServiceStateMachine service = services.get(id.getName());
    if (service == null) {
      service = newService(id);
    }
    return service.apply(serviceQuery.map(request -> request.getQuery().toByteArray()))
        .thenApply(response -> ServiceResponse.newBuilder()
            .setQuery(ByteString.copyFrom(response))
            .build()
            .toByteArray());
  }

  @Override
  public CompletableFuture<Void> apply(Query<byte[]> query, StreamHandler<byte[]> handler) {
    Query<ServiceRequest> serviceQuery = query.map(bytes -> ByteArrayDecoder.decode(bytes, ServiceRequest::parseFrom));
    ServiceId id = serviceQuery.value().getId();
    ServiceStateMachine service = services.get(id.getName());
    if (service == null) {
      return Futures.exceptionalFuture(new ServiceException.UnknownService());
    }
    return service.apply(serviceQuery.map(request -> request.getQuery().toByteArray()), new StreamHandler<byte[]>() {
      @Override
      public void next(byte[] response) {
        handler.next(ServiceResponse.newBuilder()
            .setQuery(ByteString.copyFrom(response))
            .build()
            .toByteArray());
      }

      @Override
      public void complete() {
        handler.complete();
      }

      @Override
      public void error(Throwable error) {
        handler.error(error);
      }
    });
  }
}
