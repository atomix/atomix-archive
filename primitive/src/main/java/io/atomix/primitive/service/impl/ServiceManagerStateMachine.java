package io.atomix.primitive.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.google.protobuf.ByteString;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PartitionManagementService;
import io.atomix.primitive.service.Command;
import io.atomix.primitive.service.Query;
import io.atomix.primitive.service.StateMachine;
import io.atomix.primitive.util.ByteArrayDecoder;
import io.atomix.utils.concurrent.Futures;

/**
 * Default state machine.
 */
public class ServiceManagerStateMachine implements StateMachine {
  private final PartitionId partitionId;
  private final PartitionManagementService managementService;
  private final Map<String, ServiceStateMachine> services = new HashMap<>();
  private Context context;

  public ServiceManagerStateMachine(PartitionId partitionId, PartitionManagementService managementService) {
    this.partitionId = partitionId;
    this.managementService = managementService;
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
      services.put(serviceId.getName(), newService(serviceId.getName(), serviceId.getType()));
    }
  }

  private ServiceStateMachine newService(String name, String type) {
    PrimitiveType primitiveType = managementService.getPrimitiveTypes().getPrimitiveType(type);
    ServiceStateMachine service = new ServiceStateMachine(
        name, type, primitiveType.newService(partitionId, managementService));
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
    ServiceStateMachine service = services.computeIfAbsent(id.getName(), name -> newService(id.getName(), id.getType()));
    return service.apply(serviceCommand.map(request -> request.getRequest().toByteArray()))
        .thenApply(response -> ServiceResponse.newBuilder()
            .setResponse(ByteString.copyFrom(response))
            .build()
            .toByteArray());
  }

  @Override
  public CompletableFuture<byte[]> apply(Query<byte[]> query) {
    Query<ServiceRequest> serviceQuery = query.map(bytes -> ByteArrayDecoder.decode(bytes, ServiceRequest::parseFrom));
    ServiceId id = serviceQuery.value().getId();
    ServiceStateMachine service = services.get(id.getName());
    if (service == null) {
      return Futures.exceptionalFuture(new PrimitiveException.UnknownService());
    }
    return service.apply(serviceQuery.map(request -> request.getRequest().toByteArray()))
        .thenApply(response -> ServiceResponse.newBuilder()
            .setResponse(ByteString.copyFrom(response))
            .build()
            .toByteArray());
  }
}
